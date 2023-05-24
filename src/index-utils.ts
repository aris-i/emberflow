import {
  Action,
  LogicActionType,
  LogicResult,
  LogicResultDoc,
  SecurityFn,
  ValidateFormResult,
} from "./types";
import {firestore} from "firebase-admin";
import {admin, docPaths, logicConfigs, securityConfig, validatorConfig, viewLogicConfigs} from "./index";
import {syncPeerViews} from "./logics/view-logics";
import {expandAndGroupDocPaths, findMatchingDocPathRegex} from "./utils/paths";
import {deepEqual} from "./utils/misc";
import DocumentData = firestore.DocumentData;

export const _mockable = {
  getViewLogicsConfig: () => viewLogicConfigs,
};

async function commitBatchIfNeeded(
  batch: FirebaseFirestore.WriteBatch,
  db: FirebaseFirestore.Firestore,
  writeCount: number): Promise<[FirebaseFirestore.WriteBatch, number]> {
  writeCount++; // Increment writeCount for each write operation
  if (writeCount === 500) { // Commit batch every 500 writes
    console.log("Committing batch of 500 writes...");
    await batch.commit();
    batch = db.batch();
    writeCount = 0;
  }
  return [batch, writeCount];
}

export async function distribute(docsByDstPath: Map<string, LogicResultDoc>) {
  const db = admin.firestore();
  let batch = db.batch();
  let writeCount = 0;
  const forCopy: LogicResultDoc[] = [];

  for (const dstPath of Array.from(docsByDstPath.keys()).sort()) {
    console.log(`Documents for path ${dstPath}:`);
    const resultDoc = docsByDstPath.get(dstPath);
    if (!resultDoc) continue;
    const {
      action,
      doc,
      instructions,
    } = resultDoc;
    if (action === "copy") {
      forCopy.push(resultDoc);
    } else if (action === "delete") {
      // Delete document at dstPath
      const dstDocRef = db.doc(dstPath);
      batch.delete(dstDocRef);
      [batch, writeCount] = await commitBatchIfNeeded(batch, db, writeCount);
      console.log(`Document deleted at ${dstPath}`);
    } else if (action === "merge") {
      const updateData: { [key: string]: any } = {...doc};
      if (instructions) {
        for (const [property, instruction] of Object.entries(instructions)) {
          if (instruction === "++") {
            updateData[property] = admin.firestore.FieldValue.increment(1);
          } else if (instruction === "--") {
            updateData[property] = admin.firestore.FieldValue.increment(-1);
          } else if (instruction.startsWith("+")) {
            const incrementValue = parseInt(instruction.slice(1));
            if (isNaN(incrementValue)) {
              console.log(`Invalid increment value ${instruction} for property ${property}`);
            } else {
              updateData[property] = admin.firestore.FieldValue.increment(incrementValue);
            }
          } else if (instruction.startsWith("-")) {
            const decrementValue = parseInt(instruction.slice(1));
            if (isNaN(decrementValue)) {
              console.log(`Invalid decrement value ${instruction} for property ${property}`);
            } else {
              updateData[property] = admin.firestore.FieldValue.increment(-decrementValue);
            }
          } else {
            console.log(`Invalid instruction ${instruction} for property ${property}`);
          }
        }
      }

      // Merge document to dstPath
      const dstColPath = dstPath.endsWith("/#") ? dstPath.slice(0, -2) : null;
      let dstDocRef: FirebaseFirestore.DocumentReference;
      if (dstColPath) {
        const dstColRef = db.collection(dstColPath);
        dstDocRef = dstColRef.doc();
      } else {
        dstDocRef = db.doc(dstPath);
      }
      batch.set(dstDocRef, updateData, {merge: true});
      [batch, writeCount] = await commitBatchIfNeeded(batch, db, writeCount);
      console.log(`Document merged to ${dstPath}`);
    }
  }

  if (writeCount > 0) {
    console.log(`Committing final batch of ${writeCount} writes...`);
    await batch.commit();
    writeCount = 0;
  }

  // Do copy after all other operations
  for (const resultDoc of forCopy) {
    const {
      srcPath,
      dstPath,
      skipEntityDuringRecursiveCopy=[],
      copyMode="recursive",
    } = resultDoc;
    if (!srcPath) {
      continue;
    }
    const srcDocRef = db.doc(srcPath);
    const dstDocRef = db.doc(dstPath);
    batch.set(dstDocRef, (await srcDocRef.get()).data()!);
    [batch, writeCount] = await commitBatchIfNeeded(batch, db, writeCount);
    console.log(`Document copied from ${srcPath} to ${dstPath}`);

    if (copyMode === "recursive") {
      const subDocPaths = expandAndGroupDocPaths(srcPath);
      const pathsToCopy: string[] = [];
      for (const [entity, paths] of Object.entries(subDocPaths)) {
        if (!skipEntityDuringRecursiveCopy || !skipEntityDuringRecursiveCopy.includes(entity)) {
          pathsToCopy.push(...paths);
        }
      }
      for (const path of pathsToCopy) {
        const srcDocRef = db.doc(path);
        const dstDocRef = db.doc(path.replace(srcPath, dstPath));
        batch.set(dstDocRef, (await srcDocRef.get()).data()!);
        [batch, writeCount] = await commitBatchIfNeeded(batch, db, writeCount);
        console.log(`Document copied from ${path} to ${dstDocRef.path}`);
      }
    }
  }

  if (writeCount > 0) {
    console.log(`Committing final batch of ${writeCount} writes...`);
    await batch.commit();
    writeCount = 0;
  }
}

export async function revertModificationsOutsideForm(document: FirebaseFirestore.DocumentData, beforeDocument: FirebaseFirestore.DocumentData | null | undefined, snapshot: FirebaseFirestore.DocumentSnapshot) {
  // Revert any changes made to document other than @form
  const revertedValues: Record<string, any> = {};

  if (beforeDocument) {
    const modifiedFields = Object.keys(document ?? {}).filter((key) => !key.startsWith("@form"));
    modifiedFields.forEach((field) => {
      if ( !deepEqual(document?.[field], beforeDocument[field]) ) {
        revertedValues[field] = beforeDocument[field];
      }
    });
  }
  // if revertedValues is not empty, update the document
  if (Object.keys(revertedValues).length > 0) {
    console.log("Reverting document:\n", revertedValues);
    await snapshot.ref.update(revertedValues);
  }
}

export async function validateForm(
  entity: string,
  document: FirebaseFirestore.DocumentData,
  docPath: string
): Promise<ValidateFormResult> {
  let hasValidationError = false;
  const validate = validatorConfig[entity];
  const validationResult = await validate(document, docPath);

  // Check if validation failed
  if (validationResult && Object.keys(validationResult).length > 0) {
    console.log(`Document validation failed: ${JSON.stringify(validationResult)}`);
    hasValidationError = true;
  }
  return [hasValidationError, validationResult];
}

export function getFormModifiedFields(document: DocumentData) {
  const formFields = Object.keys(document?.["@form"] ?? {}).filter((key) => !key.startsWith("@"));
  // compare value of each @form field with the value of the same field in the document to get modified fields
  return formFields.filter((field) => !deepEqual(document?.[field], document?.["@form"]?.[field]));
}

export async function delayFormSubmissionAndCheckIfCancelled(delay: number, snapshot: firestore.DocumentSnapshot) {
  let cancelFormSubmission = false;
  console.log(`Delaying document for ${delay}ms...`);
  await snapshot.ref.update({"@form.@status": "delay"});
  await new Promise((resolve) => setTimeout(resolve, delay));
  // Re-fetch document from Firestore
  const updatedSnapshot = await snapshot.ref.get();
  const updatedDocument = updatedSnapshot.data();
  console.log("Re-fetched document from Firestore after delay:\n", updatedDocument);
  // Check if form status is "cancel"
  if (updatedDocument?.["@form"]?.["@status"] === "cancel") {
    cancelFormSubmission = true;
  }
  return cancelFormSubmission;
}

export async function runBusinessLogics(actionType: LogicActionType, formModifiedFields: string[], entity: string, action: Action) {
  const matchingLogics = logicConfigs.filter((logic) => {
    return (
      (logic.actionTypes === "all" || logic.actionTypes.includes(actionType)) &&
            (logic.modifiedFields === "all" || logic.modifiedFields.some((field) => formModifiedFields?.includes(field))) &&
            (logic.entities === "all" || logic.entities.includes(entity))
    );
  });
  // TODO: Handle errors
  // TODO: Add logic for execTime
  return await Promise.all(matchingLogics.map((logic) => logic.logicFn(action)));
}

export function groupDocsByUserAndDstPath(docsByDstPath: Map<string, LogicResultDoc>, userId: string) {
  const userDocPath = docPaths["user"].replace("{userId}", userId);

  const userDocsByDstPath = new Map<string, LogicResultDoc>();
  const otherUsersDocsByDstPath = new Map<string, LogicResultDoc>();

  for (const [key, value] of docsByDstPath.entries()) {
    if (key.startsWith(userDocPath)) {
      userDocsByDstPath.set(key, value);
    } else {
      otherUsersDocsByDstPath.set(key, value);
    }
  }

  return {userDocsByDstPath, otherUsersDocsByDstPath};
}


export function getSecurityFn(entity: string): SecurityFn {
  return securityConfig[entity];
}


export function consolidateAndGroupByDstPath(logicResults: LogicResult[]): Map<string, LogicResultDoc> {
  const consolidated: Map<string, LogicResultDoc> = new Map();

  function warnOverwritingKeys(existing: any, incoming: any, type: string, dstPath: string) {
    for (const key in incoming) {
      if (Object.prototype.hasOwnProperty.call(existing, key)) {
        console.warn(`Overwriting key "${key}" in ${type} for dstPath "${dstPath}"`);
      }
    }
  }

  function processMerge(existingDoc: LogicResultDoc | undefined, doc: LogicResultDoc, dstPath: string) {
    if (existingDoc) {
      if (existingDoc.action === "merge") {
        warnOverwritingKeys(existingDoc.doc, doc.doc, "doc", dstPath);
        warnOverwritingKeys(existingDoc.instructions, doc.instructions, "instructions", dstPath);
        existingDoc.instructions = {...existingDoc.instructions, ...doc.instructions};
        existingDoc.doc = {...existingDoc.doc, ...doc.doc};
      } else {
        console.warn(
          `Action "merge" ignored because a "${existingDoc.action}" for dstPath "${dstPath}" already exists`
        );
      }
    } else {
      consolidated.set(dstPath, doc);
    }
  }

  function processDelete(existingDoc: LogicResultDoc | undefined, doc: LogicResultDoc, dstPath: string) {
    if (existingDoc) {
      if (existingDoc.action === "merge") {
        console.warn(`Action "merge" for dstPath "${dstPath}" is being overwritten by action "delete"`);
        consolidated.set(dstPath, doc);
      } else {
        console.warn(
          `Action "delete" ignored because a "${existingDoc.action}" for dstPath "${dstPath}" already exists`
        );
      }
    } else {
      consolidated.set(dstPath, doc);
    }
  }

  function processCopy(existingDoc: LogicResultDoc | undefined, doc: LogicResultDoc, dstPath: string) {
    if (existingDoc) {
      if (existingDoc.action === "copy") {
        console.warn(`Action "copy" ignored because "copy" for dstPath "${dstPath}" already exists`);
      } else {
        console.warn(`Action "${existingDoc.action}" for dstPath "${dstPath}" is being replaced by action "copy"`);
        consolidated.set(dstPath, doc);
      }
    } else {
      consolidated.set(dstPath, doc);
    }
  }

  for (const logicResult of logicResults) {
    for (const doc of logicResult.documents) {
      const {
        dstPath,
        action,
      } = doc;
      const existingDoc = consolidated.get(dstPath);

      if (action === "merge") {
        processMerge(existingDoc, doc, dstPath);
      } else if (action === "delete") {
        processDelete(existingDoc, doc, dstPath);
      } else if (action === "copy") {
        processCopy(existingDoc, doc, dstPath);
      }
    }
  }

  return consolidated;
}

export async function runViewLogics(dstPathLogicDocsMap: Map<string, LogicResultDoc>): Promise<LogicResult[]> {
  const logicResults: LogicResult[] = [];
  for (const [dstPath, logicResultDoc] of dstPathLogicDocsMap.entries()) {
    const {
      action,
      doc,
      instructions,
    } = logicResultDoc;
    const modifiedFields: string[] = [];
    if (doc) {
      modifiedFields.push(...Object.keys(doc));
    }
    if (instructions) {
      modifiedFields.push(...Object.keys(instructions));
    }
    const {entity} = findMatchingDocPathRegex(dstPath);
    if (!entity) {
      console.error("Entity should not be blank");
      continue;
    }
    const matchingLogics = _mockable.getViewLogicsConfig().filter((logic) => {
      return (
        (
          action === "merge" &&
          logic.modifiedFields.some((field) => modifiedFields.includes(field)) &&
          logic.entity === entity
        ) ||
        (
          action === "delete" &&
            logic.entity === entity
        )
      );
    });
    // TODO: Handle errors
    // TODO: Add logic for execTime
    const results = await Promise.all(matchingLogics.map((logic) => logic.viewLogicFn(logicResultDoc)));
    logicResults.push(...results);
  }
  return logicResults;
}

export async function runPeerSyncViews(userDocsByDstPath: Map<string, LogicResultDoc>) : Promise<LogicResult[]> {
  const logicResults: LogicResult[] = [];
  for (const [_, logicResultDoc] of userDocsByDstPath.entries()) {
    logicResults.push(await syncPeerViews(logicResultDoc));
  }
  return logicResults;
}

