import {
  Action,
  LogicActionType,
  LogicConfig,
  LogicResult,
  LogicResultDoc,
  SecurityFn,
  ValidateFormResult,
  ViewDefinition,
} from "./types";
import {firestore} from "firebase-admin";
import {admin, docPaths, logics, securityConfig, validatorConfig} from "./index";
import {createViewLogicFn} from "./logics";
import DocumentData = firestore.DocumentData;
import {expandAndGroupDocPaths} from "./utils/paths";
import {deepEqual} from "./utils/misc";

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

export async function distribute(userDocsByDstPath: Record<string, LogicResultDoc[]>) {
  const db = admin.firestore();
  let batch = db.batch();
  let writeCount = 0;
  const forCopy: LogicResultDoc[] = [];

  for (const [dstPath, resultDocs] of Object.entries(userDocsByDstPath).sort()) {
    console.log(`Documents for path ${dstPath}:`);
    for (const resultDoc of resultDocs) {
      const {
        action,
        doc,
        dstPath,
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
  const matchingLogics = logics.filter((logic) => {
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

export function groupDocsByUserAndDstPath(logicResults: Awaited<LogicResult>[], userId: string) {
  const docsByDstPath: Record<string, LogicResultDoc[]> = logicResults
    .filter((result) => result.status === "finished")
    .flatMap((result) => result.documents)
    .reduce<Record<string, LogicResultDoc[]>>((grouped, doc) => {
      const {dstPath} = doc;
      const documents = grouped[dstPath] ? [...grouped[dstPath], doc] : [doc];
      return {...grouped, [dstPath]: documents};
    }, {});

  const userDocPath = docPaths["user"].replace("{userId}", userId);
  const {userDocsByDstPath, otherUsersDocsByDstPath} = Object.entries(docsByDstPath)
    .reduce<Record<string, Record<string, LogicResultDoc[]>>>((result, [key, value]) => {
      if (key.startsWith(userDocPath)) {
        result.userDocsByDstPath[key] = value;
      } else {
        result.otherUsersDocsByDstPath[key] = value;
      }
      return result;
    }, {userDocsByDstPath: {}, otherUsersDocsByDstPath: {}});
  return {userDocsByDstPath, otherUsersDocsByDstPath};
}

export function getSecurityFn(entity: string): SecurityFn {
  return securityConfig[entity];
}


export function createLogicConfigsFromViewDefinitions(viewDefinitions: ViewDefinition[]): LogicConfig[] {
  const logicConfigs: LogicConfig[] = [];
  for (const viewDef of viewDefinitions) {
    const {destEntity, destProp, srcProps, srcEntity} = viewDef;
    logicConfigs.push({
      name: `${destEntity}${destProp ? `#${destProp}` : ""} view updater`,
      actionTypes: "all",
      modifiedFields: srcProps,
      entities: [srcEntity],
      logicFn: createViewLogicFn(viewDef),
    });
  }

  return logicConfigs;
}
