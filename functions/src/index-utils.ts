import {Action, ActionType, LogicConfig, LogicResult, LogicResultDoc} from "./types";
import {Entity} from "./custom/db-structure";
import {logics} from "./custom/business-logics";
import {validators} from "./custom/validators";
import {docPaths} from "./init-db-structure";
import * as admin from "firebase-admin";
import {firestore} from "firebase-admin";
import * as functions from "firebase-functions";
import {securityConfig} from "./custom/security";
import {expandAndGroupDocPaths} from "./utils";
import DocumentData = firestore.DocumentData;
import FieldPath = firestore.FieldPath;

async function fetchIds(collectionPath: string) {
  const ids: string[] = [];
  const querySnapshot = await admin.firestore().collection(collectionPath).select(FieldPath.documentId()).get();
  querySnapshot.forEach((doc) => {
    ids.push(doc.id);
  });
  return ids;
}

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
        const dstColPath = dstPath.endsWith("#") ? dstPath.slice(0, -2) : null;
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
      const subDocPaths = expandAndGroupDocPaths(srcPath, fetchIds);
      const pathsToCopy: string[] = [];
      for (const [entity, paths] of Object.entries(subDocPaths)) {
        if (!skipEntityDuringRecursiveCopy || !skipEntityDuringRecursiveCopy.includes(entity as Entity)) {
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
}

export async function revertModificationsOutsideForm(document: FirebaseFirestore.DocumentData, beforeDocument: FirebaseFirestore.DocumentData | null | undefined, snapshot: FirebaseFirestore.DocumentSnapshot) {
  // Revert any changes made to document other than @form
  const revertedValues: Record<string, any> = {};

  if (beforeDocument) {
    const modifiedFields = Object.keys(document ?? {}).filter((key) => !key.startsWith("@form"));
    modifiedFields.forEach((field) => {
      if (document?.[field] !== beforeDocument[field]) {
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

export function validateForm(entity: Entity, document: DocumentData) {
  let hasValidationError = false;
  const validate = validators[entity];
  const validationResult = validate(document);

  // Check if validation failed
  if (validationResult && Object.keys(validationResult).length > 0) {
    console.log(`Document validation failed: ${JSON.stringify(validationResult)}`);
    hasValidationError = true;
  }
  return {hasValidationError, validationResult};
}

export function getFormModifiedFields(document: DocumentData) {
  const formFields = Object.keys(document?.["@form"] ?? {}).filter((key) => !key.startsWith("@"));
  // compare value of each @form field with the value of the same field in the document to get modified fields
  return formFields.filter((field) => document?.[field] !== document?.["@form"]?.[field]);
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

export async function runBusinessLogics(actionType: ActionType, formModifiedFields: string[], entity: Entity, action: Action, logics: LogicConfig[]) {
  const matchingLogics = logics.filter((logic) => {
    return (
      (logic.actionTypes === "all" || logic.actionTypes.includes(actionType)) &&
            (logic.modifiedFields === "all" || logic.modifiedFields.some((field) => formModifiedFields?.includes(field))) &&
            (logic.entities === "all" || logic.entities.includes(entity))
    );
  });
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

  const userDocPath = docPaths[Entity.User].replace("{userId}", userId);
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

export async function onDocChange(
  entity: Entity,
  change: functions.Change<functions.firestore.DocumentSnapshot | null>,
  context: functions.EventContext,
  event: "create" | "update" | "delete"
) {
  if (!context.auth) {
    console.log("Auth is null, then this change is initiated by the service account and should be ignored");
    return;
  }
  const userId = context.auth.uid;
  if (!userId) {
    console.info("User ID is null, then this change is initiated by the service account and should be ignored");
    return;
  }

  const afterDocument = change.after ? change.after.data() : null;
  const beforeDocument = change.before ? change.before.data() : null;
  const document = afterDocument || beforeDocument;
  const snapshot = change.after || change.before;
  if (!snapshot || !document) {
    console.error("Snapshot or document should not be null");
    return;
  }
  const documentId = snapshot.ref.id;
  console.log(`Document ${event}d in ${context.resource.service.split("/")[6]} collection with ID ${documentId}`);
  console.log("After Document data: ", afterDocument);
  console.log("Before document data: ", beforeDocument);

  // Re save document if deleted
  if (event === "delete") {
    // Re-add deleted document
    await snapshot.ref.set(document);
    console.log(`Document re-added with ID ${documentId}`);
    return;
  }

  if (afterDocument) {
    await revertModificationsOutsideForm(afterDocument, beforeDocument, snapshot);
  }

  // Validate the document
  const {hasValidationError, validationResult} = validateForm(entity, document);
  if (hasValidationError) {
    await snapshot.ref.update({"@form.@status": "form-validation-failed", "@form.@message": validationResult});
    return;
  }

  const formModifiedFields = getFormModifiedFields(document);
  // Run security check
  const securityFn = securityConfig[entity];
  if (securityFn) {
    const securityResult = await securityFn(entity, document, event, formModifiedFields);
    if (securityResult.status === "rejected") {
      console.log(`Security check failed: ${securityResult.message}`);
      await snapshot.ref.update({"@form.@status": "security-error", "@form.@message": securityResult.message});
      return;
    }
  }

  // Check for delay
  const delay = document?.["@form"]?.["@delay"];
  if (delay) {
    if (await delayFormSubmissionAndCheckIfCancelled(delay, snapshot)) {
      await snapshot.ref.update({"@form.@status": "cancelled"});
      return;
    }
  }

  await snapshot.ref.update({"@form.@status": "processing"});

  // Create Action document
  const actionType = document["@form"]?.["@actionType"];
  if (!actionType) {
    return;
  }

  const path = snapshot.ref.path;
  const status = "processing";
  const timeCreated = admin.firestore.Timestamp.now();

  const action: Action = {
    actionType,
    path,
    document,
    status,
    timeCreated,
    modifiedFields: formModifiedFields,
  };
  const actionRef = await admin.firestore().collection("actions").add(action);

  await snapshot.ref.update({"@form.@status": "submitted"});

  const logicResults = await runBusinessLogics(actionType, formModifiedFields, entity, action, logics);
  // Save all logic results under logicResults collection of action document
  await Promise.all( logicResults.map(async (result) => {
    const {documents, ...logicResult} = result;
    const logicResultRef = await actionRef.collection("logicResults").add(logicResult);
    await Promise.all(documents.map(async (doc) => {
      await logicResultRef.collection("documents").add(doc);
    }));
  }));

  const errorLogicResults = logicResults.filter((result) => result.status === "error");
  if (errorLogicResults.length > 0) {
    const errorMessage = errorLogicResults.map((result) => result.message).join("\n");
    await actionRef.update({status: "finished-with-error", message: errorMessage});
  }
  const {userDocsByDstPath, otherUsersDocsByDstPath} = groupDocsByUserAndDstPath(logicResults, userId);

  await distribute(userDocsByDstPath);
  await snapshot.ref.update({"@form.@status": "finished"});
  await distribute(otherUsersDocsByDstPath);
  await actionRef.update({status: "finished"});
}
