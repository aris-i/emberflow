import {Action, ActionType, LogicConfig, LogicResult, LogicResultDoc} from "./types";
import {Entity} from "./custom/db-structure";
import {logics} from "./custom/business-logics";
import {validators} from "./custom/validators";
import {docPaths} from "./init-db-structure";
import * as admin from "firebase-admin";
import {firestore} from "firebase-admin";
import * as functions from "firebase-functions";
import {securityConfig} from "./custom/security";
import DocumentData = firestore.DocumentData;

export async function distribute(userDocsByDstPath: Record<string, LogicResultDoc[]>) {
  const db = admin.firestore();

  for (const [dstPath, resultDocs] of Object.entries(userDocsByDstPath).sort()) {
    console.log(`Documents for path ${dstPath}:`);
    for (const resultDoc of resultDocs) {
      const {doc, instructions} = resultDoc;
      if (typeof doc === "string") {
        // Copy document to dstPath
        const snapshot = await db.doc(doc).get();
        if (!snapshot.exists) {
          console.log(`Document ${doc} does not exist`);
          continue;
        }
        const data = snapshot.data();
        if (!data) {
          console.log(`Document ${doc} has no data`);
          continue;
        }
        await db.doc(dstPath).set(data);
        console.log(`Document copied from ${doc} to ${dstPath}`);
      } else if (doc === null) {
        // Delete document at dstPath
        await db.doc(dstPath).delete();
        console.log(`Document deleted at ${dstPath}`);
      } else {
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
        await db.doc(dstPath).set(updateData, {merge: true});
        console.log(`Document merged to ${dstPath}`);
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
