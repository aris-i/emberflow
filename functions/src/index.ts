import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {docPaths, Entity} from "./db-structure";
import {validators} from "./validators";
import {Action} from "./types";

admin.initializeApp();

async function onDocChange(
  entity: Entity,
  change: functions.Change<functions.firestore.DocumentSnapshot | null>,
  context: functions.EventContext,
  event: "create" | "update" | "delete"
) {
  if (!context.auth) {
    console.log("Auth is null, then this change is initiated by the service account and should be ignored");
    return;
  }

  const documentId = context.params[Object.keys(context.params)[Object.keys(context.params).length - 1]];
  const document = change.after ? change.after.data() : null;
  const beforeDocument = change.before ? change.before.data() : null;
  const snapshot = change.after || change.before;
  if (!snapshot) {
    return;
  }

  console.log(`Document ${event}d in ${context.resource.service.split("/")[6]} collection with ID ${documentId}`);
  console.log("Document data: ", document);
  console.log("Before document data: ", beforeDocument);

  // Check if @form exists in document
  if (!document?.["@form"]) {
    console.log(`@form does not exist in the document with ID ${documentId}`);
    return;
  }

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
    console.log(`Reverting document with ID ${documentId}:\n`, revertedValues);
    await snapshot.ref.update(revertedValues);
  }

  await snapshot.ref.update({"@form.@status": "processing"});

  // Validate the document
  if (document) {
    const validate = validators[entity];
    const validationResult = validate(document);

    // Check if validation failed
    if (validationResult && Object.keys(validationResult).length > 0) {
      console.log(`Document validation failed: ${JSON.stringify(validationResult)}`);
      await snapshot.ref.update({"@form.@status": "form-validation-failed", "@form.@message": validationResult});
      return;
    }
  }

  // Check for delay
  const delay = document?.["@delay"];
  if (delay) {
    console.log(`Delaying document for ${delay}ms...`);
    await snapshot.ref.update({"@form.@status": "delay"});
    await new Promise((resolve) => setTimeout(resolve, delay));
    // Re-fetch document from Firestore
    const updatedSnapshot = await snapshot.ref.get();
    const updatedDocument = updatedSnapshot.data();
    console.log(`Re-fetched document from Firestore after delay with ID ${documentId}:\n`, updatedDocument);
    // Check if form status is "cancel"
    if (updatedDocument?.["@form"]?.["@status"] === "cancel") {
      await snapshot.ref.update({"@form.@status": "cancelled"});
      return;
    }
  }

  // Create Action document
  const actionType = document?.["@form"]?.["@actionType"];
  if (!actionType) {
    return;
  }

  const path = snapshot.ref.path;
  const status = "new";
  const timeCreated = admin.firestore.Timestamp.now();

  // get all @form fields that doesn't start with @
  const formFields = Object.keys(document?.["@form"] ?? {}).filter((key) => !key.startsWith("@"));
  // compare value of each @form field with the value of the same field in the document to get modified fields
  const modifiedFields = formFields.filter((field) => document?.[field] !== document?.["@form"]?.[field]);

  const action: Action = {
    actionType,
    path,
    document,
    status,
    timeCreated,
    modifiedFields,
  };

  await admin.firestore().collection("actions").add(action);

  await snapshot.ref.update({"@form.@status": "submitted"});
}


Object.values(docPaths).forEach((path) => {
  const parts = path.split("/");
  const colPath = parts[parts.length - 1].replace(/{(\w+)Id}$/, "$1");
  const entity = colPath as Entity;

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Create`] = functions.firestore
    .document(path)
    .onCreate(async (snapshot, context) => {
      await onDocChange(entity, {before: null, after: snapshot}, context, "create");
    });

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Update`] = functions.firestore
    .document(path)
    .onUpdate(async (change, context) => {
      await onDocChange(entity, change, context, "update");
    });

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Delete`] = functions.firestore
    .document(path)
    .onDelete(async (snapshot, context) => {
      await onDocChange(entity, {before: snapshot, after: null}, context, "delete");
    });
});
