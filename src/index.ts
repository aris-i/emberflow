import * as functions from "firebase-functions";
import {Action, FirebaseAdmin, LogicConfig, SecurityConfig, ValidatorConfig} from "./types";
import {
  delayFormSubmissionAndCheckIfCancelled,
  distribute,
  getFormModifiedFields,
  getSecurityFn,
  groupDocsByUserAndDstPath,
  revertModificationsOutsideForm,
  runBusinessLogics,
  validateForm,
} from "./index-utils";
import {initDbStructure} from "./db-structure";

export let admin: FirebaseAdmin;
export let dbStructure: Record<string, object>;
export let Entity: Record<string, string>;
export let securityConfig: SecurityConfig;
export let validatorConfig: ValidatorConfig;
export let logics: LogicConfig[];
export let docPaths: Record<string, string>;
export let colPaths: Record<string, string>;
export let docPathsRegex: Record<string, RegExp>;

export function initializeEmberFlow(
  adminInstance: FirebaseAdmin,
  customDbStructure: Record<string, object>,
  CustomEntity: Record<string, string>,
  customSecurityConfig: SecurityConfig,
  customValidatorConfig: ValidatorConfig,
  customLogics: LogicConfig[],
) {
  admin = adminInstance;
  dbStructure = customDbStructure;
  Entity = CustomEntity;
  securityConfig = customSecurityConfig;
  validatorConfig = customValidatorConfig;
  logics = customLogics;

  const {docPaths: dp, colPaths: cp, docPathsRegex: dbr} = initDbStructure(dbStructure, Entity);
  docPaths = dp;
  colPaths = cp;
  docPathsRegex = dbr;

  Object.values(docPaths).forEach((path) => {
    const parts = path.split("/");
    const entity = parts[parts.length - 1].replace(/{(\w+)Id}$/, "$1");

    exports[`on${entity.charAt(0).toUpperCase() + entity.slice(1)}Create`] = functions.firestore
      .document(path)
      .onCreate(async (snapshot, context) => {
        await onDocChange(entity, {before: null, after: snapshot}, context, "create");
      });

    exports[`on${entity.charAt(0).toUpperCase() + entity.slice(1)}Update`] = functions.firestore
      .document(path)
      .onUpdate(async (change, context) => {
        await onDocChange(entity, change, context, "update");
      });

    exports[`on${entity.charAt(0).toUpperCase() + entity.slice(1)}Delete`] = functions.firestore
      .document(path)
      .onDelete(async (snapshot, context) => {
        await onDocChange(entity, {before: snapshot, after: null}, context, "delete");
      });
  });

  return {docPaths, colPaths, docPathsRegex};
}

export async function onDocChange(
  entity: string,
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

  // if not form.@status is submit then return
  if (document?.["@form"]?.["@status"] !== "submit") {
    console.log("Form is not submitted");
    return;
  }

  // Create Action document
  const actionType = document["@form"]?.["@actionType"];
  if (!actionType) {
    console.log("No actionType found");
    return;
  }

  // Validate the document
  const [hasValidationError, validationResult] = validateForm(entity, document);
  if (hasValidationError) {
    await snapshot.ref.update({"@form.@status": "form-validation-failed", "@form.@message": validationResult});
    return;
  }

  const formModifiedFields = getFormModifiedFields(document);
  // Run security check
  const securityFn = getSecurityFn(entity);
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

  const logicResults = await runBusinessLogics(actionType, formModifiedFields, entity, action);
  // Save all logic results under logicResults collection of action document
  await Promise.all(logicResults.map(async (result) => {
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

