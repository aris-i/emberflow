import * as functions from "firebase-functions";
import {
  Action,
  EventContext,
  FirebaseAdmin,
  LogicConfig,
  LogicResultDoc,
  ProjectConfig,
  SecurityConfig,
  ValidatorConfig,
  ViewDefinition,
  ViewLogicConfig,
} from "./types";
import {
  consolidateAndGroupByDstPath,
  delayFormSubmissionAndCheckIfCancelled,
  distribute,
  getFormModifiedFields,
  getSecurityFn,
  groupDocsByUserAndDstPath,
  onDeleteFunction,
  processScheduledEntities,
  runBusinessLogics,
  runPeerSyncViews,
  runViewLogics,
  validateForm,
} from "./index-utils";
import {initDbStructure} from "./init-db-structure";
import {createViewLogicFn} from "./logics/view-logics";
import {resetUsageStats, stopBillingIfBudgetExceeded, useBillProtect} from "./utils/bill-protect";
import {Firestore} from "firebase-admin/firestore";
import {DatabaseEvent, DataSnapshot, onValueCreated} from "firebase-functions/v2/database";
import {parseEntity} from "./utils/paths";
import {database} from "firebase-admin";
import Database = database.Database;


export let admin: FirebaseAdmin;
export let db: Firestore;
export let rtdb: Database;
export let dbStructure: Record<string, object>;
export let Entity: Record<string, string>;
export let securityConfig: SecurityConfig;
export let validatorConfig: ValidatorConfig;
export let logicConfigs: LogicConfig[];
export let docPaths: Record<string, string>;
export let colPaths: Record<string, string>;
export let docPathsRegex: Record<string, RegExp>;
export let viewLogicConfigs: ViewLogicConfig[];
export let projectConfig: ProjectConfig;
export const functionsConfig: Record<string, any> = {};

export const _mockable = {
  createNowTimestamp: () => admin.firestore.Timestamp.now(),
  initActionRef,
};

export function initializeEmberFlow(
  customProjectConfig: ProjectConfig,
  adminInstance: FirebaseAdmin,
  customDbStructure: Record<string, object>,
  CustomEntity: Record<string, string>,
  customSecurityConfig: SecurityConfig,
  customValidatorConfig: ValidatorConfig,
  customLogicConfigs: LogicConfig[],
) : {
  docPaths: Record<string, string>;
  colPaths: Record<string, string>;
  docPathsRegex: Record<string, RegExp>;
  functionsConfig: Record<string, any>,
} {
  projectConfig = customProjectConfig;
  admin = adminInstance;
  db = admin.firestore();
  rtdb = admin.database();
  dbStructure = customDbStructure;
  Entity = CustomEntity;
  securityConfig = customSecurityConfig;
  validatorConfig = customValidatorConfig;
  logicConfigs = customLogicConfigs;

  const {
    docPaths: dp,
    colPaths: cp,
    docPathsRegex: dbr,
    viewDefinitions: vd,
  } = initDbStructure(dbStructure, Entity);
  docPaths = dp;
  colPaths = cp;
  docPathsRegex = dbr;

  viewLogicConfigs = vd.map((viewDef: ViewDefinition): ViewLogicConfig => {
    return {
      name: `${viewDef.destEntity}${viewDef.destProp ? "#" + viewDef.destProp : ""} ViewLogic`,
      entity: viewDef.srcEntity,
      modifiedFields: viewDef.srcProps,
      viewLogicFn: createViewLogicFn(viewDef),
    };
  });

  functionsConfig["onFormSubmit"] = onValueCreated(
    {
      ref: "forms/{userId}/{formId}",
      region: projectConfig.region,
    },
    useBillProtect(onFormSubmit)
  );


  functionsConfig["onBudgetAlert"] =
        functions.pubsub.topic(projectConfig.budgetAlertTopicName).onPublish(stopBillingIfBudgetExceeded);
  functionsConfig["hourlyFunctions"] = functions.pubsub.schedule("every 1 hours")
    .onRun(resetUsageStats);
  functionsConfig["minuteFunctions"] = functions.pubsub.schedule("every 1 minutes")
    .onRun(processScheduledEntities);
  functionsConfig["onDeleteFunctions"] = functions.firestore.document("@server/delete/functions/{deleteFuncId}").onCreate(
    onDeleteFunction);

  return {docPaths, colPaths, docPathsRegex, functionsConfig};
}

async function initActionRef(actionId: string) {
  return db.collection("actions").doc(actionId);
}

export async function onFormSubmit(
  event: DatabaseEvent<DataSnapshot>,
) {
  console.log("Running onFormSubmit");
  const {userId, formId} = event.params;
  const formSnapshot = event.data;
  const formRef = formSnapshot.ref;

  try {
    const form = formSnapshot.val();
    console.log("form", form);

    console.info("Validating docPath");
    const docPath = form["@docPath"] as string;
    const {entity, entityId} = parseEntity(docPath);
    if (!entity) {
      const message = "docPath does not match any known Entity";
      console.warn(message);
      await formRef.update({"@status": "error", "@message": message});
      return;
    }

    console.info("Validating userId");
    if (!docPath.startsWith(`users/${userId}`)) {
      const message = "User id from path does not match user id from event params";
      console.warn(message);
      await formRef.update({"@status": "error", "@message": message});
      return;
    }

    // Create Action form
    console.info("Validating @actionType");
    const actionType = form["@actionType"];
    if (!actionType) {
      const message = "No @actionType found";
      console.warn(message);
      await formRef.update({"@status": "error", "@message": message});
      return;
    }

    // Validate the form
    console.info("Validating form");
    const [hasValidationError, validationResult] = await validateForm(entity, form);
    if (hasValidationError) {
      await formRef.update({"@status": "validation-error", "@message": validationResult});
      return;
    }

    console.info("Validating Security");
    const document = (await db.doc(docPath).get()).data() || {};
    const formModifiedFields = getFormModifiedFields(form, document);
    const user = (await db.doc(`users/${userId}`).get()).data();
    if (!user) {
      const message = "No user data found";
      console.warn(message);
      await formRef.update({"@status": "error", "@message": message});
      return;
    }
    // Run security check
    const securityFn = getSecurityFn(entity);
    if (securityFn) {
      const securityResult = await securityFn(entity, form, document,
        actionType, formModifiedFields, user);
      if (securityResult.status === "rejected") {
        console.log(`Security check failed: ${securityResult.message}`);
        await formRef.update({"@status": "security-error", "@message": securityResult.message});
        return;
      }
    }

    // Check for delay
    console.info("Checking for delay");
    const delay = form["@delay"];
    if (delay) {
      if (await delayFormSubmissionAndCheckIfCancelled(delay, formRef)) {
        await formRef.update({"@status": "cancelled"});
        return;
      }
    }

    await formRef.update({"@status": "processing"});

    const status = "processing";
    const timeCreated = _mockable.createNowTimestamp();

    console.info("Creating Action");
    const eventContext: EventContext = {
      id: event.id,
      uid: userId,
      formId,
      docId: entityId,
      docPath,
      entity,
    };

    const action: Action = {
      eventContext,
      actionType,
      document,
      form,
      status,
      timeCreated,
      modifiedFields: formModifiedFields,
      user,
    };
    const actionRef = await _mockable.initActionRef(formId);
    await actionRef.set(action);

    await formRef.update({"@status": "submitted"});

    console.info("Running Business Logics");
    const logicResults = await runBusinessLogics(actionType, formModifiedFields, entity, action);
    // Save all logic results under logicResults collection of action form
    for (let i = 0; i < logicResults.length; i++) {
      const {documents, ...logicResult} = logicResults[i];
      const logicResultsRef = actionRef.collection("logicResults")
        .doc(`${actionRef.id}-${i}`);
      await logicResultsRef.set(logicResult);
      const documentsRef = logicResultsRef.collection("documents");
      for (let j = 0; j < documents.length; j++) {
        await documentsRef.doc(`${logicResultsRef.id}-${j}`).set(documents[j]);
      }
    }

    const errorLogicResults = logicResults.filter((result) => result.status === "error");
    if (errorLogicResults.length > 0) {
      const errorMessage = errorLogicResults.map((result) => result.message).join("\n");
      await actionRef.update({status: "finished-with-error", message: errorMessage});
    }

    console.info("Consolidating Logic Results");
    const dstPathLogicDocsMap: Map<string, LogicResultDoc> = consolidateAndGroupByDstPath(logicResults);
    const {
      userDocsByDstPath,
      otherUsersDocsByDstPath,
    } = groupDocsByUserAndDstPath(dstPathLogicDocsMap, userId);

    console.info("Running View Logics");
    const viewLogicResults = await runViewLogics(userDocsByDstPath);
    const dstPathViewLogicDocsMap: Map<string, LogicResultDoc> = consolidateAndGroupByDstPath(viewLogicResults);
    const {
      userDocsByDstPath: userViewDocsByDstPath,
      otherUsersDocsByDstPath: otherUsersViewDocsByDstPath,
    } = groupDocsByUserAndDstPath(dstPathViewLogicDocsMap, userId);

    console.info("Distributing Logic Results for initiating user");
    await distribute(userDocsByDstPath);
    await distribute(userViewDocsByDstPath);
    await formRef.update({"@status": "finished"});

    console.info("Running Peer Sync Views");
    const peerSyncViewLogicResults = await runPeerSyncViews(userDocsByDstPath);
    const dstPathPeerSyncViewLogicDocsMap: Map<string, LogicResultDoc> = consolidateAndGroupByDstPath(peerSyncViewLogicResults);
    const {otherUsersDocsByDstPath: otherUsersPeerSyncViewDocsByDstPath} = groupDocsByUserAndDstPath(dstPathPeerSyncViewLogicDocsMap, userId);

    console.info("Distributing Logic Results for other users");
    await distribute(otherUsersDocsByDstPath);
    await distribute(otherUsersViewDocsByDstPath);
    await distribute(otherUsersPeerSyncViewDocsByDstPath);

    await actionRef.update({status: "finished"});
    console.info("Finished");
  } catch (error) {
    console.error("Error in onFormSubmit", error);
    await formRef.update({"@status": "error", "@message": error});
  }
}
