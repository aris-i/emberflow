import * as functions from "firebase-functions/v1";
import {
  Action,
  EventContext,
  FirebaseAdmin,
  Instructions,
  LogicConfig,
  LogicConfigModifiedFieldsType,
  LogicResult,
  LogicResultDoc,
  LogicResultDocAction, MetricExecution,
  PatchLogicConfig,
  ProjectConfig,
  RunBusinessLogicStatus,
  SecurityConfig,
  UserRegisterFn,
  ValidatorConfig,
  ViewDefinition,
  ViewLogicConfig,
} from "./types";
import {
  _mockable as indexUtilsMockable,
  cleanMetricComputations,
  cleanMetricExecutions,
  convertLogicResultsToMetricExecutions,
  createMetricComputation,
  createMetricLogicDoc,
  delayFormSubmissionAndCheckIfCancelled,
  distributeFnNonTransactional,
  distributeFnTransactional,
  distributeLater,
  expandConsolidateAndGroupByDstPath,
  getFormModifiedFields,
  getSecurityFn,
  groupDocsByTargetDocPath,
  onDeleteFunction,
  runBusinessLogics,
  validateForm,
} from "./index-utils";
import {initDbStructure} from "./init-db-structure";
import {createViewLogicFn, findMatchingViewLogics, onMessageViewLogicsQueue, queueRunViewLogics} from "./logics/view-logics";
import {resetUsageStats, stopBillingIfBudgetExceeded, useBillProtect} from "./utils/bill-protect";
import {Firestore} from "firebase-admin/firestore";
import {DatabaseEvent, DataSnapshot, onValueCreated} from "firebase-functions/v2/database";
import {findMatchingDocPathRegex, parseEntity} from "./utils/paths";
import {database} from "firebase-admin";
import {initClient} from "emberflow-admin-client/lib";
import {internalDbStructure, InternalEntity} from "./db-structure";
import {cleanActionsAndForms, onMessageSubmitFormQueue} from "./utils/forms";
import {PubSub, Topic} from "@google-cloud/pubsub";
import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {logMemoryUsage, reviveDateAndTimestamp, trimStrings} from "./utils/misc";
import {instructionsReducer, onMessageForDistributionQueue, onMessageInstructionsQueue} from "./utils/distribution";
import {cleanPubSubProcessedIds} from "./utils/pubsub";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {onDocumentCreated} from "firebase-functions/v2/firestore";
import {UserRecord} from "firebase-admin/lib/auth";
import {debounce} from "./utils/functions";
import {extractTransactionGetOnly} from "./utils/transaction";
import {
  findMatchingPatchLogicsByEntity,
  onMessageRunPatchLogicsQueue,
  queueRunPatchLogics,
  versionCompare,
} from "./logics/patch-logics";
import Database = database.Database;

export let admin: FirebaseAdmin;
export let db: Firestore;
export let rtdb: Database;
export let pubsub: PubSub;
export let dbStructure: Record<string, object>;
export let Entity: Record<string, string>;
export let securityConfigs: SecurityConfig[];
export let validatorConfigs: ValidatorConfig[];
export let logicConfigs: LogicConfig[];
export let patchLogicConfigs: PatchLogicConfig[];
export let docPaths: Record<string, string>;
export let colPaths: Record<string, string>;
export let docPathsRegex: Record<string, RegExp>;
export let viewLogicConfigs: ViewLogicConfig[];
export let projectConfig: ProjectConfig;
export const functionsConfig: Record<string, any> = {};
export const SUBMIT_FORM_TOPIC_NAME = "submit-form-queue";
export const VIEW_LOGICS_TOPIC_NAME = "view-logics-queue";
export const PATCH_LOGICS_TOPIC_NAME = "patch-logics-queue";
export const FOR_DISTRIBUTION_TOPIC_NAME = "for-distribution-queue";
export const INSTRUCTIONS_TOPIC_NAME = "instructions-queue";
export const pubSubTopics = [
  SUBMIT_FORM_TOPIC_NAME,
  VIEW_LOGICS_TOPIC_NAME,
  PATCH_LOGICS_TOPIC_NAME,
  FOR_DISTRIBUTION_TOPIC_NAME,
  INSTRUCTIONS_TOPIC_NAME,
];
export let SUBMIT_FORM_TOPIC: Topic;
export let VIEW_LOGICS_TOPIC: Topic;
export let PATCH_LOGICS_TOPIC: Topic;
export let FOR_DISTRIBUTION_TOPIC: Topic;
export let INSTRUCTIONS_TOPIC: Topic;
let userRegisterFn: UserRegisterFn | undefined;

export const _mockable = {
  createNowTimestamp: () => admin.firestore.Timestamp.now(),
  initActionRef,
};

export function initializeEmberFlow(
  customProjectConfig: ProjectConfig,
  adminInstance: FirebaseAdmin,
  customDbStructure: Record<string, object>,
  CustomEntity: Record<string, string>,
  customSecurityConfigs: SecurityConfig[],
  customValidatorConfigs: ValidatorConfig[],
  customLogicConfigs: LogicConfig[],
  customPatchLogicConfigs: PatchLogicConfig[],
  customUserRegisterFn?: UserRegisterFn,
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
  pubsub = new PubSub();
  dbStructure = {...customDbStructure, ...internalDbStructure};
  Entity = {...CustomEntity, ...InternalEntity};
  securityConfigs = [...customSecurityConfigs];
  validatorConfigs = [...customValidatorConfigs];
  logicConfigs = [...customLogicConfigs];
  patchLogicConfigs = [...customPatchLogicConfigs];
  userRegisterFn = customUserRegisterFn;
  initClient(admin.app(), "service", "0.0.0");
  SUBMIT_FORM_TOPIC = pubsub.topic(SUBMIT_FORM_TOPIC_NAME);
  VIEW_LOGICS_TOPIC = pubsub.topic(VIEW_LOGICS_TOPIC_NAME);
  PATCH_LOGICS_TOPIC = pubsub.topic(PATCH_LOGICS_TOPIC_NAME);
  FOR_DISTRIBUTION_TOPIC = pubsub.topic(FOR_DISTRIBUTION_TOPIC_NAME);
  INSTRUCTIONS_TOPIC = pubsub.topic(INSTRUCTIONS_TOPIC_NAME);

  const {
    docPaths: dp,
    colPaths: cp,
    docPathsRegex: dbr,
    viewDefinitions: vd,
  } = initDbStructure(dbStructure, Entity);
  docPaths = dp;
  colPaths = cp;
  docPathsRegex = dbr;

  viewLogicConfigs = vd.map((viewDef: ViewDefinition): ViewLogicConfig[] => {
    const {destEntity, destProp} = viewDef;
    const {name: destPropName} = destProp || {};
    const [srcToDstViewLogicFn, dstToSrcViewLogicFn] = createViewLogicFn(viewDef);
    const srcToDstLogicConfig = {
      name: `${destEntity}${destPropName ? `#${destPropName}` : ""} ViewLogic`,
      entity: viewDef.srcEntity,
      actionTypes: ["create", "merge", "delete"] as LogicResultDocAction[],
      modifiedFields: viewDef.srcProps,
      viewLogicFn: srcToDstViewLogicFn,
      version: viewDef.version,
    };
    const dstToSrcLogicConfig = {
      name: `${destEntity}${destPropName ? `#${destPropName}` : ""} Reverse ViewLogic`,
      entity: viewDef.destEntity,
      actionTypes: ["create", "delete"] as LogicResultDocAction[],
      modifiedFields: "all" as LogicConfigModifiedFieldsType,
      ...(viewDef.destProp ? {destProp: viewDef.destProp.name} : {}),
      viewLogicFn: dstToSrcViewLogicFn,
      version: viewDef.version,
    } as ViewLogicConfig;
    return [srcToDstLogicConfig, dstToSrcLogicConfig];
  }).flat();

  const logicNames = logicConfigs.map((config) => config.name);
  const viewLogicNames = viewLogicConfigs.map((config) => config.name);
  const allLogicNames = [...logicNames, ...viewLogicNames];
  allLogicNames.forEach(createMetricLogicDoc);

  functionsConfig["onFormSubmit"] = onValueCreated(
    {
      ref: "forms/{userId}/{formId}",
      region: projectConfig.region,
      memory: "512MiB",
      concurrency: 80,
      maxInstances: 20,
      ...projectConfig.functionsConfig?.onFormSubmit as any,
    },
    useBillProtect(onFormSubmit)
  );

  functionsConfig["onBudgetAlert"] =
        functions.pubsub.topic(projectConfig.budgetAlertTopicName).onPublish(stopBillingIfBudgetExceeded);
  functionsConfig["onMessageSubmitFormQueue"] = onMessagePublished({
    topic: SUBMIT_FORM_TOPIC_NAME,
    region: projectConfig.region,
    memory: "512MiB",
    maxInstances: 5,
    timeoutSeconds: 540,
    retry: true,
    ...projectConfig.functionsConfig?.onMessageSubmitFormQueue as any,
  }, onMessageSubmitFormQueue);
  functionsConfig["onMessageViewLogicsQueue"] = onMessagePublished({
    topic: VIEW_LOGICS_TOPIC_NAME,
    region: projectConfig.region,
    memory: "512MiB",
    maxInstances: 5,
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.onMessageViewLogicsQueue as any,
  }, onMessageViewLogicsQueue);
  functionsConfig["onMessagePatchLogicsQueue"] = onMessagePublished({
    topic: PATCH_LOGICS_TOPIC_NAME,
    region: projectConfig.region,
    memory: "512MiB",
    maxInstances: 5,
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.onMessagePatchLogicsQueue as any,
  }, onMessageRunPatchLogicsQueue);
  functionsConfig["onMessageForDistributionQueue"] = onMessagePublished({
    topic: FOR_DISTRIBUTION_TOPIC_NAME,
    region: projectConfig.region,
    memory: "512MiB",
    maxInstances: 5,
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.onMessageForDistributionQueue as any,
  }, onMessageForDistributionQueue);
  functionsConfig["onMessageInstructionsQueue"] = onMessagePublished({
    topic: INSTRUCTIONS_TOPIC_NAME,
    region: projectConfig.region,
    memory: "512MiB",
    maxInstances: 1,
    concurrency: 1,
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.onMessageInstructionsQueue as any,
  }, debounce(
    onMessageInstructionsQueue,
    200,
    1000,
    {
      reducerFn: instructionsReducer,
      initialValueFactory: () => {
        return new Map<string, Instructions>();
      },
    }
  ));
  functionsConfig["resetUsageStats"] = onSchedule({
    schedule: "every 1 hours",
    region: projectConfig.region,
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.resetUsageStats as any,
  }, resetUsageStats);
  functionsConfig["cleanPubSubProcessedIds"] = onSchedule({
    schedule: "every 1 hours",
    region: projectConfig.region,
    memory: "512MiB",
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.cleanPubSubProcessedIds as any,
  }, cleanPubSubProcessedIds);
  functionsConfig["cleanMetricComputations"] = onSchedule({
    schedule: "every 24 hours",
    region: projectConfig.region,
    memory: "512MiB",
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.cleanMetricComputations as any,
  }, cleanMetricComputations);
  functionsConfig["cleanMetricExecutions"] = onSchedule({
    schedule: "every 24 hours",
    region: projectConfig.region,
    memory: "512MiB",
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.cleanMetricExecutions as any,
  }, cleanMetricExecutions);
  functionsConfig["cleanActionsAndForms"] = onSchedule({
    schedule: "every 24 hours",
    region: projectConfig.region,
    memory: "512MiB",
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.cleanActionsAndForms as any,
  }, cleanActionsAndForms);
  functionsConfig["createMetricComputation"] = onSchedule({
    schedule: "every 1 hours",
    region: projectConfig.region,
    memory: "512MiB",
    timeoutSeconds: 540,
    ...projectConfig.functionsConfig?.createMetricComputation as any,
  }, createMetricComputation);
  functionsConfig["onDeleteFunctions"] = onDocumentCreated({
    document: "@server/delete/functions/{deleteFuncId}",
    ...projectConfig.functionsConfig?.onDeleteFunctions as any,
  } as any, onDeleteFunction as any);
  const onUserRegisterConfig = projectConfig.functionsConfig?.onUserRegister;
  functionsConfig["onUserRegister"] =
    functions.runWith({
      ...(onUserRegisterConfig?.memory ? {memory: onUserRegisterConfig.memory.replace("MiB", "") as functions.RuntimeOptions["memory"]} : {}),
      ...(onUserRegisterConfig?.timeoutSeconds ? {timeoutSeconds: onUserRegisterConfig.timeoutSeconds} : {}),
      ...(onUserRegisterConfig?.minInstances ? {minInstances: onUserRegisterConfig.minInstances} : {}),
      ...(onUserRegisterConfig?.maxInstances ? {maxInstances: onUserRegisterConfig.maxInstances} : {}),
    }).auth.user().onCreate(onUserRegister);

  return {docPaths, colPaths, docPathsRegex, functionsConfig};
}

function initActionRef(actionId: string) {
  return db.collection("@actions").doc(actionId);
}

export async function onFormSubmit(
  event: DatabaseEvent<DataSnapshot>,
) {
  console.log("Running onFormSubmit");
  const {userId, formId} = event.params;
  logMemoryUsage(`${formId}: Start Running onFormSubmit`);
  const formSnapshot = event.data;
  const formRef = formSnapshot.ref;
  const start = performance.now();
  const logicResults: LogicResult[] = [];

  try {
    const trimmedForm = trimStrings(JSON.parse(formSnapshot.val().formData));
    const form = reviveDateAndTimestamp(trimmedForm);
    console.log("form", form);
    const appVersion = form["@appVersion"];

    console.info("Validating appVersion");
    if (!appVersion) {
      const message = "No appVersion found in metadata";
      console.warn(message);
      await formRef.update({"@status": "error", "@messages": message});
      return;
    }

    console.info("Validating docPath");
    const docPath = form["@docPath"] as string;
    const {entity, entityId} = parseEntity(docPath);
    if (!entity) {
      const message = "docPath does not match any known Entity";
      console.warn(message);
      await formRef.update({"@status": "error", "@messages": message});
      return;
    }

    const isUsersDocPath = docPath.startsWith("users");
    const isServiceAccount = userId === "service";

    console.info(`Validating userId: ${userId}`);
    if (!isServiceAccount && isUsersDocPath && !docPath.startsWith(`users/${userId}`)) {
      const message = "User id from path does not match user id from event params";
      console.warn(message);
      await formRef.update({"@status": "error", "@messages": message});
      return;
    }

    console.info("Validating @actionType");
    const actionType = form["@actionType"];
    if (!actionType) {
      const message = "No @actionType found";
      console.warn(message);
      await formRef.update({"@status": "error", "@messages": message});
      return;
    }

    // Check for delay
    console.info("Checking for delay");
    const delay = form["@delay"];
    if (delay) {
      const cancelled = await delayFormSubmissionAndCheckIfCancelled(delay, formRef);
      if (cancelled) {
        await formRef.update({"@status": "cancelled"});
        return;
      }
    }

    let runBusinessLogicStatus:RunBusinessLogicStatus = {
      status: "running",
      logicResults: [],
    };

    const docsForViewLogics: LogicResultDoc[] = [];
    const pathsForPatchLogics: Set<string> = new Set();
    const actionRef = _mockable.initActionRef(formId);
    let targetVersion = appVersion;
    logMemoryUsage(`${formId}: Starting Transaction`);
    const runTransactionStatus = await db.runTransaction(async (txn) => {
      const docRef = db.doc(docPath);
      const document = (await txn.get(docRef)).data() || {};
      const formModifiedFields = getFormModifiedFields(form, document);

      const dataVersion = document["@dataVersion"] || appVersion;
      targetVersion = versionCompare(appVersion, dataVersion) <= 0 ? appVersion : dataVersion;

      console.info("Validating form");
      const validateFormStart = performance.now();
      const [hasValidationError, validationResult] = await validateForm(entity, form, targetVersion);
      const validateFormEnd = performance.now();
      const validateFormLogicResult: LogicResult = {
        name: "validateForm",
        status: "finished",
        documents: [],
        execTime: validateFormEnd - validateFormStart,
      };
      logicResults.push(validateFormLogicResult);
      if (hasValidationError) {
        await formRef.update({"@status": "validation-error", "@messages": JSON.stringify(validationResult)});
        return "form-validation-error";
      }
      logMemoryUsage(`${formId}: After Validating form`);

      // Let's get the user data
      let user;
      if (!isServiceAccount) {
        const userRef = db.doc(`users/${userId}`);
        user = (await txn.get(userRef)).data();
        if (!user) {
          const message = "No user data found";
          console.warn(message);
          await formRef.update({"@status": "error", "@messages": message});
          return "no-user-data-error";
        }
      } else {
        user = {"@id": userId};
      }

      console.info("Validating Security");
      const txnGet = extractTransactionGetOnly(txn);
      const securityFn = getSecurityFn(entity, targetVersion);
      if (securityFn) {
        const securityFnStart = performance.now();
        const securityResult = await securityFn(txnGet, entity, docPath, document,
          actionType, formModifiedFields, user);
        const securityFnEnd = performance.now();
        const securityLogicResult: LogicResult = {
          name: "securityFn",
          status: "finished",
          documents: [],
          execTime: securityFnEnd - securityFnStart,
        };
        logicResults.push(securityLogicResult);
        if (securityResult.status === "rejected") {
          console.log(`Security check failed: ${securityResult.message}`);
          await formRef.update({"@status": "security-error", "@messages": securityResult.message});
          return "security-error";
        }
      }
      logMemoryUsage(`${formId}: After Validating Security`);

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

      const metadata = form["@metadata"] || {};
      const action: Action = {
        eventContext,
        actionType,
        document,
        status,
        timeCreated,
        modifiedFields: formModifiedFields,
        user,
        metadata,
        appVersion,
      };

      await formRef.update({"@status": "submitted"});
      console.info("Running Business Logics");
      const businessLogicStart = performance.now();
      runBusinessLogicStatus = await runBusinessLogics(txnGet, action, targetVersion);
      const businessLogicEnd = performance.now();
      const runBusinessLogicsMetrics: LogicResult = {
        name: "runBusinessLogics",
        status: "finished",
        documents: [],
        execTime: businessLogicEnd - businessLogicStart,
      };
      logicResults.push(runBusinessLogicsMetrics);
      logMemoryUsage(`${formId}: After Running Business Logics`);

      let errorMessage= "";

      if (runBusinessLogicStatus.status === "no-matching-logics") {
        errorMessage = "No matching logics found";
      }

      const errorLogicResults = runBusinessLogicStatus.logicResults.filter((result) => result.status === "error");
      if (errorLogicResults.length > 0) {
        errorMessage = errorMessage + errorLogicResults.map((result) => result.message).join("\n");
      }

      if (errorMessage) {
        action.status = "processed-with-errors";
      } else {
        action.status = "processed";
      }

      txn.set(actionRef, action);
      async function saveLogicResults() {
        for (let i = 0; i < runBusinessLogicStatus.logicResults.length; i++) {
          const {documents, ...logicResult} = runBusinessLogicStatus.logicResults[i];
          const logicResultsRef = actionRef.collection("logicResults")
            .doc(`${actionRef.id}-${i}`);
          txn.set(logicResultsRef, logicResult);
          const documentsRef = logicResultsRef.collection("documents");
          for (let j = 0; j < documents.length; j++) {
            const docRef = documentsRef.doc(`${logicResultsRef.id}-${j}`);
            txn.set(docRef, documents[j]);
          }
        }
      }
      await saveLogicResults();
      logMemoryUsage(`${formId}: After Saving Logic Logics`);

      const distributeTransactionalLogicResultsStart = performance.now();
      const transactionalLogicResults = await distributeFnTransactional(txn, runBusinessLogicStatus.logicResults);
      for (const doc of transactionalLogicResults) {
        if (findMatchingViewLogics(doc, targetVersion)?.size) {
          docsForViewLogics.push(doc);
        }
        const {entity} = findMatchingDocPathRegex(doc.dstPath);
        if (entity && findMatchingPatchLogicsByEntity(entity, targetVersion).length > 0) {
          pathsForPatchLogics.add(doc.dstPath);
        }
      }
      const distributeTransactionalLogicResultsEnd = performance.now();
      const distributeTransactionalLogicResults: LogicResult = {
        name: "distributeTransactionalLogicResults",
        status: "finished",
        documents: [],
        execTime: distributeTransactionalLogicResultsEnd - distributeTransactionalLogicResultsStart,
      };
      logMemoryUsage(`${formId}: After Distributing Transactional Logic Logics`);
      logicResults.push(distributeTransactionalLogicResults);

      // Clear documents from transactional logic results to free up memory
      if (process.env.NODE_ENV !== "test") {
        for (const result of runBusinessLogicStatus.logicResults) {
          if (result.transactional) {
            result.documents = [];
          }
        }
      }

      return "transaction-complete";
    });
    logMemoryUsage(`${formId}: After Running Transaction`);

    if (runTransactionStatus.includes("error")) {
      console.warn("Error in transaction", runTransactionStatus);
      return;
    }
    const distributeNonTransactionalLogicResultsStart = performance.now();
    const {docsForViewLogics: nonTransactionalDocsForViewLogics, pathsForPatchLogics: nonTransactionalPathsForPatchLogics} = await distributeNonTransactionalLogicResults(
      runBusinessLogicStatus.logicResults, docPath, appVersion, targetVersion,
    );
    docsForViewLogics.push(...nonTransactionalDocsForViewLogics);
    for (const path of nonTransactionalPathsForPatchLogics) {
      pathsForPatchLogics.add(path);
    }

    // Clear documents from non-transactional logic results to free up memory
    if (process.env.NODE_ENV !== "test") {
      for (const result of runBusinessLogicStatus.logicResults) {
        result.documents = [];
      }
    }

    logMemoryUsage(`${formId}: After Distributing Non Transactional Logic Logics`);
    const distributeNonTransactionalLogicResultsEnd = performance.now();
    const distributeNonTransactionalPerfLogicResults: LogicResult = {
      name: "distributeNonTransactionalLogicResults",
      status: "finished",
      documents: [],
      execTime: distributeNonTransactionalLogicResultsEnd - distributeNonTransactionalLogicResultsStart,
    };
    logicResults.push(distributeNonTransactionalPerfLogicResults);

    await formRef.update({"@status": "finished"});

    await queueRunViewLogics(targetVersion, docsForViewLogics);
    logMemoryUsage(`${formId}: After Saving Transactional Logic Logics`);

    await queueRunPatchLogics(
      appVersion,
      ...new Set([docPath, ...pathsForPatchLogics])
    );
    logMemoryUsage(`${formId}: After Queuing Run Patch Logics`);


    const end = performance.now();
    const execTime = end - start;

    await actionRef.update({execTime: execTime});

    const onFormSubmitLogicResult: LogicResult = {
      name: "onFormSubmit",
      status: "finished",
      documents: [],
      execTime,
    };
    logicResults.push(onFormSubmitLogicResult);

    const metricExecutions =
      convertLogicResultsToMetricExecutions(logicResults);
    await indexUtilsMockable.saveMetricExecution(metricExecutions);
    console.info("Finished");
    logMemoryUsage(`${formId}: Finshed onFormSubmit`);
  } catch (error) {
    console.error("Error in onFormSubmit", error);
    const end = performance.now();
    const execTime = end - start;
    const onFormSubmitLogicResult: LogicResult = {
      name: "onFormSubmit",
      status: "finished",
      documents: [],
      execTime,
    };
    logicResults.push(onFormSubmitLogicResult);
    await formRef.update({"@status": "error", "@messages": error, "execTime": execTime});
  }
}

async function distributeNonTransactionalLogicResults(
  logicResults: LogicResult[],
  docPath: string,
  appVersion: string,
  targetVersion: string,
): Promise<{docsForViewLogics: LogicResultDoc[], pathsForPatchLogics: Set<string>}> {
  const docsForViewLogics: LogicResultDoc[] = [];
  const pathsForPatchLogics: Set<string> = new Set();

  const nonTransactionalResults = logicResults.filter((result) => !result.transactional);
  console.info(`Group logic docs by priority: ${nonTransactionalResults.length}`);
  const {highPriorityDocs, normalPriorityDocs, lowPriorityDocs} = nonTransactionalResults
    .map((result) => result.documents)
    .flat()
    .reduce((acc, doc) => {
      if (doc.priority === "high") {
        acc.highPriorityDocs.push(doc);
      } else if (!doc.priority || doc.priority === "normal") {
        acc.normalPriorityDocs.push(doc);
      } else {
        acc.lowPriorityDocs.push(doc);
      }
      return acc;
    }, {
      highPriorityDocs: [] as LogicResultDoc[],
      normalPriorityDocs: [] as LogicResultDoc[],
      lowPriorityDocs: [] as LogicResultDoc[],
    });

  console.info(`Consolidating and Distributing High Priority Logic Results: ${highPriorityDocs.length}`);
  const highPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
      await expandConsolidateAndGroupByDstPath(highPriorityDocs);
  const {
    docsByDocPath: highPriorityDocsByDocPath,
    otherDocsByDocPath: highPriorityOtherDocsByDocPath,
  } = groupDocsByTargetDocPath(highPriorityDstPathLogicDocsMap, docPath);

  const distributedHighPriorityDocs = [
    ...await distributeFnNonTransactional(highPriorityDocsByDocPath),
    ...await distributeFnNonTransactional(highPriorityOtherDocsByDocPath),
  ];
  for (const doc of distributedHighPriorityDocs) {
    if (findMatchingViewLogics(doc, targetVersion)?.size) {
      docsForViewLogics.push(doc);
    }
    const {entity} = findMatchingDocPathRegex(doc.dstPath);
    if (entity && findMatchingPatchLogicsByEntity(entity, targetVersion).length > 0) {
      pathsForPatchLogics.add(doc.dstPath);
    }
  }

  // Clear highPriorityDocs to free up memory
  if (process.env.NODE_ENV !== "test") {
    highPriorityDocs.length = 0;
    highPriorityDstPathLogicDocsMap.clear();
  }

  console.info(`Consolidating and Distributing Normal Priority Logic Results: ${normalPriorityDocs.length}`);
  const normalPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
      await expandConsolidateAndGroupByDstPath(normalPriorityDocs);
  const {
    docsByDocPath: normalPriorityDocsByDocPath,
    otherDocsByDocPath: normalPriorityOtherDocsByDocPath,
  } = groupDocsByTargetDocPath(normalPriorityDstPathLogicDocsMap, docPath);

  const distributedNormalPriorityDocs = await distributeFnNonTransactional(normalPriorityDocsByDocPath);
  for (const doc of distributedNormalPriorityDocs) {
    if (findMatchingViewLogics(doc, targetVersion)?.size) {
      docsForViewLogics.push(doc);
    }
    const {entity} = findMatchingDocPathRegex(doc.dstPath);
    if (entity && findMatchingPatchLogicsByEntity(entity, targetVersion).length > 0) {
      pathsForPatchLogics.add(doc.dstPath);
    }
  }

  await distributeLater(normalPriorityOtherDocsByDocPath, appVersion, targetVersion);

  // Clear normalPriorityDocs to free up memory
  if (process.env.NODE_ENV !== "test") {
    normalPriorityDocs.length = 0;
    normalPriorityDstPathLogicDocsMap.clear();
  }

  console.info(`Consolidating and Distributing Low Priority Logic Results: ${lowPriorityDocs.length}`);
  const lowPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
      await expandConsolidateAndGroupByDstPath(lowPriorityDocs);
  const {
    docsByDocPath: lowPriorityDocsByDocPath,
    otherDocsByDocPath: lowPriorityOtherDocsByDocPath,
  } = groupDocsByTargetDocPath(lowPriorityDstPathLogicDocsMap, docPath);

  await distributeLater(lowPriorityDocsByDocPath, appVersion, targetVersion);
  await distributeLater(lowPriorityOtherDocsByDocPath, appVersion, targetVersion);

  // Clear lowPriorityDocs to free up memory
  if (process.env.NODE_ENV !== "test") {
    lowPriorityDocs.length = 0;
    lowPriorityDstPathLogicDocsMap.clear();
  }

  return {docsForViewLogics, pathsForPatchLogics};
}

export const onUserRegister = async (user: UserRecord) => {
  const {
    uid,
    displayName,
    photoURL,
    email,
    providerData,
  } = user;

  const {
    displayName: providerDisplayName = "",
    photoURL: providerPhotoURL = "",
    email: providerEmail= "",
  } = providerData[0] || {};

  function splitDisplayName(displayName: string) {
    const parts = displayName.trim().split(/\s+/); // split by any whitespace

    if (parts.length === 0) {
      return {firstName: "", lastName: ""};
    }

    if (parts.length === 1) {
      return {firstName: parts[0], lastName: ""};
    }

    // For 2+ words, assume the last word is the last name
    const lastName = parts.pop();
    const firstName = parts.join(" "); // join remaining words as first name

    return {firstName, lastName};
  }

  const start = performance.now();
  console.debug("Starting Transaction");
  const customUserRegisterMetricExecution = await db.runTransaction(async (txn) => {
    console.debug("Creating user document");
    txn.set(db.doc(`users/${user.uid}`), {
      "@id": uid,
      ...splitDisplayName(displayName || providerDisplayName),
      "avatarUrl": photoURL || providerPhotoURL,
      "username": email || providerEmail,
      "email": email || providerEmail,
      "registeredAt": _mockable.createNowTimestamp(),
    });


    const customUserRegisterFn = userRegisterFn;
    if (!customUserRegisterFn) return;

    console.debug("Running custom user register function");
    const logicStart = performance.now();
    const txnGet = extractTransactionGetOnly(txn);
    const customUserRegisterFnLogicResult = await customUserRegisterFn(txnGet, user);
    const logicEnd = performance.now();
    console.debug("Distributing logic results");
    await distributeFnTransactional(txn, [customUserRegisterFnLogicResult]);
    console.debug("Finished custom user register function");

    return {
      name: customUserRegisterFnLogicResult.name,
      execTime: logicEnd - logicStart,
    } as MetricExecution;
  });

  const end = performance.now();
  const execTime = end - start;
  const onUserRegisterMetricExecution: MetricExecution = {
    name: "onUserRegister",
    execTime: execTime,
  };
  const metricExecutions = [onUserRegisterMetricExecution];
  if (customUserRegisterMetricExecution) {
    metricExecutions.push(customUserRegisterMetricExecution);
  }
  await indexUtilsMockable.saveMetricExecution(metricExecutions);
};
