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
  delayFormSubmissionAndCheckIfCancelled,
  distribute,
  distributeLater,
  expandConsolidateAndGroupByDstPath,
  getFormModifiedFields,
  getSecurityFn,
  groupDocsByUserAndDstPath,
  onDeleteFunction,
  processScheduledEntities,
  runBusinessLogics,
  validateForm,
} from "./index-utils";
import {initDbStructure} from "./init-db-structure";
import {createViewLogicFn} from "./logics/view-logics";
import {resetUsageStats, stopBillingIfBudgetExceeded, useBillProtect} from "./utils/bill-protect";
import {Firestore} from "firebase-admin/firestore";
import {DatabaseEvent, DataSnapshot, onValueCreated} from "firebase-functions/v2/database";
import {parseEntity} from "./utils/paths";
import {database} from "firebase-admin";
import {initClient} from "emberflow-admin-client/lib";
import {internalDbStructure, InternalEntity} from "./db-structure";
import {forDistributionLogicConfig} from "./logics/logics";
import {onMessageSubmitFormQueue} from "./utils/forms";
import {PubSub} from "@google-cloud/pubsub";
import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {deleteForms} from "./utils/misc";
import Database = database.Database;


export let admin: FirebaseAdmin;
export let db: Firestore;
export let rtdb: Database;
export let pubsub: PubSub;
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
  pubsub = new PubSub();
  dbStructure = {...customDbStructure, ...internalDbStructure};
  Entity = {...CustomEntity, ...InternalEntity};
  securityConfig = customSecurityConfig;
  validatorConfig = customValidatorConfig;
  logicConfigs = [...customLogicConfigs, forDistributionLogicConfig];
  initClient(admin.app());

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
      memory: "256MiB",
    },
    useBillProtect(onFormSubmit)
  );
  // TODO: Make this disappear when deployed to production
  functionsConfig["deleteForms"] = functions.https.onRequest(deleteForms);

  functionsConfig["onBudgetAlert"] =
        functions.pubsub.topic(projectConfig.budgetAlertTopicName).onPublish(stopBillingIfBudgetExceeded);
  functionsConfig["onMessageSubmitFormQueue"] = onMessagePublished({
    topic: projectConfig.submitFormQueueTopicName,
    region: projectConfig.region,
    memory: "256MiB",
    maxInstances: 1,
  }, onMessageSubmitFormQueue);
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
    const form = JSON.parse(formSnapshot.val().formData);
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

    const isUsersDocPath = docPath.startsWith("users");

    console.info("Validating userId");
    if (isUsersDocPath && !docPath.startsWith(`users/${userId}`)) {
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

    console.info("Validating form");
    const [hasValidationError, validationResult] = await validateForm(entity, form);
    if (hasValidationError) {
      await formRef.update({"@status": "validation-error", "@message": validationResult});
      return;
    }

    const document = (await db.doc(docPath).get()).data() || {};
    const formModifiedFields = getFormModifiedFields(form, document);
    let user;
    if (isUsersDocPath) {
      user = (await db.doc(`users/${userId}`).get()).data();
      if (!user) {
        const message = "No user data found";
        console.warn(message);
        await formRef.update({"@status": "error", "@message": message});
        return;
      }
    } else {
      user = {"@id": userId};
    }

    console.info("Validating Security");
    const securityFn = getSecurityFn(entity);
    if (securityFn) {
      const securityResult = await securityFn(entity, docPath, document,
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
      status,
      timeCreated,
      modifiedFields: formModifiedFields,
      user,
    };
    const actionRef = await _mockable.initActionRef(formId);
    await actionRef.set(action);

    await formRef.update({"@status": "submitted"});

    console.info("Running Business Logics");
    let errorMessage = "";
    await runBusinessLogics(
      actionType,
      formModifiedFields,
      entity,
      action,
      async (logicResults, page) => {
        // Save all logic results under logicResults collection of action form
        for (let i = 0; i < logicResults.length; i++) {
          const {documents, ...logicResult} = logicResults[i];
          const logicResultsRef = actionRef.collection("logicResults")
            .doc(`${actionRef.id}-${page}-${i}`);
          await logicResultsRef.set(logicResult);
          const documentsRef = logicResultsRef.collection("documents");
          for (let j = 0; j < documents.length; j++) {
            await documentsRef.doc(`${logicResultsRef.id}-${j}`).set(documents[j]);
          }
        }

        const errorLogicResults = logicResults.filter((result) => result.status === "error");
        if (errorLogicResults.length > 0) {
          errorMessage = errorMessage + errorLogicResults.map((result) => result.message).join("\n");
        }

        console.info("Group logic docs by priority");
        const {highPriorityDocs, normalPriorityDocs, lowPriorityDocs} = logicResults
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

        console.info("Consolidating and Distributing High Priority Logic Results", highPriorityDocs);
        const highPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
            await expandConsolidateAndGroupByDstPath(highPriorityDocs);
        const {
          userDocsByDstPath: highPriorityUserDocsByDstPath,
          otherUsersDocsByDstPath: highPriorityOtherUsersDocsByDstPath,
        } = groupDocsByUserAndDstPath(highPriorityDstPathLogicDocsMap, userId);
        await distribute(highPriorityUserDocsByDstPath);
        await distribute(highPriorityOtherUsersDocsByDstPath);

        if (page === 0) {
          await formRef.update({"@status": "finished"});
        }

        console.info("Consolidating and Distributing Normal Priority Logic Results", normalPriorityDocs);
        const normalPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
            await expandConsolidateAndGroupByDstPath(normalPriorityDocs);
        const {
          userDocsByDstPath: normalPriorityUserDocsByDstPath,
          otherUsersDocsByDstPath: normalPriorityOtherUsersDocsByDstPath,
        } = groupDocsByUserAndDstPath(normalPriorityDstPathLogicDocsMap, userId);
        await distribute(normalPriorityUserDocsByDstPath);
        await distributeLater(normalPriorityOtherUsersDocsByDstPath, `${formId}-normal-${page}`);

        console.info("Consolidating and Distributing Low Priority Logic Results", lowPriorityDocs);
        const lowPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
            await expandConsolidateAndGroupByDstPath(lowPriorityDocs);
        const {
          userDocsByDstPath: lowPriorityUserDocsByDstPath,
          otherUsersDocsByDstPath: lowPriorityOtherUsersDocsByDstPath,
        } = groupDocsByUserAndDstPath(lowPriorityDstPathLogicDocsMap, userId);
        await distributeLater(lowPriorityUserDocsByDstPath, `${formId}-low-user-${page}`);
        await distributeLater(lowPriorityOtherUsersDocsByDstPath, `${formId}-low-others-${page}`);

        // const userDocsByDstPath = new Map([
        //   ...highPriorityUserDocsByDstPath,
        //   ...normalPriorityUserDocsByDstPath,
        // ]);

        // console.info("Running View Logics");
        // const viewLogicResults = await runViewLogics(userDocsByDstPath);
        // const viewLogicResultDocs = viewLogicResults.map((result) => result.documents).flat();
        // const dstPathViewLogicDocsMap: Map<string, LogicResultDoc[]> = await expandConsolidateAndGroupByDstPath(viewLogicResultDocs);
        // console.info("Distributing View Logic Results");
        // await distribute(dstPathViewLogicDocsMap);

        // console.info("Running Peer Sync Views");
        // const peerSyncViewLogicResults = await runPeerSyncViews(userDocsByDstPath);
        // const peerSyncViewLogicResultDocs = peerSyncViewLogicResults.map((result) => result.documents).flat();
        // const dstPathPeerSyncViewLogicDocsMap: Map<string, LogicResultDoc> = await expandConsolidateAndGroupByDstPath(peerSyncViewLogicResultDocs);
        // const {otherUsersDocsByDstPath: otherUsersPeerSyncViewDocsByDstPath} = groupDocsByUserAndDstPath(dstPathPeerSyncViewLogicDocsMap, userId);
        //
        // console.info("Distributing Logic Results for Peer Sync Views");
        // await distributeLater(otherUsersPeerSyncViewDocsByDstPath, `${formId}-peers`);
      }
    );

    if (errorMessage) {
      await actionRef.update({status: "finished-with-error", message: errorMessage});
    } else {
      await actionRef.update({status: "finished"});
    }
    console.info("Finished");
  } catch (error) {
    console.error("Error in onFormSubmit", error);
    await formRef.update({"@status": "error", "@message": error});
  }
}
