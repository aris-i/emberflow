import * as functions from "firebase-functions";
import {
  Action,
  EventContext,
  FirebaseAdmin,
  Instructions,
  LogicConfig,
  LogicConfigModifiedFieldsType, LogicResult,
  LogicResultDoc,
  LogicResultDocAction,
  ProjectConfig,
  RunBusinessLogicStatus,
  SecurityConfig,
  ValidatorConfig,
  ViewDefinition,
  ViewLogicConfig,
} from "./types";
import {
  _mockable as indexUtilsMockable,
  cleanMetricComputations,
  cleanMetricExecutions,
  createMetricComputation,
  createMetricLogicDoc,
  delayFormSubmissionAndCheckIfCancelled, distributeDoc,
  distributeFnNonTransactional,
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
import {createViewLogicFn, onMessageViewLogicsQueue, queueRunViewLogics} from "./logics/view-logics";
import {resetUsageStats, stopBillingIfBudgetExceeded, useBillProtect} from "./utils/bill-protect";
import {Firestore} from "firebase-admin/firestore";
import {DatabaseEvent, DataSnapshot, onValueCreated} from "firebase-functions/v2/database";
import {parseEntity} from "./utils/paths";
import {database, firestore} from "firebase-admin";
import {initClient} from "emberflow-admin-client/lib";
import {internalDbStructure, InternalEntity} from "./db-structure";
import {cleanActionsAndForms, onMessageSubmitFormQueue} from "./utils/forms";
import {PubSub, Topic} from "@google-cloud/pubsub";
import {onMessagePublished} from "firebase-functions/v2/pubsub";
import {reviveDateAndTimestamp, deleteForms, trimStrings} from "./utils/misc";
import Database = database.Database;
import {
  onMessageForDistributionQueue,
  onMessageInstructionsQueue,
  instructionsReducer, convertInstructionsToDbValues,
} from "./utils/distribution";
import {cleanPubSubProcessedIds} from "./utils/pubsub";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {onDocumentCreated} from "firebase-functions/v2/firestore";
import {onRequest} from "firebase-functions/v2/https";
import {UserRecord} from "firebase-admin/lib/auth";
import {debounce} from "./utils/functions";
import {DocumentData} from "firebase-admin/lib/firestore";
import FieldValue = firestore.FieldValue;
import Transaction = firestore.Transaction;
import {extractTransactionGetOnly} from "./utils/transaction";

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
export const SUBMIT_FORM_TOPIC_NAME = "submit-form-queue";
export const VIEW_LOGICS_TOPIC_NAME = "view-logics-queue";
export const FOR_DISTRIBUTION_TOPIC_NAME = "for-distribution-queue";
export const INSTRUCTIONS_TOPIC_NAME = "instructions-queue";
export const pubSubTopics = [
  SUBMIT_FORM_TOPIC_NAME,
  VIEW_LOGICS_TOPIC_NAME,
  FOR_DISTRIBUTION_TOPIC_NAME,
  INSTRUCTIONS_TOPIC_NAME,
];
export let SUBMIT_FORM_TOPIC: Topic;
export let VIEW_LOGICS_TOPIC: Topic;
export let FOR_DISTRIBUTION_TOPIC: Topic;
export let INSTRUCTIONS_TOPIC: Topic;

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
  logicConfigs = [...customLogicConfigs];
  initClient(admin.app(), "service");
  SUBMIT_FORM_TOPIC = pubsub.topic(SUBMIT_FORM_TOPIC_NAME);
  VIEW_LOGICS_TOPIC = pubsub.topic(VIEW_LOGICS_TOPIC_NAME);
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
    const [srcToDstViewLogicFn, dstToSrcViewLogicFn] = createViewLogicFn(viewDef);
    const srcToDstLogicConfig = {
      name: `${viewDef.destEntity} ViewLogic`,
      entity: viewDef.srcEntity,
      actionTypes: ["create", "merge", "delete"] as LogicResultDocAction[],
      modifiedFields: viewDef.srcProps,
      viewLogicFn: srcToDstViewLogicFn,
    };
    const dstToSrcLogicConfig = {
      name: `${viewDef.destEntity} Reverse ViewLogic`,
      entity: viewDef.destEntity,
      actionTypes: ["create", "delete"] as LogicResultDocAction[],
      modifiedFields: "all" as LogicConfigModifiedFieldsType,
      ...(viewDef.destProp ? {destProp: viewDef.destProp.name} : {}),
      viewLogicFn: dstToSrcViewLogicFn,
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
      memory: "256MiB",
    },
    useBillProtect(onFormSubmit)
  );
  // TODO: Make this disappear when deployed to production
  functionsConfig["deleteForms"] = onRequest({
    timeoutSeconds: 540,
    region: projectConfig.region,
  }, deleteForms);

  functionsConfig["onBudgetAlert"] =
        functions.pubsub.topic(projectConfig.budgetAlertTopicName).onPublish(stopBillingIfBudgetExceeded);
  functionsConfig["onMessageSubmitFormQueue"] = onMessagePublished({
    topic: SUBMIT_FORM_TOPIC_NAME,
    region: projectConfig.region,
    maxInstances: 5,
    timeoutSeconds: 540,
    retry: true,
  }, onMessageSubmitFormQueue);
  functionsConfig["onMessageViewLogicsQueue"] = onMessagePublished({
    topic: VIEW_LOGICS_TOPIC_NAME,
    region: projectConfig.region,
    maxInstances: 5,
    timeoutSeconds: 540,
  }, onMessageViewLogicsQueue);
  functionsConfig["onMessageForDistributionQueue"] = onMessagePublished({
    topic: FOR_DISTRIBUTION_TOPIC_NAME,
    region: projectConfig.region,
    maxInstances: 5,
    timeoutSeconds: 540,
  }, onMessageForDistributionQueue);
  functionsConfig["onMessageInstructionsQueue"] = onMessagePublished({
    topic: INSTRUCTIONS_TOPIC_NAME,
    region: projectConfig.region,
    maxInstances: 1,
    concurrency: 1,
    timeoutSeconds: 540,
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
  }, resetUsageStats);
  functionsConfig["cleanPubSubProcessedIds"] = onSchedule({
    schedule: "every 1 hours",
    region: projectConfig.region,
    timeoutSeconds: 540,
  }, cleanPubSubProcessedIds);
  functionsConfig["cleanMetricComputations"] = onSchedule({
    schedule: "every 24 hours",
    region: projectConfig.region,
    timeoutSeconds: 540,
  }, cleanMetricComputations);
  functionsConfig["cleanMetricExecutions"] = onSchedule({
    schedule: "every 24 hours",
    region: projectConfig.region,
    timeoutSeconds: 540,
  }, cleanMetricExecutions);
  functionsConfig["cleanActionsAndForms"] = onSchedule({
    schedule: "every 24 hours",
    region: projectConfig.region,
    timeoutSeconds: 540,
  }, cleanActionsAndForms);
  functionsConfig["createMetricComputation"] = onSchedule({
    schedule: "every 1 hours",
    region: projectConfig.region,
    timeoutSeconds: 540,
  }, createMetricComputation);
  functionsConfig["onDeleteFunctions"] = onDocumentCreated(
    "@server/delete/functions/{deleteFuncId}", onDeleteFunction);
  functionsConfig["onUserRegister"] =
    functions.auth.user().onCreate(onUserRegister);

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
  const formSnapshot = event.data;
  const formRef = formSnapshot.ref;
  const start = performance.now();
  const logicResults: LogicResult[] = [];

  try {
    const trimmedForm = trimStrings(JSON.parse(formSnapshot.val().formData));
    const form = reviveDateAndTimestamp(trimmedForm);
    console.log("form", form);

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

    // Create Action form
    console.info("Validating @actionType");
    const actionType = form["@actionType"];
    if (!actionType) {
      const message = "No @actionType found";
      console.warn(message);
      await formRef.update({"@status": "error", "@messages": message});
      return;
    }

    console.info("Validating form");
    const validateFormStart = performance.now();
    const [hasValidationError, validationResult] = await validateForm(entity, form);
    const validateFormEnd = performance.now();
    const validateFormLogicResult: LogicResult = {
      name: "validateForm",
      status: "finished",
      documents: [],
      execTime: validateFormEnd - validateFormStart,
    };
    logicResults.push(validateFormLogicResult);
    if (hasValidationError) {
      await formRef.update({"@status": "validation-error", "@messages": validationResult});
      return;
    }

    let runStatus:RunBusinessLogicStatus = {status: "running", logicResults: []};

    let errorMessage= "";
    const actionRef = _mockable.initActionRef(formId);
    await db.runTransaction(async (txn) => {
      const docRef = db.doc(docPath);
      const document = (await txn.get(docRef)).data() || {};
      const formModifiedFields = getFormModifiedFields(form, document);
      let user;
      if (!isServiceAccount) {
        const userRef = db.doc(`users/${userId}`);
        user = (await txn.get(userRef)).data();
        if (!user) {
          const message = "No user data found";
          console.warn(message);
          await formRef.update({"@status": "error", "@messages": message});
          return;
        }
      } else {
        user = {"@id": userId};
      }

      console.info("Validating Security");
      const securityFn = getSecurityFn(entity);
      if (securityFn) {
        const securityFnStart = performance.now();
        const securityResult = await securityFn(entity, docPath, document,
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
          return;
        }
      }

      // Check for delay
      console.info("Checking for delay");
      const delay = form["@delay"];
      if (delay) {
        const delayStart = performance.now();
        const cancelled = await delayFormSubmissionAndCheckIfCancelled(delay, formRef);
        const delayEnd = performance.now();
        const delayLogicResult: LogicResult = {
          name: "delayFormSubmission",
          status: "finished",
          documents: [],
          execTime: delayEnd - delayStart,
        };
        logicResults.push(delayLogicResult);
        if (cancelled) {
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

      await formRef.update({"@status": "submitted"});
      console.info("Running Business Logics");
      const businessLogicStart = performance.now();
      runStatus = await runBusinessLogics(extractTransactionGetOnly(txn), action);
      const businessLogicEnd = performance.now();
      const businessLogicLogicResult: LogicResult = {
        name: "runBusinessLogics",
        status: "finished",
        documents: [],
        execTime: businessLogicEnd - businessLogicStart,
      };
      logicResults.push(businessLogicLogicResult);

      await actionRef.set(action);
      async function saveLogicResults() {
        for (let i = 0; i < runStatus.logicResults.length; i++) {
          const {documents, ...logicResult} = runStatus.logicResults[i];
          const logicResultsRef = actionRef.collection("logicResults")
            .doc(`${actionRef.id}-${i}`);
          await logicResultsRef.set(logicResult);
          const documentsRef = logicResultsRef.collection("documents");
          for (let j = 0; j < documents.length; j++) {
            await documentsRef.doc(`${logicResultsRef.id}-${j}`).set(documents[j]);
          }
        }
      }
      await saveLogicResults();

      function updateErrorMessage() {
        const errorLogicResults = runStatus.logicResults.filter((result) => result.status === "error");
        if (errorLogicResults.length > 0) {
          errorMessage = errorMessage + errorLogicResults.map((result) => result.message).join("\n");
        }
      }
      updateErrorMessage();

      const distributeTransactionalLogicResultsStart = performance.now();
      await distributeFnTransactional(txn, runStatus.logicResults);
      const distributeTransactionalLogicResultsEnd = performance.now();
      const distributeTransactionalLogicResults: LogicResult = {
        name: "distributeTransactionalLogicResults",
        status: "finished",
        documents: [],
        execTime: distributeTransactionalLogicResultsEnd - distributeTransactionalLogicResultsStart,
      };
      logicResults.push(distributeTransactionalLogicResults);
    });

    const distributeNonTransactionalLogicResultsStart = performance.now();
    await distributeNonTransactionalLogicResults(runStatus.logicResults, docPath);
    const distributeNonTransactionalLogicResultsEnd = performance.now();
    const distributeNonTransactionalPerfLogicResults: LogicResult = {
      name: "distributeNonTransactionalLogicResults",
      status: "finished",
      documents: [],
      execTime: distributeNonTransactionalLogicResultsEnd - distributeNonTransactionalLogicResultsStart,
    };
    logicResults.push(distributeNonTransactionalPerfLogicResults);

    await formRef.update({"@status": "finished"});

    const end = performance.now();
    const execTime = end - start;

    if (errorMessage) {
      await actionRef.update({status: "finished-with-error", message: errorMessage, execTime: execTime});
    } else {
      await actionRef.update({status: "finished", execTime: execTime});
    }

    const onFormSubmitLogicResult: LogicResult = {
      name: "onFormSubmit",
      status: "finished",
      documents: [],
      execTime,
    };
    logicResults.push(onFormSubmitLogicResult);

    await indexUtilsMockable.createMetricExecution(logicResults);
    console.info("Finished");
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
  docPath: string
) {
  const nonTransactionalResults = logicResults.filter((result) => !result.transactional);
  console.info(`Group logic docs by priority: ${nonTransactionalResults.length}`);
  const {highPriorityDocs, normalPriorityDocs, lowPriorityDocs} = nonTransactionalResults
    .map((result) => result.documents)
    .flat()
    .filter((doc) => !doc.journalEntries || doc.journalEntries && doc.action === "delete")
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
  await distributeFnNonTransactional(highPriorityDocsByDocPath);
  await distributeFnNonTransactional(highPriorityOtherDocsByDocPath);

  console.info(`Consolidating and Distributing Normal Priority Logic Results: ${normalPriorityDocs.length}`);
  const normalPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
      await expandConsolidateAndGroupByDstPath(normalPriorityDocs);
  const {
    docsByDocPath: normalPriorityDocsByDocPath,
    otherDocsByDocPath: normalPriorityOtherDocsByDocPath,
  } = groupDocsByTargetDocPath(normalPriorityDstPathLogicDocsMap, docPath);
  await distributeFnNonTransactional(normalPriorityDocsByDocPath);
  await distributeLater(normalPriorityOtherDocsByDocPath);

  console.info(`Consolidating and Distributing Low Priority Logic Results: ${lowPriorityDocs.length}`);
  const lowPriorityDstPathLogicDocsMap: Map<string, LogicResultDoc[]> =
      await expandConsolidateAndGroupByDstPath(lowPriorityDocs);
  const {
    docsByDocPath: lowPriorityDocsByDocPath,
    otherDocsByDocPath: lowPriorityOtherDocsByDocPath,
  } = groupDocsByTargetDocPath(lowPriorityDstPathLogicDocsMap, docPath);
  await distributeLater(lowPriorityDocsByDocPath);
  await distributeLater(lowPriorityOtherDocsByDocPath);
}

async function distributeFnTransactional(
  txn: Transaction,
  logicResults: LogicResult[]) {
  async function writeJournalEntriesFirst() {
    // Gather all logicResultDoc with journalEntries
    const logicResultDocsWithJournalEntries = logicResults
      .map((result) => result.documents).flat()
      .filter((logicResultDoc) => logicResultDoc.journalEntries &&
            logicResultDoc.action !== "delete");
    if (logicResultDocsWithJournalEntries.length === 0) {
      console.info("No journal entries to write");
      return;
    }

    const dstPathLogicDocsWithJournalEntriesMap =
        await expandConsolidateAndGroupByDstPath(logicResultDocsWithJournalEntries);
    for (const [dstPath, logicDocs] of dstPathLogicDocsWithJournalEntriesMap) {
      const docId = dstPath.split("/").pop();
      if (!docId) {
        console.error("Dst path has no docId");
        continue;
      }

      for (const logicDoc of logicDocs) {
        const {
          doc,
          instructions,
          journalEntries,
          action,
          skipRunViewLogics,
        } = logicDoc;

        if (!journalEntries) {
          continue;
        }

        const accounts = new Set(
          journalEntries.map((entry) => entry.ledgerEntries).flat()
            .map((entry) => entry.account)
        );
        if (Object.keys(doc || {}).some((key) => accounts.has(key))) {
          console.error("Doc cannot have keys that are the same as account names");
          continue;
        }
        if (Object.keys(instructions || {}).some((key) => accounts.has(key))) {
          console.error("Instructions cannot have keys that are the same as account names");
          continue;
        }

        for (let i= 0; i < journalEntries.length; i++) {
          const {
            ledgerEntries,
            recordEntry,
            equation,
            date,
          } = journalEntries[i];

          const consolidatedPerAccount = ledgerEntries
            .reduce((acc, entry) => {
              const {account} = entry;
              if (acc[account]) {
                acc[account].debit += entry.debit;
                acc[account].credit += entry.credit;
              } else {
                acc[account] = {
                  debit: entry.debit,
                  credit: entry.credit,
                };
              }
              return acc;
            }, {} as {[key: string]: {debit: number, credit: number}});

          // loop through keys of consolidatedPerAccount
          const totalCreditDebit = Object.entries(consolidatedPerAccount)
            .reduce((acc, [account, {debit, credit}]) => {
              return {
                debit: acc.debit + debit,
                credit: acc.credit + credit,
              };
            }, {debit: 0, credit: 0});
          if (totalCreditDebit.debit !== totalCreditDebit.credit) {
            console.error("Debit and credit should be equal");
            continue;
          }

          const docRef = db.doc(dstPath);
          const currData = (await txn.get(docRef)).data();

          let instructionsDbValues;
          if (instructions) {
            instructionsDbValues = await convertInstructionsToDbValues(txn, instructions);
          }

          const finalDoc: DocumentData = {
            ...(doc ? doc : {}),
            ...(instructionsDbValues ? instructionsDbValues : {}),
          };
          if (currData) {
            txn.update(docRef, {
              ...finalDoc,
              "@forDeletionLater": true,
            });
          } else {
            txn.set(docRef, {
              ...finalDoc,
              "@forDeletionLater": true,
            });
          }

          const [leftSide, ..._] = equation.split("=");

          Object.entries(consolidatedPerAccount).forEach(([account, {debit, credit}]) => {
            const increment = leftSide.includes(account) ? debit - credit : credit - debit;
            if (increment === 0) {
              txn.update(
                docRef,
                {
                  "@forDeletionLater": FieldValue.delete(),
                });
              return;
            }
            const accountVal = (currData?.[account] || 0) + increment;
            if (accountVal < 0) {
              throw new Error("Account value cannot be negative");
            }

            finalDoc[account] = accountVal;
            txn.update(
              docRef,
              {
                [account]: accountVal,
                "@forDeletionLater": FieldValue.delete(),
              });
          });

          if (Object.keys(finalDoc).length > 0 && !skipRunViewLogics &&
              ["create", "merge"].includes(action)) {
            logicDoc.doc = finalDoc;
            await queueRunViewLogics(logicDoc);
          }

          if (recordEntry) {
            for (let j = 0; j < ledgerEntries.length; j++) {
              const {account, debit, credit, description} = ledgerEntries[j];
              const journalEntryId = docId + i;
              const ledgerEntryId = journalEntryId + j;
              const ledgerEntryDoc: DocumentData = {
                journalEntryId,
                account,
                credit,
                debit,
                equation,
                date,
                ...(description && {description}),
              };
              const entryRef = docRef.collection("@ledgers").doc(ledgerEntryId);
              txn.set(
                entryRef,
                ledgerEntryDoc,
              );
            }
          }
        }
      }
    }
  }
  await writeJournalEntriesFirst();

  async function distributeTransactionalLogicResults() {
    const transactionalResults = logicResults.filter((result) => result.transactional);
    if (transactionalResults.length === 0) {
      console.info("No transactional logic results to distribute");
      return;
    }
    // We always distribute transactional results first
    const transactionalDstPathLogicDocsMap =
        await expandConsolidateAndGroupByDstPath(transactionalResults.map(
          (result) => result.documents).flat().filter((doc) =>
          !doc.journalEntries || doc.journalEntries && doc.action === "delete"));
      // Write to firestore in one transaction
    for (const [_, logicDocs] of transactionalDstPathLogicDocsMap) {
      for (const logicDoc of logicDocs) {
        await distributeDoc(logicDoc, undefined, txn);
      }
    }
  }
  await distributeTransactionalLogicResults();
}

const onUserRegister = async (user: UserRecord) => {
  await db.doc(`users/${user.uid}`).set({
    "@id": user.uid,
    "firstName": user.displayName || "",
    "lastName": "",
    "avatarUrl": user.photoURL || "",
    "username": user.email || "",
    "email": user.email || "",
    "registeredAt": admin.firestore.Timestamp.now(),
    "tokens": [],
  });
};
