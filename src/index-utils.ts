import {
  Action,
  ActionType,
  Instructions,
  LogicActionType,
  LogicConfig,
  LogicResult,
  LogicResultDoc, MetricExecution,
  RunBusinessLogicStatus,
  SecurityFn,
  TxnGet,
  ValidateFormResult,
} from "./types";
import {database, firestore} from "firebase-admin";
import {
  admin,
  db,
  logicConfigs,
  patchLogicConfigs,
  projectConfig,
  securityConfigs,
  validatorConfigs,
  viewLogicConfigs,
} from "./index";
import {_mockable as _pathMockable, expandAndGroupDocPathsByEntity, getDestPropAndDestPropId} from "./utils/paths";
import {deepEqual, deleteCollection} from "./utils/misc";
import {CloudFunctionsServiceClient} from "@google-cloud/functions";
import {BatchUtil} from "./utils/batch";
import {queueSubmitForm} from "./utils/forms";
import {
  convertInstructionsToDbValues,
  mergeInstructions,
  queueForDistributionLater,
  queueInstructions,
} from "./utils/distribution";
import {FirestoreEvent} from "firebase-functions/lib/v2/providers/firestore";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import {versionCompare} from "./logics/patch-logics";
import {FormData} from "emberflow-admin-client/lib/types";
import QueryDocumentSnapshot = firestore.QueryDocumentSnapshot;
import DocumentReference = FirebaseFirestore.DocumentReference;
import DocumentData = FirebaseFirestore.DocumentData;
import Reference = database.Reference;
import FieldValue = firestore.FieldValue;
import Timestamp = firestore.Timestamp;
import Transaction = firestore.Transaction;

export const _mockable = {
  getViewLogicConfigs: () => viewLogicConfigs,
  getPatchLogicConfigs: () => patchLogicConfigs,
  createNowTimestamp: () => admin.firestore.Timestamp.now(),
  createMetricExecution,
};

export async function distributeDoc(
  logicResultDoc: LogicResultDoc,
  batch?: BatchUtil,
  txn?: Transaction) {
  async function _delete(dstDocRef: DocumentReference) {
    if (batch) {
      await batch.deleteDoc(dstDocRef);
    } else if (txn) {
      await txn.delete(dstDocRef);
    } else {
      await dstDocRef.delete();
    }
  }

  async function _merge(dstDocRef: DocumentReference, data: DocumentData) {
    if (batch) {
      await batch.set(dstDocRef, data, {merge: true});
    } else if (txn) {
      txn.set(dstDocRef, data, {merge: true});
    } else {
      await dstDocRef.set(data, {merge: true});
    }
  }

  async function processInstructions(instructions: Instructions, destProp?: string, destPropId?: string) {
    if (!txn) {
      return;
    }

    const {updateData, removeData} = await convertInstructionsToDbValues(
      txn,
      instructions,
      destProp,
      destPropId
    );
    if (Object.keys(updateData).length > 0) {
      await _merge(dstDocRef, updateData);
    }
    if (Object.keys(removeData).length > 0) {
      await _merge(dstDocRef, removeData);
    }
  }

  const {
    action,
    doc,
    instructions,
    dstPath,
  } = logicResultDoc;

  const {basePath, destProp, destPropId} = getDestPropAndDestPropId(dstPath);

  const dstDocRef = db.doc(basePath);
  console.debug(`Distributing doc with Action: ${action}`);
  if (action === "delete") {
    if (destProp) {
      if (destPropId) {
        await _merge(dstDocRef, {
          [destProp]: {
            [destPropId]: FieldValue.delete(),
          },
        });
      } else {
        await _merge(dstDocRef, {
          [destProp]: FieldValue.delete(),
        });
      }
    } else {
      await _delete(dstDocRef);
    }
    console.log(`Document deleted at ${dstPath}`);
  } else if (action === "merge" || action === "create") {
    if (instructions) {
      if (txn) {
        await processInstructions(instructions, destProp, destPropId);
      } else {
        await queueInstructions(dstPath, instructions);
      }
    }

    if (doc) {
      let updateData: { [key: string]: any } = {};
      if (destProp) {
        if (destPropId) {
          updateData[destProp] = {[destPropId]: doc};
        } else {
          updateData[destProp] = doc;
        }
      } else {
        updateData = {
          ...doc,
          "@id": dstDocRef.id,
          ...(action === "create" ? {"@dateCreated": Timestamp.now()} : {}),
        };
      }
      await _merge(dstDocRef, updateData);

      console.log(`Document merged to ${dstPath}`);
    }
  } else if (action === "submit-form") {
    if (txn) {
      console.error("Submit-form is not supported in transactional logic result");
    } else {
      console.debug("Queuing submit form...");
      await queueSubmitForm({
        ...doc,
        "@docPath": dstPath,
      } as FormData);
    }
  }
}

export async function distributeFnNonTransactional(docsByDstPath: Map<string, LogicResultDoc[]>) {
  const forRunViewLogicQueuing: LogicResultDoc[] = [];
  const batch = BatchUtil.getInstance();
  for (const dstPath of Array.from(docsByDstPath.keys()).sort()) {
    console.log(`Documents for path ${dstPath}:`);
    const resultDocs = docsByDstPath.get(dstPath);
    if (!resultDocs) continue;
    for (const resultDoc of resultDocs) {
      forRunViewLogicQueuing.push(resultDoc);
      await distributeDoc(resultDoc, batch);
    }
  }

  if (batch.writeCount > 0) {
    console.log(`Committing final batch of ${batch.writeCount} writes...`);
    await batch.commit();
  }

  return forRunViewLogicQueuing;
}

export async function distributeLater(docsByDstPath: Map<string, LogicResultDoc[]>, appVersion: string, targetVersion: string) {
  console.log("Submitting to form for later processing...");
  const documents = Array.from(docsByDstPath.values()).flat();
  await queueForDistributionLater(appVersion, targetVersion, ...documents);
}

export async function validateForm(
  entity: string,
  form: FirebaseFirestore.DocumentData,
  targetVersion: string
): Promise<ValidateFormResult> {
  let hasValidationError = false;
  console.info(`Validating form for entity ${entity}`);
  const validatorFn = validatorConfigs
    .filter((config) =>
      config.entity === entity &&
        versionCompare(config.version, targetVersion) <= 0
    ).sort((a, b) => versionCompare(b.version, a.version))[0]
    ?.validatorFn;
  if (!validatorFn) {
    console.log(`No validator found for entity ${entity}`);
    return [false, {}];
  }
  const validationResult = await validatorFn(form);

  // Check if validation failed
  if (validationResult && Object.keys(validationResult).length > 0) {
    console.log(`Document validation failed: ${JSON.stringify(validationResult)}`);
    hasValidationError = true;
  }
  return [hasValidationError, validationResult];
}

export function getFormModifiedFields(form: DocumentData, document: DocumentData): DocumentData {
  const modifiedFields: DocumentData = {};

  for (const key in form) {
    if (key.startsWith("@")) continue;
    if (!(key in document) || !deepEqual(document[key], form[key])) {
      modifiedFields[key] = form[key];
    }
  }

  return modifiedFields;
}

export async function delayFormSubmissionAndCheckIfCancelled(delay: number, formRef: Reference) {
  let cancelFormSubmission = false;
  console.log(`Delaying document for ${delay}ms...`);
  await formRef.update({"@status": "delay"});
  await new Promise((resolve) => setTimeout(resolve, delay));
  // Re-fetch document from Firestore
  const updatedSnapshot = await formRef.get();
  const updatedDocument = updatedSnapshot.val();
  console.log("Re-fetched document from Firestore after delay:\n", updatedDocument);
  // Check if form status is "cancel"
  if (updatedDocument["@status"] === "cancel") {
    cancelFormSubmission = true;
  }
  return cancelFormSubmission;
}

function getMatchingLogics(actionType: ActionType, modifiedFields: DocumentData,
  document: DocumentData, entity: string, metadata: Record<string, any>, targetVersion: string) {
  const matchingLogics = logicConfigs.filter((logic) => {
    return (
      (logic.actionTypes === "all" || logic.actionTypes.includes(actionType as LogicActionType)) &&
        (logic.modifiedFields === "all" || logic.modifiedFields.some((field) => field in modifiedFields)) &&
        (logic.entities === "all" || logic.entities.includes(entity)) &&
      (logic.addtlFilterFn ? logic.addtlFilterFn(actionType, modifiedFields, document, entity, metadata) : true) &&
      (logic.obsoleteAfterVersion ? versionCompare(targetVersion, logic.obsoleteAfterVersion) <= 0 : true) &&
          versionCompare(logic.version, targetVersion) <= 0
    );
  });

  // Let's group by name
  const nameIndex = new Map<string, number>();
  return matchingLogics.reduce((acc, logicConfig) => {
    const {name} = logicConfig;
    if (!nameIndex.has(name)) {
      const length = acc.push(logicConfig);
      nameIndex.set(name, length-1);
    } else {
      const index = nameIndex.get(name);
      const logicConfigPrev = acc[index!];
      if (versionCompare(logicConfigPrev.version, logicConfig.version) < 0) {
        acc[index!] = logicConfig; // Replace with the latest version
      }
    }
    return acc;
  }, [] as LogicConfig[]);
}

export const runBusinessLogics = async (
  txnGet: TxnGet,
  action: Action,
  targetVersion: string,
): Promise<RunBusinessLogicStatus> => {
  const {actionType, modifiedFields, document, eventContext: {entity}, metadata} = action;

  const matchingLogics = getMatchingLogics(
    actionType, modifiedFields, document, entity, metadata, targetVersion
  );
  console.debug("Matching logics:", matchingLogics.map((logic) => logic.name));
  if (matchingLogics.length === 0) {
    console.log("No matching logics found");
    return {status: "no-matching-logics", logicResults: []};
  }

  const sharedMap = new Map<string, any>();

  const logicResults: LogicResult[] = [];
  for (let i = 0; i < matchingLogics.length; i++) {
    const start = performance.now();
    const logic = matchingLogics[i];
    console.debug("Running logic:", logic.name);
    try {
      const result = await logic.logicFn(txnGet, action, sharedMap );
      const end = performance.now();
      const execTime = end - start;
      logicResults.push({...result, execTime, timeFinished: admin.firestore.Timestamp.now()});
    } catch (e) {
      const end = performance.now();
      const execTime = end - start;
      logicResults.push({
        name: logic.name,
        status: "error",
        documents: [],
        execTime,
        message: (e as Error).message,
        timeFinished: admin.firestore.Timestamp.now(),
      });
    }
  }

  return {status: "done", logicResults};
};

export function groupDocsByTargetDocPath(docsByDstPath: Map<string, LogicResultDoc[]>, docPath: string) {
  const docsByDocPath = new Map<string, LogicResultDoc[]>();
  const otherDocsByDocPath = new Map<string, LogicResultDoc[]>();

  for (const [key, values] of docsByDstPath.entries()) {
    if (key.startsWith(docPath)) {
      docsByDocPath.set(key, values);
    } else {
      otherDocsByDocPath.set(key, values);
    }
  }

  return {docsByDocPath, otherDocsByDocPath};
}

export function getSecurityFn(entity: string, targetVersion: string): SecurityFn {
  return securityConfigs
    .filter((security) =>
      security.entity === entity &&
        versionCompare(security.version, targetVersion) <= 0)
    .sort((a, b) => versionCompare(b.version, a.version))[0]
    ?.securityFn;
}

export const expandConsolidateAndGroupByDstPath = async (logicDocs: LogicResultDoc[]): Promise<Map<string, LogicResultDoc[]>> => {
  function warnOverwritingKeys(existing: any, incoming: any, type: string, dstPath: string) {
    for (const key in incoming) {
      if (existing && Object.prototype.hasOwnProperty.call(existing, key)) {
        console.warn(`Overwriting key "${key}" in ${type} for dstPath "${dstPath}"`);
      }
    }
  }

  function processMergeAndCreate(existingDocs: LogicResultDoc[], logicResultDoc: LogicResultDoc, dstPath: string) {
    let merged = false;
    for (const existingDoc of existingDocs) {
      if (existingDoc.action === "delete") {
        console.warn(`Action ${logicResultDoc.action} ignored because a "delete" for dstPath "${logicResultDoc.dstPath}" already exists`);
        merged = true;
        break;
      }
      if (existingDoc.action === "merge" || existingDoc.action === "create") {
        warnOverwritingKeys(existingDoc.doc, logicResultDoc.doc, "doc", dstPath);
        if (existingDoc.action === "merge" && logicResultDoc.action === "create") {
          console.info(`Existing doc Action "merge" for dstPath "${dstPath}" is being converted to "create"`);
          existingDoc.action = "create";
        }
        if (logicResultDoc.instructions) {
          if (!existingDoc.instructions) {
            existingDoc.instructions = {...logicResultDoc.instructions};
          } else {
            console.info(`merging multiple instructions for dstPath "${dstPath}"`);
            mergeInstructions(
              existingDoc.instructions as Instructions,
              logicResultDoc.instructions as Instructions
            );
          }
        }
        existingDoc.doc = {...existingDoc.doc, ...logicResultDoc.doc};

        merged = true;
        break;
      }
    }
    if (!merged) {
      existingDocs.push({
        ...logicResultDoc,
        ...(logicResultDoc.instructions ? {instructions: logicResultDoc.instructions} : {}),
      });
    }
  }

  function processDelete(existingDocs: LogicResultDoc[], logicResultDoc: LogicResultDoc, dstPath: string) {
    for (let i = existingDocs.length-1; i >= 0; i--) {
      const existingDoc = existingDocs[i];
      if (existingDoc.action === "merge" || existingDoc.action === "delete") {
        console.warn(`Action ${existingDoc.action} for dstPath "${dstPath}" is being overwritten by action "delete"`);
        existingDocs.splice(i, 1);
      }
    }
    existingDocs.push(logicResultDoc);
  }

  async function expandRecursiveActions() {
    const expandedLogicResultDocs: LogicResultDoc[] = [];
    for (let i = logicDocs.length - 1; i >= 0; i--) {
      const logicResultDoc = logicDocs[i];
      const {
        action,
        dstPath,
        srcPath,
        skipEntityDuringRecursion,
        priority,
      } = logicResultDoc;

      if (!["recursive-delete", "recursive-copy"].includes(action)) continue;

      const toExpandPath = action === "recursive-delete" ? dstPath : srcPath;
      if (!toExpandPath) {
        continue;
      }
      const subDocPaths = await expandAndGroupDocPathsByEntity(
        toExpandPath,
        undefined,
        skipEntityDuringRecursion
      );

      for (const [_, paths] of Object.entries(subDocPaths)) {
        for (const path of paths) {
          if (action === "recursive-delete") {
            expandedLogicResultDocs.push({
              action: "delete",
              dstPath: path,
              priority,
            });
          } else if (action === "recursive-copy" && srcPath) {
            const absoluteDstPath = path.replace(srcPath, dstPath);
            const data = (await db.doc(path).get()).data();
            expandedLogicResultDocs.push({
              action: "merge",
              doc: data,
              dstPath: absoluteDstPath,
              priority,
            });
          }
        }
      }

      logicDocs.splice(i, 1, ...expandedLogicResultDocs);
    }
  }
  await expandRecursiveActions();

  async function convertCopyToMerge() {
    for (const doc of logicDocs) {
      const {
        srcPath,
        action,
      } = doc;

      if (action !== "copy" || !srcPath) {
        continue;
      }

      const data = (await db.doc(srcPath).get()).data();
      delete doc.srcPath;
      doc.doc = data;
      doc.action = "merge";
    }
  }
  await convertCopyToMerge();

  const consolidated: Map<string, LogicResultDoc[]> = new Map();

  for (const doc of logicDocs) {
    const {
      dstPath,
      action,
    } = doc;
    const existingDocs = consolidated.get(dstPath) || [];
    if (existingDocs.length === 0) {
      consolidated.set(dstPath, existingDocs);
    }

    if (action === "merge" || action === "create") {
      processMergeAndCreate(existingDocs, doc, dstPath);
    } else if (action === "delete") {
      processDelete(existingDocs, doc, dstPath);
    } else if (action === "submit-form") {
      existingDocs.push(doc);
    }
  }

  return consolidated;
};

export async function onDeleteFunction(event: FirestoreEvent<QueryDocumentSnapshot | undefined, {deleteFuncId: string}>) {
  const data = event.data;
  if (!data) {
    console.error("Data should not be null");
    return;
  }
  const {name} = data.data();
  if (!name) {
    console.error("name should not be null");
    return;
  }
  return deleteFunction(projectConfig.projectId, name);
}

async function getFunctionLocation(projectId: string, functionName: string): Promise<string | undefined> {
  const client = new CloudFunctionsServiceClient();

  const [functionsResponse] = await client.listFunctions({
    parent: `projects/${projectId}/locations/-`,
  });

  const targetFunction = functionsResponse.find((fn) => fn.name === functionName);

  if (targetFunction && targetFunction.name) {
    const locationParts = targetFunction.name.split("/");
    return locationParts[3];
  }

  return undefined;
}

async function deleteFunction(projectId: string, functionName: string): Promise<void> {
  const location = await getFunctionLocation(projectId, functionName);

  if (location) {
    const client = new CloudFunctionsServiceClient();
    const name = `projects/${projectId}/locations/${location}/functions/${functionName}`;
    await client.deleteFunction({name});
    console.log(`Function '${functionName}' in location '${location}' deleted successfully.`);
  } else {
    console.log(`Function '${functionName}' not found or location not available.`);
  }
}

export async function createMetricLogicDoc(logicName: string) {
  const metricsRef = db.doc(`@metrics/${logicName}`);
  if (!await _pathMockable.doesPathExists(metricsRef.path)) {
    await metricsRef.set({
      totalExecTime: 0,
      totalExecCount: 0,
    });
  }
}

export function convertLogicResultsToMetricExecutions(
  logicResults: LogicResult[]
): MetricExecution[] {
  return logicResults.map(({name, execTime}) =>
    ({name, execTime} as MetricExecution)
  );
}

async function createMetricExecution(metricExecutions: MetricExecution[]) {
  const metricsRef = db.collection("@metrics");
  for (const metricExecution of metricExecutions) {
    const {name, execTime} = metricExecution;
    if (!execTime) {
      console.warn(`No execTime found for logic ${name}`);
      continue;
    }
    if (execTime > 100) {
      console.warn(`${name} took ${execTime}ms to execute`);
    }

    const logicRef = metricsRef.doc(name);
    await queueInstructions(logicRef.path, {
      totalExecTime: `+${execTime}`,
      totalExecCount: "++",
    });

    const execRef = logicRef.collection("executions").doc();
    await execRef.set({
      execDate: admin.firestore.Timestamp.now(),
      execTime,
    });
  }
}

export async function cleanMetricExecutions(event: ScheduledEvent) {
  console.info("Running cleanMetricExecutions");
  const metricsSnapshot = await db.collection("@metrics").get();
  let i = 0;
  for (const metricDoc of metricsSnapshot.docs) {
    const query = metricDoc.ref.collection("executions")
      .where("execDate", "<", new Date(Date.now() - 1000 * 60 * 60 * 24 * 7));

    await deleteCollection(query, (snapshot) => {
      i += snapshot.size;
    });
  }
  console.info(`Cleaned ${i} logic metric executions`);
}

export async function createMetricComputation(event: ScheduledEvent) {
  console.info("Creating metric computation");
  const metricsSnapshot = await db.collection("@metrics").get();
  for (const metricDoc of metricsSnapshot.docs) {
    const query = metricDoc.ref.collection("executions")
      .where("execDate", ">=", new Date(Date.now() - 1000 * 60 * 60));

    const snapshots = await query.get();
    if (snapshots.empty) {
      console.info(`No executions found for ${metricDoc.id}`);
      continue;
    }

    const execTimes = snapshots.docs.map((doc) => doc.data().execTime);
    const maxExecTime = Math.max(...execTimes);
    const minExecTime = Math.min(...execTimes);
    const totalExecTime = execTimes.reduce((a, b) => a + b, 0);
    const execCount = execTimes.length;
    const avgExecTime = totalExecTime / execCount;
    const jitterTime = maxExecTime - minExecTime;

    const execRef = metricDoc.ref.collection("computations").doc();
    await execRef.set({
      createdAt: admin.firestore.Timestamp.now(),
      maxExecTime,
      minExecTime,
      totalExecTime,
      execCount,
      avgExecTime,
      jitterTime,
    });
  }
}

export async function cleanMetricComputations(event: ScheduledEvent) {
  console.info("Running cleanMetricComputations");
  const metricsSnapshot = await db.collection("@metrics").get();
  let i = 0;
  for (const metricDoc of metricsSnapshot.docs) {
    const query = metricDoc.ref.collection("computations")
      .where("createdAt", "<", new Date(Date.now() - 1000 * 60 * 60 * 24 * 30));

    await deleteCollection(query, (snapshot) => {
      i += snapshot.size;
    });
  }
  console.info(`Cleaned ${i} logic metric computations`);
}

export async function distributeFnTransactional(
  txn: Transaction,
  logicResults: LogicResult[],
): Promise<LogicResultDoc[]> {
  const distributedLogicResultDocs: LogicResultDoc[] = [];

  const transactionalResults = logicResults.filter((result) => result.transactional);
  if (transactionalResults.length === 0) {
    console.info("No transactional logic results to distribute");
    return distributedLogicResultDocs;
  }
  // We always distribute transactional results first
  const transactionalDstPathLogicDocsMap = await expandConsolidateAndGroupByDstPath(
    transactionalResults.flatMap((result) => result.documents)
  );
    // Write to firestore in one transaction
  for (const [_, logicDocs] of transactionalDstPathLogicDocsMap) {
    for (const logicDoc of logicDocs) {
      distributedLogicResultDocs.push(logicDoc);
      await distributeDoc(logicDoc, undefined, txn);
    }
  }


  return distributedLogicResultDocs;
}


