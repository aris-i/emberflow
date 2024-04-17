import {
  Action, DistributeFn, LogicActionType, LogicConfig,
  LogicResult,
  LogicResultDoc,
  SecurityFn,
  ValidateFormResult,
} from "./types";
import {database, firestore} from "firebase-admin";
import {
  admin,
  db,
  logicConfigs,
  projectConfig,
  securityConfig,
  validatorConfig,
  viewLogicConfigs,
} from "./index";
import {expandAndGroupDocPathsByEntity, findMatchingDocPathRegex} from "./utils/paths";
import {deepEqual, deleteCollection} from "./utils/misc";
import {CloudFunctionsServiceClient} from "@google-cloud/functions";
import {FormData} from "emberflow-admin-client/lib/types";
import {BatchUtil} from "./utils/batch";
import {queueSubmitForm} from "./utils/forms";
import {queueForDistributionLater, queueInstructions} from "./utils/distribution";
import QueryDocumentSnapshot = firestore.QueryDocumentSnapshot;
import DocumentReference = FirebaseFirestore.DocumentReference;
import DocumentData = FirebaseFirestore.DocumentData;
import Reference = database.Reference;
import {FirestoreEvent} from "firebase-functions/lib/v2/providers/firestore";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import {queueRunViewLogics} from "./logics/view-logics";

export const _mockable = {
  getViewLogicsConfig: () => viewLogicConfigs,
  createNowTimestamp: () => admin.firestore.Timestamp.now(),
  simulateSubmitForm,
  createMetricExecution,
};

export async function distributeDoc(logicResultDoc: LogicResultDoc, batch?: BatchUtil) {
  const {
    action,
    doc,
    instructions,
    dstPath,
  } = logicResultDoc;
  const dstDocRef = db.doc(dstPath);
  console.debug(`Distributing doc with Action: ${action}`);
  if (action === "delete") {
    // Delete document at dstPath
    if (batch) {
      await batch.deleteDoc(dstDocRef);
    } else {
      await dstDocRef.delete();
    }
    console.log(`Document deleted at ${dstPath}`);
  } else if (action === "merge" || action === "create") {
    if (instructions) {
      await queueInstructions(dstPath, instructions);
    }

    const updateData: { [key: string]: any } = {...doc, "@id": dstDocRef.id};
    if (batch) {
      if (action === "merge") {
        await batch.update(dstDocRef, updateData);
      } else {
        await batch.set(dstDocRef, updateData);
      }
    } else {
      if (action === "merge") {
        await dstDocRef.update(updateData);
      } else {
        await dstDocRef.set(updateData);
      }
    }
    console.log(`Document merged to ${dstPath}`);
  } else if (action === "submit-form") {
    console.debug("Queuing submit form...");
    const formData: FormData = {
      "@docPath": dstPath,
      "@actionType": "create",
      ...doc,
    };
    await queueSubmitForm(formData);
  } else if (action === "simulate-submit-form") {
    console.debug("Not distributing doc for action simulate-submit-form");
  }

  if (["merge", "delete"].includes(action)) {
    await queueRunViewLogics(logicResultDoc);
  }
}

export async function distribute(
  docsByDstPath: Map<string, LogicResultDoc[]> ) {
  const batch = BatchUtil.getInstance();
  for (const dstPath of Array.from(docsByDstPath.keys()).sort()) {
    console.log(`Documents for path ${dstPath}:`);
    const resultDocs = docsByDstPath.get(dstPath);
    if (!resultDocs) continue;
    for (const resultDoc of resultDocs) {
      await distributeDoc(resultDoc, batch);
    }
  }

  if (batch.writeCount > 0) {
    console.log(`Committing final batch of ${batch.writeCount} writes...`);
    await batch.commit();
  }
}

export async function distributeLater(docsByDstPath: Map<string, LogicResultDoc[]>) {
  console.log("Submitting to form for later processing...");
  const documents = Array.from(docsByDstPath.values()).flat();
  await queueForDistributionLater(...documents);
}

export async function validateForm(
  entity: string,
  form: FirebaseFirestore.DocumentData
): Promise<ValidateFormResult> {
  let hasValidationError = false;
  console.info(`Validating form for entity ${entity}`);
  const validate = validatorConfig[entity];
  if (!validate) {
    console.log(`No validator found for entity ${entity}`);
    return [false, {}];
  }
  const validationResult = await validate(form);

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

async function simulateSubmitForm(logicResults: LogicResult[], action: Action,
  distributeFn: DistributeFn) {
  const forSimulateSubmitForm = logicResults
    .map((result: LogicResult) => result.documents)
    .flat()
    .filter((doc: LogicResultDoc) => doc.action === "simulate-submit-form")
    .map((doc: LogicResultDoc) => ({logicResultDoc: doc, retryCount: 0}));
  console.debug("Simulating submit form: ", forSimulateSubmitForm.length);

  const retryQueue = [];
  const backoffTime = 1000; // Starting with 1 second
  const maxRetryCount = 5;
  let runCount = 1;
  while (forSimulateSubmitForm.length > 0 || retryQueue.length > 0) {
    if (forSimulateSubmitForm.length > 0) {
      const forRunning = forSimulateSubmitForm.shift();
      if (!forRunning) continue;
      const {logicResultDoc, retryCount} = forRunning;
      const {entity} = findMatchingDocPathRegex(logicResultDoc.dstPath);
      if (!entity) {
        console.warn(`No matching entity found for logic ${logicResultDoc.dstPath}. Skipping`);
        continue;
      }
      const docId = logicResultDoc.dstPath.split("/").pop();
      if (!docId) {
        console.warn("docId should not be blank. Skipping");
        continue;
      }
      if (!logicResultDoc.doc) {
        console.warn("LogicResultDoc.doc should not be undefined. Skipping");
        continue;
      }
      if (!logicResultDoc.doc["@actionType"]) {
        console.warn("No @actionType found. Skipping");
        continue;
      }
      const eventContext = {
        id: action.eventContext.id + `-${runCount}`,
        uid: action.eventContext.uid,
        formId: action.eventContext.formId + `-${runCount}`,
        docId,
        docPath: logicResultDoc.dstPath,
        entity,
      };
      let user;
      const submitFormAs = logicResultDoc.doc["@submitFormAs"];
      if (submitFormAs) {
        user = (await db.collection("users").doc(submitFormAs).get()).data();
        if (!user) {
          console.warn(`User ${submitFormAs} not found. Skipping`);
          continue;
        }
      }
      const modifiedFields: DocumentData = {};
      for (const key in logicResultDoc.doc) {
        if (!key.startsWith("@")) {
          modifiedFields[key] = logicResultDoc.doc[key];
        }
      }
      const _action: Action = {
        eventContext,
        actionType: logicResultDoc.doc["@actionType"],
        document: (await db.doc(logicResultDoc.dstPath).get()).data() || {},
        modifiedFields: modifiedFields,
        user: user || action.user,
        status: "new",
        timeCreated: _mockable.createNowTimestamp(),
      };
      const _actionRef = db.collection("@actions").doc(eventContext.formId);
      await _actionRef.set(_action);

      const status = await runBusinessLogics(_actionRef, _action, distributeFn);
      if (status === "cancel-then-retry") {
        if (retryCount + 1 > maxRetryCount) {
          console.warn(`Maximum retry count reached for logic ${logicResultDoc.dstPath}`);
          continue;
        } else {
          retryQueue.unshift({logicResultDoc, retryCount: retryCount + 1, timeAdded: Date.now()});
        }
      }
    }

    const currentTime = Date.now();
    for (let i = retryQueue.length - 1; i >= 0; i--) {
      if (currentTime >= retryQueue[i].timeAdded + Math.pow(2, retryQueue[i].retryCount) * backoffTime) {
        const {logicResultDoc, retryCount} = retryQueue.splice(i, 1)[0];
        forSimulateSubmitForm.push({logicResultDoc, retryCount});
      }
    }

    // Avoid tight looping
    await new Promise((resolve) => setTimeout(resolve, 100));
    runCount++;
  }
}

function getMatchingLogics(actionType: "create" | "update" | "delete", modifiedFields: FirebaseFirestore.DocumentData,
  entity: string) {
  return logicConfigs.filter((logic) => {
    return (
      (logic.actionTypes === "all" || logic.actionTypes.includes(actionType as LogicActionType)) &&
        (logic.modifiedFields === "all" ||
            logic.modifiedFields.some((field) => field in modifiedFields)) &&
        (logic.entities === "all" || logic.entities.includes(entity))
    );
  });
}

async function runLogic(action: Action, matchingLogics: LogicConfig[], nextPageMarkers: (object | undefined)[],
  sharedMap: Map<string, any>, logicResults: LogicResult[]) {
  for (let i = matchingLogics.length - 1; i >= 0; i--) {
    const start = performance.now();
    const logic = matchingLogics[i];
    console.debug("Running logic:", logic.name, "nextPageMarker:", nextPageMarkers[i]);
    try {
      const result = await logic.logicFn(action, sharedMap, nextPageMarkers[i]);
      const end = performance.now();
      const execTime = end - start;
      const {status, nextPage} = result;
      if (status === "finished" || status === "error") {
        matchingLogics.splice(i, 1);
        nextPageMarkers.splice(i, 1);
      } else if (status === "partial-result") {
        nextPageMarkers[i] = nextPage;
      }

      logicResults.push({...result, execTime, timeFinished: admin.firestore.Timestamp.now()});
      if (status === "cancel-then-retry") {
        return "cancel-then-retry";
      }
    } catch (e) {
      const end = performance.now();
      const execTime = end - start;
      matchingLogics.splice(i, 1);
      nextPageMarkers.splice(i, 1);
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
  return "done";
}

async function distributeLogicResults(actionRef: FirebaseFirestore.DocumentReference<FirebaseFirestore.DocumentData>, distributeFn: (actionRef: FirebaseFirestore.DocumentReference, logicResults: LogicResult[], page: number) => Promise<void>, logicResults: LogicResult[], page: number) {
  const start = performance.now();
  await distributeFn(actionRef, logicResults, page);
  const end = performance.now();
  const execTime = end - start;
  const distributeFnLogicResult: LogicResult = {
    name: "distributeFn",
    status: "finished",
    documents: [],
    execTime: execTime,
  };
  await _mockable.createMetricExecution([...logicResults, distributeFnLogicResult]);
}

export const runBusinessLogics = async (
  actionRef: DocumentReference,
  action: Action,
  distributeFn: DistributeFn): Promise<"done" | "cancel-then-retry" | "no-matching-logics"> => {
  const {actionType, modifiedFields, eventContext: {entity}} = action;

  const matchingLogics = getMatchingLogics(actionType, modifiedFields, entity);
  console.debug("Matching logics:", matchingLogics.map((logic) => logic.name));
  if (matchingLogics.length === 0) {
    console.log("No matching logics found");
    await distributeFn(actionRef, [], 0);
    return "no-matching-logics";
  }

  const config = (await db.doc("@server/config").get()).data();
  const maxLogicResultPages = config?.maxLogicResultPages || 20;

  let page = 0;
  const nextPageMarkers: (object|undefined)[] = Array(matchingLogics.length).fill(undefined);
  const sharedMap = new Map<string, any>();
  matchingLogics.reverse();
  while (matchingLogics.length > 0) {
    if (page > 0) {
      console.debug(`Page ${page} Remaining logics:`, matchingLogics.map((logic) => logic.name));
    }
    const logicResults: LogicResult[] = [];
    const status = await runLogic(action, matchingLogics, nextPageMarkers, sharedMap, logicResults);
    if (status === "cancel-then-retry") {
      return "cancel-then-retry";
    }

    await distributeLogicResults(actionRef, distributeFn, logicResults, page++);
    if (page >= maxLogicResultPages) {
      console.warn(`Maximum number of logic result pages (${maxLogicResultPages}) reached`);
      break;
    }

    await _mockable.simulateSubmitForm(logicResults, action, distributeFn);
  }

  return "done";
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

export function getSecurityFn(entity: string): SecurityFn {
  return securityConfig[entity];
}

export async function expandConsolidateAndGroupByDstPath(logicDocs: LogicResultDoc[]): Promise<Map<string, LogicResultDoc[]>> {
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
        warnOverwritingKeys(existingDoc.instructions, logicResultDoc.instructions, "instructions", dstPath);
        if (existingDoc.action === "merge" && logicResultDoc.action === "create") {
          console.info(`Existing doc Action "merge" for dstPath "${dstPath}" is being converted to "create"`);
          existingDoc.action = "create";
        }
        existingDoc.instructions = {...existingDoc.instructions, ...logicResultDoc.instructions};
        existingDoc.doc = {...existingDoc.doc, ...logicResultDoc.doc};
        merged = true;
        break;
      }
    }
    if (!merged) {
      existingDocs.push(logicResultDoc);
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

  await expandRecursiveActions();

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
    } else if (action === "simulate-submit-form") {
      existingDocs.push(doc);
    }
  }

  return consolidated;
}

export async function runViewLogics(logicResultDoc: LogicResultDoc): Promise<LogicResult[]> {
  const {
    action,
    doc,
    instructions,
    dstPath,
  } = logicResultDoc;
  const modifiedFields: string[] = [];
  if (doc) {
    modifiedFields.push(...Object.keys(doc));
  }
  if (instructions) {
    modifiedFields.push(...Object.keys(instructions));
  }
  const {entity} = findMatchingDocPathRegex(dstPath);
  if (!entity) {
    console.error("Entity should not be blank");
    return [];
  }
  const matchingLogics = _mockable.getViewLogicsConfig().filter((logic) => {
    return (
      (
        logic.actionTypes.includes(action) &&
        (
          logic.modifiedFields === "all" ||
            logic.modifiedFields.some((field) => modifiedFields.includes(field))
        ) &&
                logic.entity === entity
      ) ||
            (
              action === "delete" &&
                logic.entity === entity
            )
    );
  });
  // TODO: Handle errors

  const logicResults = [];
  for (const logic of matchingLogics) {
    const start = performance.now();
    const viewLogicResult = await logic.viewLogicFn(logicResultDoc);
    const end = performance.now();
    const execTime = end - start;
    logicResults.push({...viewLogicResult, execTime});
  }
  return logicResults;
}

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

async function createMetricExecution(logicResults: LogicResult[]) {
  const metricsRef = db.collection("@metrics");
  for (const logicResult of logicResults) {
    const {name, execTime} = logicResult;
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
