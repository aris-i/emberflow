import {
  Action,
  LogicActionType,
  LogicResult,
  LogicResultDoc,
  ScheduledEntity,
  SecurityFn,
  ValidateFormResult,
} from "./types";
import {database, firestore} from "firebase-admin";
import {
  admin,
  db,
  docPaths,
  logicConfigs,
  projectConfig,
  securityConfig,
  validatorConfig,
  viewLogicConfigs,
} from "./index";
import {syncPeerViews} from "./logics/view-logics";
import {expandAndGroupDocPathsByEntity, findMatchingDocPathRegex} from "./utils/paths";
import {deepEqual} from "./utils/misc";
import {CloudFunctionsServiceClient} from "@google-cloud/functions";
import {FormData} from "emberflow-admin-client/lib/types";
import {BatchUtil} from "./utils/batch";
import {queueSubmitForm} from "./utils/forms";
import QueryDocumentSnapshot = firestore.QueryDocumentSnapshot;
import DocumentData = FirebaseFirestore.DocumentData;
import Reference = database.Reference;

export const _mockable = {
  getViewLogicsConfig: () => viewLogicConfigs,
  createNowTimestamp: () => admin.firestore.Timestamp.now(),
};


export async function distribute(
  docsByDstPath: Map<string, LogicResultDoc[]> ) {
  const batch = BatchUtil.getInstance();
  for (const dstPath of Array.from(docsByDstPath.keys()).sort()) {
    console.log(`Documents for path ${dstPath}:`);
    const resultDocs = docsByDstPath.get(dstPath);
    if (!resultDocs) continue;
    for (const resultDoc of resultDocs) {
      const {
        action,
        doc,
        instructions,
      } = resultDoc;
      console.debug(`Distributing doc with Action: ${action}`);
      if (action === "delete") {
        // Delete document at dstPath
        const dstDocRef = db.doc(dstPath);
        await batch.deleteDoc(dstDocRef);
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

        const dstDocRef = db.doc(dstPath);
        await batch.set(dstDocRef, updateData);
        console.log(`Document merged to ${dstPath}`);
      } else if (action === "submit-form") {
        console.debug("Queuing submit form...");
        const formData: FormData = {
          "@docPath": dstPath,
          "@actionType": "create",
          ...doc,
        };
        await queueSubmitForm(formData);
      }
    }
  }

  if (batch.writeCount > 0) {
    console.log(`Committing final batch of ${batch.writeCount} writes...`);
    await batch.commit();
  }
}

export async function distributeLater(docsByDstPath: Map<string, LogicResultDoc[]>, id: string) {
  console.log("Submitting to form for later processing...");
  const documents = Array.from(docsByDstPath.values()).flat();
  if (documents.length === 0) {
    return;
  }
  documents.forEach((doc) => {
    if (!doc.priority || doc.priority === "normal") {
      doc.priority = "high";
    } else if (doc.priority === "low") {
      doc.priority = "normal";
    }
  });
  const formData: FormData = {
    "@docPath": `@internal/forDistribution/distributions/${id}`,
    "@actionType": "create",
    "logicResultDocs": documents,
  };
  await queueSubmitForm(formData);
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

export async function runBusinessLogics(
  actionType: LogicActionType,
  formModifiedFields: DocumentData,
  entity: string,
  action: Action,
  distributeFn: (logicResults: LogicResult[], page: number) => Promise<void>,
) {
  const matchingLogics = logicConfigs.filter((logic) => {
    return (
      (logic.actionTypes === "all" || logic.actionTypes.includes(actionType)) &&
            (logic.modifiedFields === "all" || logic.modifiedFields.some((field) => field in formModifiedFields)) &&
            (logic.entities === "all" || logic.entities.includes(entity))
    );
  });
  console.debug("Matching logics:", matchingLogics.map((logic) => logic.name));
  if (matchingLogics.length === 0) {
    console.log("No matching logics found");
    await distributeFn([], 0);
    return;
  }

  const config = (await db.doc("@server/config").get()).data();
  const maxLogicResultPages = config?.maxLogicResultPages || 20;
  let page = 0;
  const nextPageMarkers: (object|undefined)[] = Array(matchingLogics.length).fill(undefined);
  while (matchingLogics.length > 0) {
    if (page > 0) {
      console.debug(`Page ${page} Remaining logics:`, matchingLogics.map((logic) => logic.name));
    }
    const logicResults: LogicResult[] = [];
    for (let i = matchingLogics.length-1; i >= 0; i--) {
      const start = performance.now();
      const logic = matchingLogics[i];
      console.debug("Running logic:", logic.name, "nextPageMarker:", nextPageMarkers[i]);
      try {
        const result = await logic.logicFn(action, nextPageMarkers[i]);
        const end = performance.now();
        const execTime = end - start;
        const {status, nextPage} = result;
        if (status === "finished") {
          matchingLogics.splice(i, 1);
          nextPageMarkers.splice(i, 1);
        } else if (status === "partial-result") {
          nextPageMarkers[i] = nextPage;
        }

        logicResults.push({...result, execTime, timeFinished: admin.firestore.Timestamp.now()});
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
    await distributeFn(logicResults, page++);
    if (page >= maxLogicResultPages) {
      console.warn(`Maximum number of logic result pages (${maxLogicResultPages}) reached`);
      break;
    }
  }
}

export function groupDocsByUserAndDstPath(docsByDstPath: Map<string, LogicResultDoc[]>, userId: string) {
  const userDocPath = docPaths["user"].replace("{userId}", userId);

  const userDocsByDstPath = new Map<string, LogicResultDoc[]>();
  const otherUsersDocsByDstPath = new Map<string, LogicResultDoc[]>();

  for (const [key, values] of docsByDstPath.entries()) {
    if (key.startsWith(userDocPath)) {
      userDocsByDstPath.set(key, values);
    } else {
      otherUsersDocsByDstPath.set(key, values);
    }
  }

  return {userDocsByDstPath, otherUsersDocsByDstPath};
}


export function getSecurityFn(entity: string): SecurityFn {
  return securityConfig[entity];
}


export async function expandConsolidateAndGroupByDstPath(logicDocs: LogicResultDoc[]): Promise<Map<string, LogicResultDoc[]>> {
  function warnOverwritingKeys(existing: any, incoming: any, type: string, dstPath: string) {
    for (const key in incoming) {
      if (Object.prototype.hasOwnProperty.call(existing, key)) {
        console.warn(`Overwriting key "${key}" in ${type} for dstPath "${dstPath}"`);
      }
    }
  }

  function processMerge(existingDocs: LogicResultDoc[], doc: LogicResultDoc, dstPath: string) {
    let merged = false;
    for (const existingDoc of existingDocs) {
      if (existingDoc.action === "merge") {
        warnOverwritingKeys(existingDoc.doc, doc.doc, "doc", dstPath);
        warnOverwritingKeys(existingDoc.instructions, doc.instructions, "instructions", dstPath);
        existingDoc.instructions = {...existingDoc.instructions, ...doc.instructions};
        existingDoc.doc = {...existingDoc.doc, ...doc.doc};
        merged = true;
        break;
      }
    }
    if (!merged) {
      existingDocs.push(doc);
    }
  }

  function processDelete(existingDocs: LogicResultDoc[], doc: LogicResultDoc, dstPath: string) {
    for (let i = existingDocs.length-1; i >= 0; i--) {
      const existingDoc = existingDocs[i];
      if (existingDoc.action === "merge" || existingDoc.action === "delete") {
        console.warn(`Action ${existingDoc.action} for dstPath "${dstPath}" is being overwritten by action "delete"`);
        existingDocs.splice(i, 1);
      }
    }
    existingDocs.push(doc);
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

    if (action === "merge") {
      processMerge(existingDocs, doc, dstPath);
    } else if (action === "delete") {
      processDelete(existingDocs, doc, dstPath);
    } else if (action === "submit-form") {
      existingDocs.push(doc);
    }
  }

  return consolidated;
}

export async function runViewLogics(dstPathLogicDocsMap: Map<string, LogicResultDoc[]>): Promise<LogicResult[]> {
  const logicResults: LogicResult[] = [];
  for (const [dstPath, logicResultDocs] of dstPathLogicDocsMap.entries()) {
    for (const logicResultDoc of logicResultDocs) {
      const {
        action,
        doc,
        instructions,
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
        continue;
      }
      const matchingLogics = _mockable.getViewLogicsConfig().filter((logic) => {
        return (
          (
            action === "merge" &&
                logic.modifiedFields.some((field) => modifiedFields.includes(field)) &&
                logic.entity === entity
          ) ||
            (
              action === "delete" &&
                logic.entity === entity
            )
        );
      });
      // TODO: Handle errors
      // TODO: Add logic for execTime
      const results = await Promise.all(matchingLogics.map((logic) => logic.viewLogicFn(logicResultDoc)));
      logicResults.push(...results);
    }
  }
  return logicResults;
}

export async function runPeerSyncViews(userDocsByDstPath: Map<string, LogicResultDoc>) : Promise<LogicResult[]> {
  const logicResults: LogicResult[] = [];
  for (const [_, logicResultDoc] of userDocsByDstPath.entries()) {
    logicResults.push(await syncPeerViews(logicResultDoc));
  }
  return logicResults;
}

export async function processScheduledEntities() {
  // Query all documents inside @scheduled collection where runAt is less than now
  const now = _mockable.createNowTimestamp();
  const scheduledDocs = await db.collection("@scheduled")
    .where("runAt", "<=", now).get();
    // For each document, get path, data and docId.  Then copy data to the path
  const batch = BatchUtil.getInstance();
  try {
    scheduledDocs.forEach((doc) => {
      const {
        colPath,
        data,
      } = doc.data() as ScheduledEntity;
      const docRef = db.collection(colPath).doc(doc.id);
      batch.set(docRef, data);
      batch.deleteDoc(doc.ref);
    });
  } finally {
    await batch.commit();
  }
}

export async function onDeleteFunction(snapshot: QueryDocumentSnapshot) {
  const data = snapshot.data();
  if (!data) {
    console.error("Data should not be null");
    return;
  }
  const {name} = data;
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
