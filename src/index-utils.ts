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
import * as batch from "./utils/batch";
import {CloudFunctionsServiceClient} from "@google-cloud/functions";
import QueryDocumentSnapshot = firestore.QueryDocumentSnapshot;
import DocumentData = FirebaseFirestore.DocumentData;
import Reference = database.Reference;
import {submitForm} from "emberflow-admin-client/lib";
import {FormData} from "emberflow-admin-client/lib/types";

export const _mockable = {
  getViewLogicsConfig: () => viewLogicConfigs,
  createNowTimestamp: () => admin.firestore.Timestamp.now(),
};


export async function distribute(
  docsByDstPath: Map<string, LogicResultDoc>,
  mode : "immediate" | "later" = "immediate") {
  if (mode === "immediate") {
    for (const dstPath of Array.from(docsByDstPath.keys()).sort()) {
      console.log(`Documents for path ${dstPath}:`);
      const resultDoc = docsByDstPath.get(dstPath);
      if (!resultDoc) continue;
      const {
        action,
        doc,
        instructions,
      } = resultDoc;
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

        // Merge document to dstPath
        const dstDocRef = db.doc(dstPath);
        await batch.set(dstDocRef, updateData);
        console.log(`Document merged to ${dstPath}`);
      }
    }

    if (batch.writeCount > 0) {
      console.log(`Committing final batch of ${batch.writeCount} writes...`);
      await batch.commit();
    }
  } else {
    console.log("Submitting to form for later processing...");
    docsByDstPath.forEach((doc, dstPath) => {
      if (doc.priority === "normal"){
        doc.priority = "high";
      } else if (doc.priority === "low"){
        doc.priority = "normal";
      }
    });
    // TODO: Need to make this idempotent
    const id = db.collection("@internal/forDistribution/distributions").doc().id;
    const formData: FormData = {
      "@docPath": `@internal/forDistribution/distributions/${id}`,
      "@actionType": "create",
      "logicResultDocs": Array.from(docsByDstPath.values()),
    };
    await submitForm(formData, (status: string, data: DocumentData, isLastUpdate: boolean) => {
      console.log(`Status: ${status}, data: ${JSON.stringify(data)}, isLastUpdate: ${isLastUpdate}`);
    });
  }
}

export async function validateForm(
  entity: string,
  form: FirebaseFirestore.DocumentData
): Promise<ValidateFormResult> {
  let hasValidationError = false;
  console.info(`Validating form for entity ${entity}`);
  const validate = validatorConfig[entity];
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

  const nextPageMarkers: (object|undefined)[] = Array(matchingLogics.length).fill(undefined);
  let page = 0;
  while (matchingLogics.length > 0) {
    const logicResults: LogicResult[] = [];
    for (let i = matchingLogics.length-1; i >= 0; i--) {
      const start = performance.now();
      const logic = matchingLogics[i];
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
  }
}

export function groupDocsByUserAndDstPath(docsByDstPath: Map<string, LogicResultDoc>, userId: string) {
  const userDocPath = docPaths["user"].replace("{userId}", userId);

  const userDocsByDstPath = new Map<string, LogicResultDoc>();
  const otherUsersDocsByDstPath = new Map<string, LogicResultDoc>();

  for (const [key, value] of docsByDstPath.entries()) {
    if (key.startsWith(userDocPath)) {
      userDocsByDstPath.set(key, value);
    } else {
      otherUsersDocsByDstPath.set(key, value);
    }
  }

  return {userDocsByDstPath, otherUsersDocsByDstPath};
}


export function getSecurityFn(entity: string): SecurityFn {
  return securityConfig[entity];
}


export async function expandConsolidateAndGroupByDstPath(logicDocs: LogicResultDoc[]): Promise<Map<string, LogicResultDoc>> {
  function warnOverwritingKeys(existing: any, incoming: any, type: string, dstPath: string) {
    for (const key in incoming) {
      if (Object.prototype.hasOwnProperty.call(existing, key)) {
        console.warn(`Overwriting key "${key}" in ${type} for dstPath "${dstPath}"`);
      }
    }
  }

  function processMerge(existingDoc: LogicResultDoc | undefined, doc: LogicResultDoc, dstPath: string) {
    if (existingDoc) {
      if (existingDoc.action === "merge") {
        warnOverwritingKeys(existingDoc.doc, doc.doc, "doc", dstPath);
        warnOverwritingKeys(existingDoc.instructions, doc.instructions, "instructions", dstPath);
        existingDoc.instructions = {...existingDoc.instructions, ...doc.instructions};
        existingDoc.doc = {...existingDoc.doc, ...doc.doc};
      } else {
        console.warn(
          `Action "merge" ignored because a "${existingDoc.action}" for dstPath "${dstPath}" already exists`
        );
      }
    } else {
      consolidated.set(dstPath, doc);
    }
  }

  function processDelete(existingDoc: LogicResultDoc | undefined, doc: LogicResultDoc, dstPath: string) {
    if (existingDoc) {
      if (existingDoc.action === "merge") {
        console.warn(`Action "merge" for dstPath "${dstPath}" is being overwritten by action "delete"`);
        consolidated.set(dstPath, doc);
      } else {
        console.warn(
          `Action "delete" ignored because a "${existingDoc.action}" for dstPath "${dstPath}" already exists`
        );
      }
    } else {
      consolidated.set(dstPath, doc);
    }
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

  const consolidated: Map<string, LogicResultDoc> = new Map();

  for (const doc of logicDocs) {
    const {
      dstPath,
      action,
    } = doc;
    const existingDoc = consolidated.get(dstPath);

    if (action === "merge") {
      processMerge(existingDoc, doc, dstPath);
    } else if (action === "delete") {
      processDelete(existingDoc, doc, dstPath);
    }
  }

  return consolidated;
}

export async function runViewLogics(dstPathLogicDocsMap: Map<string, LogicResultDoc>): Promise<LogicResult[]> {
  const logicResults: LogicResult[] = [];
  for (const [dstPath, logicResultDoc] of dstPathLogicDocsMap.entries()) {
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
