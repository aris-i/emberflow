import {GroupPatchMessage, GroupPatchProgress, GroupPatchType, Instructions, InstructionsMessage, LogicResultDoc} from "../types";
import {hydratePath, getDestPropAndDestPropId, findMatchingDocPathRegex} from "./paths";
import {
  GROUP_PATCH_TOPIC,
  GROUP_PATCH_TOPIC_NAME,
  FOR_DISTRIBUTION_TOPIC,
  FOR_DISTRIBUTION_TOPIC_NAME,
  INSTRUCTIONS_TOPIC,
  INSTRUCTIONS_TOPIC_NAME,
  admin,
  db,
} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import type {MessagePublishedData} from "firebase-functions/v2/pubsub";
import {distributeDoc, patchGroupDocs} from "../index-utils";
import {firestore} from "firebase-admin";
import {pubsubUtils} from "./pubsub";
import {reviveDateAndTimestamp} from "./misc";
import FieldValue = firestore.FieldValue;
import Transaction = firestore.Transaction;
import {findMatchingViewLogics, queueRunViewLogics} from "../logics/view-logics";
import {findMatchingPatchLogicsByEntity, queueRunPatchLogics} from "../logics/patch-logics";

/**
 * Converts a live Firestore {@link FieldValue} sentinel into its JSON-safe
 * instruction-string equivalent. Sentinels cannot survive `JSON.stringify`
 * (they collapse to `{}`), so any sentinel embedded in a `LogicResultDoc.doc`
 * must be turned into an instruction before the doc is queued.
 *
 * Returns `undefined` for values that are not a supported sentinel.
 *
 * @param {any} value - The value to inspect, possibly a FieldValue sentinel.
 * @return {string | undefined} The equivalent instruction string, or undefined.
 */
export function sentinelToInstruction(value: any): string | undefined {
  if (!(value instanceof admin.firestore.FieldValue)) {
    return undefined;
  }

  // `methodName` is exposed by every FieldTransform subclass and uniquely
  // identifies the kind of sentinel.
  const methodName = (value as any).methodName as string | undefined;

  if (methodName === "FieldValue.delete") {
    return "del";
  }
  if (methodName === "FieldValue.serverTimestamp") {
    return "serverTimestamp";
  }
  if (methodName === "FieldValue.increment") {
    const operand = (value as any).operand as number;
    if (operand === 1) {
      return "++";
    }
    if (operand === -1) {
      return "--";
    }
    return operand >= 0 ? `+${operand}` : `${operand}`;
  }
  if (methodName === "FieldValue.arrayUnion") {
    const elements = (value as any).elements as any[];
    return `arr(${elements.map((element) => `+${element}`).join(",")})`;
  }
  if (methodName === "FieldValue.arrayRemove") {
    const elements = (value as any).elements as any[];
    return `arr(${elements.map((element) => `-${element}`).join(",")})`;
  }

  return undefined;
}

/**
 * Walks `doc` and extracts every {@link FieldValue} sentinel into a matching
 * (possibly nested) {@link Instructions} object, removing the extracted keys
 * from `doc` so an empty/lossy value is not merged back on the queued path.
 *
 * Only plain object branches are traversed; arrays, Timestamps, Dates and other
 * class instances are left untouched.
 *
 * @param {object} doc - The document to walk; extracted sentinel keys are removed in place.
 * @return {Instructions} The extracted (possibly nested) instructions.
 */
export function extractSentinelsFromDoc(doc: { [key: string]: any }): Instructions {
  const instructions: Instructions = {};
  for (const key of Object.keys(doc)) {
    const value = doc[key];

    const instruction = sentinelToInstruction(value);
    if (instruction !== undefined) {
      instructions[key] = instruction;
      delete doc[key];
      continue;
    }

    // Unsupported sentinel: fail loudly instead of silently losing it to JSON.
    if (value instanceof admin.firestore.FieldValue) {
      console.warn(
        `Unsupported FieldValue sentinel at property "${key}"; it will be lost during JSON serialization.`
      );
      continue;
    }

    // Recurse only into plain object branches (skip arrays, Timestamps, Dates, etc.).
    if (value !== null && typeof value === "object" && value.constructor === Object) {
      const nested = extractSentinelsFromDoc(value);
      if (Object.keys(nested).length > 0) {
        instructions[key] = nested;
      }
    }
  }
  return instructions;
}

export const queueForDistributionLater = async (appVersion: string, targetVersion: string, ...logicResultDocs: LogicResultDoc[]) => {
  try {
    for (const logicResultDoc of logicResultDocs) {
      if (logicResultDoc.doc) {
        const extractedInstructions = extractSentinelsFromDoc(logicResultDoc.doc);
        if (Object.keys(extractedInstructions).length > 0) {
          if (logicResultDoc.instructions) {
            mergeInstructions(logicResultDoc.instructions, extractedInstructions);
          } else {
            logicResultDoc.instructions = extractedInstructions as Record<string, string>;
          }
        }
      }
      const forDistributionMessageId = await FOR_DISTRIBUTION_TOPIC.publishMessage(
        {json: {doc: logicResultDoc, targetVersion, appVersion}}
      );
      console.log(`queueForDistributionLater: Message ${forDistributionMessageId} published.`);
    }
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error(`Received error while publishing: ${error.message}`);
    } else {
      console.error("An unknown error occurred during publishing");
    }
    throw error;
  }
};

export async function onMessageForDistributionQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(FOR_DISTRIBUTION_TOPIC_NAME, event.id)) {
    return;
  }
  try {
    const {appVersion, targetVersion, doc} = event.data.message.json;
    const logicResultDoc = reviveDateAndTimestamp(doc) as LogicResultDoc;

    const {priority = "normal"} = logicResultDoc;
    if (priority === "high") {
      await distributeDoc(logicResultDoc, appVersion);
      if (findMatchingViewLogics(logicResultDoc, targetVersion)?.size) {
        await queueRunViewLogics(targetVersion, appVersion, [logicResultDoc]);
      }
      const {basePath} = getDestPropAndDestPropId(logicResultDoc.dstPath);
      const {entity} = findMatchingDocPathRegex(basePath);
      if (entity && findMatchingPatchLogicsByEntity(entity, appVersion).length > 0) {
        await queueRunPatchLogics(appVersion, logicResultDoc.dstPath);
      }
    } else if (priority === "normal") {
      logicResultDoc.priority = "high";
      await queueForDistributionLater(appVersion, targetVersion, logicResultDoc);
    } else if (priority === "low") {
      logicResultDoc.priority = "normal";
      await queueForDistributionLater(appVersion, targetVersion, logicResultDoc);
    }

    await pubsubUtils.trackProcessedIds(FOR_DISTRIBUTION_TOPIC_NAME, event.id);
    return "Processed for distribution later";
  } catch (e) {
    console.error("Error in onMessageForDistributionQueue", e);
    throw new Error("Error in onMessageForDistributionQueue");
  }
}

export const queueInstructions = async (dstPath: string, instructions: { [p: string]: string }) => {
  try {
    await INSTRUCTIONS_TOPIC.publishMessage({json: {dstPath, instructions}});
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error(`Received error while publishing: ${error.message}`);
    } else {
      console.error("An unknown error occurred during publishing");
    }
    throw error;
  }
};

export async function convertInstructionsToDbValues(txn: Transaction, instructions: Instructions, destProp?: string, destPropId?: string) {
  let actualUpdateData: { [key: string ]: object | FieldValue | number} = {};
  let actualRemoveData: { [key: string]: object | FieldValue | number } = {};
  const updateData: { [key: string ]: FieldValue | number } = {};
  const removeData: { [key: string]: FieldValue } = {};

  if (destProp) {
    if (destPropId) {
      actualUpdateData[destProp] = {[destPropId]: updateData};
      actualRemoveData[destProp] = {[destPropId]: removeData};
    } else {
      actualUpdateData[destProp] = updateData;
      actualRemoveData[destProp] = removeData;
    }
  } else {
    actualUpdateData = updateData;
    actualRemoveData = removeData;
  }

  await _convert(txn, instructions, updateData, removeData);

  // Clean up to empty destProps to avoid overriding whole object
  if (destProp && Object.keys(updateData).length == 0) {
    actualUpdateData = {};
  }
  if (destProp && Object.keys(removeData).length == 0) {
    actualRemoveData = {};
  }

  return {updateData: actualUpdateData, removeData: actualRemoveData};
}

async function _convert(txn: Transaction, instructions: Instructions, updateData: any, removeData: any, path = "") {
  for (const [property, instruction] of Object.entries(instructions)) {
    const currentPath = path ? `${path}.${property}` : property;
    if (typeof instruction === "object") {
      await _convert(txn, instruction as Instructions, updateData, removeData, currentPath);
      continue;
    }

    if (instruction === "++") {
      updateData[currentPath] = admin.firestore.FieldValue.increment(1);
    } else if (instruction === "--") {
      updateData[currentPath] = admin.firestore.FieldValue.increment(-1);
    } else if (instruction.startsWith("+")) {
      const incrementValue = parseFloat(instruction.slice(1));
      if (isNaN(incrementValue)) {
        console.log(`Invalid increment value ${instruction} for property ${currentPath}`);
      } else {
        updateData[currentPath] = admin.firestore.FieldValue.increment(incrementValue);
      }
    } else if (instruction.startsWith("-")) {
      const decrementValue = parseFloat(instruction.slice(1));
      if (isNaN(decrementValue)) {
        console.log(`Invalid decrement value ${instruction} for property ${currentPath}`);
      } else {
        updateData[currentPath] = admin.firestore.FieldValue.increment(-decrementValue);
      }
    } else if (instruction.startsWith("arr")) {
      const regex = /\((.*?)\)/;
      const match = instruction.match(regex);

      if (!match) {
        console.log(`Invalid instruction ${instruction} for property ${currentPath}`);
        continue;
      }

      const paramsStr = match[1];
      if (!paramsStr) {
        console.log(`No values found in instruction ${instruction} for property ${currentPath}`);
        continue;
      }

      const params = paramsStr.split(",").map((value) => value.trim());
      const valuesToAdd = [];
      const valuesToRemove = [];
      for (const param of params) {
        const operation = param[0];
        let value = param;
        if (operation === "-" || operation === "+") {
          value = param.slice(1);
        }
        if (operation === "-") {
          valuesToRemove.push(value);
          continue;
        }
        valuesToAdd.push(value);
      }
      if (valuesToAdd.length > 0) {
        updateData[currentPath] = admin.firestore.FieldValue.arrayUnion(...valuesToAdd);
      }
      if (valuesToRemove.length > 0) {
        removeData[currentPath] = admin.firestore.FieldValue.arrayRemove(...valuesToRemove);
      }
    } else if (instruction === "del") {
      updateData[currentPath] = admin.firestore.FieldValue.delete();
    } else if (instruction === "serverTimestamp") {
      updateData[currentPath] = admin.firestore.FieldValue.serverTimestamp();
    } else if (instruction.startsWith("globalCounter")) {
      const regex = /globalCounter\(([^,]+)(?:,\s*(\d+))?\)/;
      const match = instruction.match(regex);

      if (!match) {
        console.log(`Invalid global instruction ${instruction} for property ${currentPath}`);
        continue;
      }

      const counterName = match[1];
      const maxValue = parseInt(match[2], 10);
      const now = admin.firestore.Timestamp.now();

      try {
        let newCount: number;
        const counterRef = db.doc(`@counters/${counterName}`);
        const counterDoc = await txn.get(counterRef);
        const counterData = counterDoc?.data();
        if (!counterData) {
          newCount = 1;
          const newDocument = {
            "@id": counterName,
            "count": newCount,
            "lastUpdatedAt": now,
          };
          txn.set(counterRef, newDocument);
        } else {
          const {count} = counterData;
          const maxValueReached = maxValue && count >= maxValue;

          newCount = maxValueReached ? 1 : count + 1;

          txn.update(counterRef, {
            "count": newCount,
            "lastUpdatedAt": now,
          });
        }
        updateData[currentPath] = newCount;
      } catch (error) {
        console.error(error);
      }
    } else {
      console.log(`Invalid instruction ${instruction} for property ${currentPath}`);
    }
  }
}

export async function onMessageInstructionsQueue(event: CloudEvent<MessagePublishedData> | Map<string, Instructions>) {
  async function applyInstructions(txn: Transaction, instructions: Instructions, dstPath: string) {
    const {basePath, destProp, destPropId} = getDestPropAndDestPropId(dstPath);
    const {updateData, removeData} = await convertInstructionsToDbValues(
      txn,
      instructions,
      destProp,
      destPropId
    );
    const dstDocRef = db.doc(basePath);
    if (Object.keys(updateData).length > 0) {
      txn.set(dstDocRef, updateData, {merge: true});
    }
    if (Object.keys(removeData).length > 0) {
      txn.set(dstDocRef, removeData, {merge: true});
    }
  }

  if (event instanceof Map) {
    // Process the reduced instructions here
    await db.runTransaction(async (txn) => {
      for (const [dstPath, instructions] of event.entries()) {
        await applyInstructions(txn, instructions, dstPath);
      }
    });
  } else {
    if (await pubsubUtils.isProcessed(INSTRUCTIONS_TOPIC_NAME, event.id)) {
      console.log("Skipping duplicate message");
      return;
    }
    try {
      const instructionsMessage: InstructionsMessage = event.data.message.json;

      const {dstPath, instructions} = instructionsMessage;
      await db.runTransaction(async (txn) => {
        await applyInstructions(txn, instructions, dstPath);
      });

      await pubsubUtils.trackProcessedIds(INSTRUCTIONS_TOPIC_NAME, event.id);
    } catch (e) {
      console.error("PubSub message was not JSON", e);
      throw new Error("No json in message");
    }
  }
}

export const mergeInstructions = (existingInstructions: Instructions, instructions: Instructions) => {
  function getValue(instruction: string) {
    if (instruction === "++") {
      return 1;
    } else if (instruction === "--") {
      return -1;
    } else if (instruction.startsWith("+")) {
      return parseFloat(instruction.slice(1));
    } else if (instruction.startsWith("-")) {
      return -parseFloat(instruction.slice(1));
    } else {
      return 0;
    }
  }

  function getArrValues(existingInstruction: string) {
    const existingParamsMap = new Map<string, number>();
    const regex = /arr\((.*?)\)/;
    const match = existingInstruction.match(regex);
    const existingParamsStr = match ? match[1] : "";
    const existingParams = existingParamsStr.split(",").map((value) => value.trim());
    // Let's create a map of existingParams to their values without the sign
    for (const param of existingParams) {
      // Remove the "+" or "-" sign at the start of param.  If there is no sign, then it's a "+" sign
      const sign = param.startsWith("-") ? -1 : 1;
      const value = param.replace(/^[+-]/, "");
      existingParamsMap.set(value, sign);
    }
    return existingParamsMap;
  }

  for (const property of Object.keys(instructions)) {
    const existingInstruction = existingInstructions[property];
    const instruction = instructions[property];

    if (typeof instruction === "object") {
      if (typeof existingInstruction === "string") {
        continue;
      }
      if (!existingInstruction) {
        existingInstructions[property] = instruction;
        continue;
      }
      mergeInstructions(existingInstruction as Instructions, instruction as Instructions);
      continue;
    }

    if (!existingInstruction) {
      existingInstructions[property] = instruction;
      continue;
    }

    if (typeof existingInstruction === "object") {
      continue;
    }

    // check if existingInstructions and instructions starts with '+' or '-'
    if (/^[+-]/.test(existingInstruction) && /^[+-]/.test(instruction)) {
      const newValue = getValue(existingInstruction) + getValue(instruction);
      if (newValue === 0) {
        delete existingInstructions[property];
        continue;
      }
      existingInstructions[property] = newValue > 0 ? `+${newValue}` : `${newValue}`;
      continue;
    }

    if (existingInstruction.startsWith("arr") && instruction.startsWith("arr")) {
      // Parse values inside parentheses on this pattern arr([+-]value1, [+-]value2, ...)
      const existingParamsMap = getArrValues(existingInstruction);
      const paramsMap = getArrValues(instruction);
      // Loop through paramsMap and merge with existingParamsMap
      for (const [param, sign] of paramsMap.entries()) {
        const existingSign = existingParamsMap.get(param) || 0;
        const newValue = existingSign + sign;
        if (newValue === 0) {
          existingParamsMap.delete(param);
          continue;
        }
        existingParamsMap.set(param, newValue > 0 ? 1 : -1);
      }
      // Convert existingParamsMap to string
      const existingParams = Array.from(existingParamsMap.entries())
        .map(([param, sign]) => `${sign > 0 ? "+" : "-"}${param}`);
      const existingParamsStr = existingParams.join(",");
      existingInstructions[property] = `arr(${existingParamsStr})`;
      continue;
    }

    if (existingInstruction === "del") {
      console.warn(`Property ${property} is set to be deleted. Skipping..`);
      continue;
    }

    if (instruction === "del") {
      existingInstructions[property] = "del";
      continue;
    }

    // Identical non-arithmetic instructions (e.g. serverTimestamp) are idempotent; keep as-is.
    if (existingInstruction === instruction) {
      continue;
    }

    console.warn(`Property ${property} has conflicting instructions ${existingInstruction} and ${instruction}. Skipping..`);
  }
};

export const instructionsReducer = async (reducedInstructions: Map<string, Instructions>, event: CloudEvent<MessagePublishedData>) => {
  try {
    const instructionsMessage: InstructionsMessage = event.data.message.json;
    console.log("Instructions reducer: Received user logic result doc:", instructionsMessage);

    const {dstPath, instructions} = instructionsMessage;
    const existingInstructions = reducedInstructions.get(dstPath);
    console.debug("Instructions reducer: Existing instructions", existingInstructions);
    if (!existingInstructions) {
      reducedInstructions.set(dstPath, instructions);
    } else {
      mergeInstructions(existingInstructions, instructions);
    }
    console.debug("Instructions reducer: Reduced instructions", reducedInstructions);
  } catch (e) {
    console.error("Error in instructionsReducer:", e);
  }
};

export function getGroupPatchStatusPath(
  collectionPath: string,
  patchType: GroupPatchType,
  backFillPatchName?: string,
): string {
  const patchIdentity = patchType === "back-fill" ?
    `back-fill_${backFillPatchName}` :
    "patch-logics";
  return `@emberflow/internal/group-patches/${collectionPath.replace(/\//g, "_")}_${patchIdentity}`;
}

export interface QueueGroupPatchParams {
  path: string;
  patchType: GroupPatchType;
  backFillPatchName?: string;
  appVersion?: string;
  lastPatchedId?: string;
  /**
   * How many times this patch chain has rescheduled itself. Propagated into the
   * queued message so the engine's circuit breaker can abort a runaway backfill.
   */
  iteration?: number;
}

export const queueGroupPatch = async (params: QueueGroupPatchParams) => {
  const {path, patchType, backFillPatchName, appVersion, lastPatchedId, iteration} = params;
  const segments = path.split("/").filter((s) => s.length > 0);
  const collectionPath = segments.length % 2 === 0 ?
    "/" + segments.slice(0, -1).join("/") :
    (path.startsWith("/") ? "" : "/") + path;

  if (!collectionPath || !collectionPath.includes("/")) {
    return;
  }

  // If this is the start of a new patch (no lastPatchedId), check/lock it in Firestore.
  // Placeholder (wildcard) paths are NOT locked/tracked here: they never patch a real
  // document, they only get hydrated into concrete collections downstream, and each
  // concrete collection goes through queueGroupPatch again and gets its own status
  // doc + in-flight guard (which is what actually prevents a runaway backfill). So for
  // a placeholder path we skip straight to publishing the hydration message.
  if (!lastPatchedId && !collectionPath.includes("{")) {
    const patchStatusPath = getGroupPatchStatusPath(collectionPath, patchType, backFillPatchName);
    const patchStatusRef = db.doc(patchStatusPath);

    try {
      const alreadyQueued = await db.runTransaction(async (txn) => {
        const doc = await txn.get(patchStatusRef);
        if (doc.exists) {
          const data = doc.data();
          const status = data?.status;
          // A patch that is already in flight is NEVER restarted here. Re-locking +
          // re-publishing while a chain is still running would spawn a second,
          // overlapping chain (a runaway backfill), so any trigger for the same
          // collection is treated as ALREADY QUEUED and ignored until it settles.
          const isInFlight = status === "queued" || status === "running" || status === "hydrating";
          if (isInFlight) {
            console.info(
              `[backfill-patch-emberflow] queueGroupPatch: status doc already in-flight (status="${status}") for ${collectionPath} -> treating as ALREADY QUEUED, will NOT re-publish.`
            );
            return true;
          }
          // Not in flight (settled). Only "error"/"reset" may restart; any other
          // settled status (e.g. "completed") is treated as already done. An
          // operator restart is done by stamping the status doc(s) to "reset"
          // (see onGroupPatchRequest), which falls through here to re-lock/re-run.
          if (status !== "error" && status !== "reset") {
            console.info(
              `[backfill-patch-emberflow] queueGroupPatch: status doc already exists with status="${status}" -> treating as ALREADY QUEUED, will NOT re-publish for ${collectionPath}.`
            );
            return true; // Already exists and not in a state that allows restart
          }
        }
        // Create or update record to "lock" it
        txn.set(patchStatusRef, {
          status: "queued",
          patchType,
          backFillPatchName: backFillPatchName ?? null,
          collectionPath,
          count: 0,
          lastPatchedId: null, // Reset cursor if restarting
          createdAt: admin.firestore.Timestamp.now(),
          updatedAt: admin.firestore.Timestamp.now(),
        }, {merge: true});
        return false;
      });

      if (alreadyQueued) {
        return;
      }
    } catch (e) {
      console.error(`[GroupPatch] Error during transaction in queueGroupPatch for ${collectionPath}:`, e);
      // We don't throw here to avoid failing the distribution, but we don't proceed to publish
      return;
    }
  }

  try {
    const message: GroupPatchMessage = {collectionPath, patchType, backFillPatchName, appVersion, lastPatchedId, iteration};
    await GROUP_PATCH_TOPIC.publishMessage({json: message});
  } catch (error: unknown) {
    console.error(`[GroupPatch] Received error while publishing to ${GROUP_PATCH_TOPIC_NAME}:`, error);
    throw error;
  }
};

export async function onMessageGroupPatchQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(GROUP_PATCH_TOPIC_NAME, event.id)) {
    return;
  }

  try {
    const {collectionPath, patchType, backFillPatchName, appVersion, lastPatchedId, hydrationState, iteration} =
      event.data.message.json as GroupPatchMessage;

    if (collectionPath.includes("{")) {
      console.log(`[GroupPatch] Hydration in Progress for ${collectionPath}...`);
      const {documentPaths, hydrationState: nextHydrationState} = await hydratePath(collectionPath, {}, hydrationState);

      // Note: we do NOT create/update a status doc for the placeholder (wildcard)
      // path here. Placeholder paths are not tracked (see queueGroupPatch) — only
      // the concrete collections that hydration fans out to get their own status
      // doc + in-flight guard.
      if (nextHydrationState) {
        // Re-queue hydration
        const message: GroupPatchMessage = {
          collectionPath,
          patchType,
          backFillPatchName,
          appVersion,
          hydrationState: nextHydrationState,
        };
        await GROUP_PATCH_TOPIC.publishMessage({json: message});
      } else {
        console.log(`[GroupPatch] Hydration complete for ${collectionPath}`);
      }

      console.log(`[GroupPatch] Hydrated ${documentPaths.length} paths for ${collectionPath}. Remaining batches: ${nextHydrationState ? "yes" : "no"}`);

      for (const path of documentPaths) {
        // Guard against re-queueing a hydrated path that still contains a
        // placeholder ("{"). Under normal template expansion, hydratePath only
        // ever emits fully-resolved paths, so a "{" here means a real document
        // id literally contains "{" (likely bad data written from an
        // un-hydrated path). Re-queueing such a path would make it hydrate again
        // (collectionPath.includes("{") is true), producing an infinite loop.
        if (path.includes("{")) {
          console.warn(
            `[GroupPatch] Skipping re-queue of hydrated path that still contains a placeholder: ${path}. ` +
            "This usually means a document id literally contains '{' (likely bad data written from an un-hydrated path)."
          );
          continue;
        }
        // We trigger a patch for each hydrated path.
        // Note: These will go through the same queueGroupPatch logic,
        // so they will be tracked/locked individually. If the operator stamped
        // the concrete status doc(s) to "reset", queueGroupPatch restarts them.
        await queueGroupPatch({path, patchType, backFillPatchName, appVersion});
      }
    } else {
      await patchGroupDocs({collectionPath, patchType, backFillPatchName, appVersion, lastPatchedId, iteration});
    }
    await pubsubUtils.trackProcessedIds(GROUP_PATCH_TOPIC_NAME, event.id);
  } catch (e) {
    console.error("[GroupPatch] Error in onMessageGroupPatchQueue", e);
    throw new Error("Error in onMessageGroupPatchQueue");
  }
}

export interface GetGroupPatchProgressParams {
  collectionPath: string;
  patchType: GroupPatchType;
  backFillPatchName?: string;
}

export async function getGroupPatchProgress(
  params: GetGroupPatchProgressParams
): Promise<GroupPatchProgress | undefined> {
  const {collectionPath, patchType, backFillPatchName} = params;
  const patchStatusPath = getGroupPatchStatusPath(collectionPath, patchType, backFillPatchName);
  const patchStatusDoc = await db.doc(patchStatusPath).get();
  if (!patchStatusDoc.exists) {
    return undefined;
  }

  const data = patchStatusDoc.data();
  if (!data) {
    return undefined;
  }

  return {
    patchType,
    backFillPatchName,
    collectionPath,
    status: data.status,
    lastPatchedId: data.lastPatchedId ?? undefined,
    patchedCount: data.count ?? undefined,
    startedAt: data.createdAt ?? undefined,
    updatedAt: data.updatedAt ?? undefined,
    error: data.error ?? undefined,
  };
}
