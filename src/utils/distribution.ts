import {
  Instructions,
  InstructionsMessage,
  LogicResultDoc,
} from "../types";
import {
  admin,
  db,
  FOR_DISTRIBUTION_TOPIC,
  FOR_DISTRIBUTION_TOPIC_NAME,
  INSTRUCTIONS_TOPIC,
  INSTRUCTIONS_TOPIC_NAME,
} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {distributeDoc} from "../index-utils";
import {firestore} from "firebase-admin";
import {pubsubUtils} from "./pubsub";
import {reviveDateAndTimestamp} from "./misc";
import FieldValue = firestore.FieldValue;

export const queueForDistributionLater = async (...logicResultDocs: LogicResultDoc[]) => {
  try {
    for (const logicResultDoc of logicResultDocs) {
      const forDistributionMessageId = await FOR_DISTRIBUTION_TOPIC.publishMessage({json: logicResultDoc});
      console.log(`Message ${forDistributionMessageId} published.`);
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
    console.log("Skipping duplicate message");
    return;
  }
  try {
    const logicResultDoc = reviveDateAndTimestamp(event.data.message.json) as LogicResultDoc;
    console.log("Received user logic result doc:", logicResultDoc);

    console.info("Running For Distribution");
    const {priority = "normal"} = logicResultDoc;
    if (priority === "high") {
      await distributeDoc(logicResultDoc);
    } else if (priority === "normal") {
      logicResultDoc.priority = "high";
      await queueForDistributionLater(logicResultDoc);
    } else if (priority === "low") {
      logicResultDoc.priority = "normal";
      await queueForDistributionLater(logicResultDoc);
    }

    await pubsubUtils.trackProcessedIds(FOR_DISTRIBUTION_TOPIC_NAME, event.id);
    return "Processed for distribution later";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}

export const queueInstructions = async (dstPath: string, instructions: { [p: string]: string }) => {
  try {
    const messageId = await INSTRUCTIONS_TOPIC.publishMessage({json: {dstPath, instructions}});
    console.log(`Message ${messageId} published.`);
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error(`Received error while publishing: ${error.message}`);
    } else {
      console.error("An unknown error occurred during publishing");
    }
    throw error;
  }
};

export async function convertInstructionsToDbValues(instructions: Instructions) {
  const updateData: { [key: string ]: FieldValue | number } = {};
  const removeData: { [key: string]: FieldValue } = {};

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
    } else if (instruction.startsWith("arr")) {
      const regex = /\((.*?)\)/;
      const match = instruction.match(regex);

      if (!match) {
        console.log(`Invalid instruction ${instruction} for property ${property}`);
        continue;
      }

      const paramsStr = match[1];
      if (!paramsStr) {
        console.log(`No values found in instruction ${instruction} for property ${property}`);
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
        updateData[property] = admin.firestore.FieldValue.arrayUnion(...valuesToAdd);
      }
      if (valuesToRemove.length > 0) {
        removeData[property] = admin.firestore.FieldValue.arrayRemove(...valuesToRemove);
      }
    } else if (instruction === "del") {
      updateData[property] = admin.firestore.FieldValue.delete();
    } else if (instruction.startsWith("globalCounter")) {
      const regex = /globalCounter\(([^,]+)(?:,\s*(\d+))?\)/;
      const match = instruction.match(regex);

      if (!match) {
        console.log(`Invalid global instruction ${instruction} for property ${property}`);
        continue;
      }

      const counterName = match[1];
      const maxValue = parseInt(match[2], 10);
      const now = admin.firestore.Timestamp.now();

      await db.runTransaction(async (transaction) => {
        let newCount: number;
        const counterRef = db.doc(`@counters/${counterName}`);
        const counterDoc = await transaction.get(counterRef);
        const counterData = counterDoc?.data();
        if (!counterData) {
          newCount = 1;
          const newDocument = {
            "@id": counterName,
            "count": newCount,
            "lastUpdatedAt": now,
          };
          transaction.set(counterRef, newDocument);
        } else {
          const {count, lastUpdatedAt} = counterData;

          const dateNow = new Date();
          dateNow.setHours(0, 0, 0, 0);

          const lastUpdatedAtDate = lastUpdatedAt.toDate();

          const isDifferentDate = lastUpdatedAtDate < dateNow;
          const maxValueReached = maxValue && count >= maxValue;

          newCount = maxValueReached || isDifferentDate ? 1 : count + 1;

          transaction.update(counterRef, {
            "count": newCount,
            "lastUpdatedAt": now,
          });
        }
        updateData[property] = newCount;
      });
    } else {
      console.log(`Invalid instruction ${instruction} for property ${property}`);
    }
  }
  return {updateData, removeData};
}

export async function onMessageInstructionsQueue(event: CloudEvent<MessagePublishedData> | Map<string, Instructions>) {
  async function applyInstructions(instructions: Instructions, dstPath: string) {
    const {updateData, removeData} = await convertInstructionsToDbValues(instructions);
    const dstDocRef = db.doc(dstPath);
    if (Object.keys(updateData).length > 0) {
      await dstDocRef.update(updateData);
    }
    if (Object.keys(removeData).length > 0) {
      await dstDocRef.update(removeData);
    }
  }

  if (event instanceof Map) {
    // Process the reduced instructions here
    for (const [dstPath, instructions] of event.entries()) {
      await applyInstructions(instructions, dstPath);
    }
  } else {
    if (await pubsubUtils.isProcessed(INSTRUCTIONS_TOPIC_NAME, event.id)) {
      console.log("Skipping duplicate message");
      return;
    }
    try {
      const instructionsMessage: InstructionsMessage = event.data.message.json;
      console.log("Received user logic result doc:", instructionsMessage);

      console.info("Applying Instructions");
      const {dstPath, instructions} = instructionsMessage;
      await applyInstructions(instructions, dstPath);

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
      return parseInt(instruction.slice(1));
    } else if (instruction.startsWith("-")) {
      return -parseInt(instruction.slice(1));
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
    if (!existingInstruction) {
      existingInstructions[property] = instruction;
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

    console.warn(`Property ${property} has conflicting instructions ${existingInstruction} and ${instruction}. Skipping..`);
  }
};

export const instructionsReducer = async (reducedInstructions: Map<string, Instructions>, event: CloudEvent<MessagePublishedData>) => {
  if (await pubsubUtils.isProcessed(INSTRUCTIONS_TOPIC_NAME, event.id)) {
    console.log("Skipping duplicate message");
    return;
  }
  try {
    const instructionsMessage: InstructionsMessage = event.data.message.json;
    console.log("Received user logic result doc:", instructionsMessage);

    const {dstPath, instructions} = instructionsMessage;
    const existingInstructions = reducedInstructions.get(dstPath);
    if (!existingInstructions) {
      reducedInstructions.set(dstPath, instructions);
    } else {
      mergeInstructions(existingInstructions, instructions);
    }

    await pubsubUtils.trackProcessedIds(INSTRUCTIONS_TOPIC_NAME, event.id);
  } catch (e) {
    console.error("PubSub message was not JSON. Continue processing queue", e);
  }
};
