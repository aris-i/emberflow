import {InstructionsMessage, LogicResultDoc} from "../types";
import {admin, db, FOR_DISTRIBUTION_TOPIC_NAME, INSTRUCTIONS_TOPIC_NAME, pubsub} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {distributeDoc} from "../index-utils";
import {firestore} from "firebase-admin";
import FieldValue = firestore.FieldValue;
import {pubsubUtils} from "./pubsub";
import {reviveDateAndTimestamp} from "./misc";

export const queueForDistributionLater = async (...logicResultDocs: LogicResultDoc[]) => {
  const topic = pubsub.topic(FOR_DISTRIBUTION_TOPIC_NAME);

  try {
    for (const logicResultDoc of logicResultDocs) {
      const messageId = await topic.publishMessage({json: logicResultDoc});
      console.log(`Message ${messageId} published.`);
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

export async function queueInstructions(dstPath: string, instructions: { [p: string]: string }) {
  const topic = pubsub.topic(INSTRUCTIONS_TOPIC_NAME);

  try {
    const messageId = await topic.publishMessage({json: {dstPath, instructions}});
    console.log(`Message ${messageId} published.`);
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error(`Received error while publishing: ${error.message}`);
    } else {
      console.error("An unknown error occurred during publishing");
    }
    throw error;
  }
}

export async function onMessageInstructionsQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(INSTRUCTIONS_TOPIC_NAME, event.id)) {
    console.log("Skipping duplicate message");
    return;
  }
  try {
    const instructionsMessage: InstructionsMessage = event.data.message.json;
    console.log("Received user logic result doc:", instructionsMessage);

    console.info("Running Instructions");
    const {dstPath, instructions} = instructionsMessage;
    const updateData: {[key: string]: FieldValue} = {};

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
      } else if (instruction.startsWith("arr+") || instruction.startsWith("arr-")) {
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
        if (instruction.startsWith("arr+")) {
          updateData[property] = admin.firestore.FieldValue.arrayUnion(...params);
        } else {
          updateData[property] = admin.firestore.FieldValue.arrayRemove(...params);
        }
      } else {
        console.log(`Invalid instruction ${instruction} for property ${property}`);
      }
    }
    const dstDocRef = db.doc(dstPath);
    await dstDocRef.set(updateData, {merge: true});
    await pubsubUtils.trackProcessedIds(INSTRUCTIONS_TOPIC_NAME, event.id);
    return "Processed instructions";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}
