import {Instructions, InstructionsMessage, LogicResultDoc} from "../types";
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
import FieldValue = firestore.FieldValue;
import {pubsubUtils} from "./pubsub";
import {reviveDateAndTimestamp} from "./misc";

export const queueForDistributionLater = async (...logicResultDocs: LogicResultDoc[]) => {
  try {
    for (const logicResultDoc of logicResultDocs) {
      const {
        instructions,
        dstPath,
        ...rest
      } = logicResultDoc;
      const forDistributionMessageId = await FOR_DISTRIBUTION_TOPIC.publishMessage({json: {...rest, dstPath}});
      console.log(`Message ${forDistributionMessageId} published.`);
      if (instructions) {
        const instructionsReducerMessageId = await instructionsReducerTopic.publishMessage({json: {dstPath, instructions}});
        console.log(`Instructions ${instructionsReducerMessageId} published for reduction.`);
      }
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
export async function reduceInstructions() {
  const duration = 3000;
  const subscription = pubsub.subscription(INSTRUCTIONS_REDUCER_TOPIC_NAME);

  const runReduceInstructions = (): Promise<Map<string, Instructions>> => {
    const reducedInstructions: Map<string, Instructions> = new Map();
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        subscription.close();
        console.log("Time is up, stopping message reception");
        resolve(reducedInstructions);
      }, duration);

      subscription.on("message", (message) => {
        console.log(`Received message ${message.id}:`);
        const {dstPath, instructions} = JSON.parse(message.data.toString());
        const existingInstructions = reducedInstructions.get(dstPath);
        if (!existingInstructions) {
          reducedInstructions.set(dstPath, instructions);
          message.ack();
          return;
        }
        // TODO: Merge existing instructions with new instructions
        message.ack();
      });

      subscription.on("error", (error) => {
        clearTimeout(timeoutId);
        console.error(`Received error: ${error}`);
        reject(error);
      });
    });
  };

  const reduceAndQueue = async () => {
    try {
      const reducedInstructions = await runReduceInstructions();
      console.log(`Received ${reducedInstructions.size} messages within ${duration / 1000} seconds.`);
      // Process the reduced instructions here
      for (const [dstPath, instructions] of reducedInstructions.entries()) {
        await queueInstructions(dstPath, instructions);
      }
    } catch (error) {
      console.error(`Error while receiving messages: ${error}`);
    }
  };

  await reduceAndQueue();
}

