import {LogicResultDoc} from "../types";
import {FOR_DISTRIBUTION_TOPIC_NAME, pubsub} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {distributeDoc} from "../index-utils";

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
  try {
    const logicResultDoc: LogicResultDoc = event.data.message.json;
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

    return "Processed for distribution later";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}
