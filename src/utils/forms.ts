import {FormData} from "emberflow-admin-client/lib/types";
import {projectConfig, pubsub} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {submitForm} from "emberflow-admin-client/lib";

export async function queueSubmitForm(formData: FormData) {
  const topic = pubsub.topic(projectConfig.submitFormQueueTopicName);

  try {
    const messageId = await topic.publishMessage({json: formData});
    console.log(`Message ${messageId} published.`);
    return messageId;
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error(`Received error while publishing: ${error.message}`);
    } else {
      console.error("An unknown error occurred during publishing");
    }
    throw error;
  }
}

export async function onMessageSubmitFormQueue(event: CloudEvent<MessagePublishedData>) {
  try {
    const formData = event.data.message.json;
    console.log("Received form submission:", formData);

    // TODO:  Let's make status handler optional
    await submitForm(formData);

    return "Processed form data";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}
