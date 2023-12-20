import {FormData} from "emberflow-admin-client/lib/types";
import {pubsub, SUBMIT_FORM_TOPIC_NAME} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {submitForm} from "emberflow-admin-client/lib";
import {pubsubUtils} from "./pubsub";

export async function queueSubmitForm(formData: FormData) {
  const topic = pubsub.topic(SUBMIT_FORM_TOPIC_NAME);

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
  if (await pubsubUtils.isProcessed(SUBMIT_FORM_TOPIC_NAME, event.id)) {
    console.log("Skipping duplicate event");
    return;
  }
  let formData = event.data.message.json;
  console.log("Received form submission:", formData);

  formData = await submitForm(formData);
  // TODO: Test this
  const status = formData["@status"];
  const message = formData["@message"];
  if (status === "cancelled" && message.startsWith("cancel-then-retry")) {
    console.log("Throwing error so that the message is retried");
    throw new Error("cancel-then-retry");
  }

  await pubsubUtils.trackProcessedIds(SUBMIT_FORM_TOPIC_NAME, event.id);
  return "Processed form data";
}
