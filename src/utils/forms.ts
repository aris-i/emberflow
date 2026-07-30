import {FormData} from "emberflow-admin-client/lib/types";
import {SUBMIT_FORM_TOPIC, SUBMIT_FORM_TOPIC_NAME} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import type {MessagePublishedData} from "firebase-functions/v2/pubsub";
import {submitForm} from "emberflow-admin-client/lib";
import {pubsubUtils} from "./pubsub";

export async function queueSubmitForm(formData: FormData) {
  try {
    return await SUBMIT_FORM_TOPIC.publishMessage({json: formData});
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
    return;
  }
  let formData = event.data.message.json as FormData;

  const submitFormAs = formData["@submitFormAs"];
  const appVersion = formData["@appVersion"];
  const metadata = formData["@metadata"];
  delete formData["@submitFormAs"];
  delete formData["@appVersion"];
  formData = await submitForm(formData, {
    uid: submitFormAs,
    appVersion,
    metadata,
  });

  await pubsubUtils.trackProcessedIds(SUBMIT_FORM_TOPIC_NAME, event.id);
  return "Processed form data";
}
