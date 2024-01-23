import {FormData} from "@primeanalytiq/emberflow-admin-client/lib/types";
import {db, pubsub, rtdb, SUBMIT_FORM_TOPIC_NAME} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {submitForm} from "@primeanalytiq/emberflow-admin-client/lib";
import {pubsubUtils} from "./pubsub";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import {deleteCollection} from "./misc";

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

  const submitFormAs = formData["@submitFormAs"];
  delete formData["@submitFormAs"];
  formData = await submitForm(formData, submitFormAs);
  const status = formData["@status"];
  const messages = formData["@messages"];
  console.debug("Form submission status:", status, messages);
  if (status === "cancelled" && messages.startsWith("cancel-then-retry")) {
    console.log("Throwing error so that the message is retried");
    throw new Error("cancel-then-retry");
  }

  await pubsubUtils.trackProcessedIds(SUBMIT_FORM_TOPIC_NAME, event.id);
  return "Processed form data";
}

export async function cleanActionsAndForms(event: ScheduledEvent) {
  console.info("Running cleanActionsAndForms");
  const query = db.collection("@actions")
    .where("timeCreated", "<", new Date(Date.now() - 1000 * 60 * 60 * 24 * 7));

  await deleteCollection(query, async (snapshot) => {
    const forms: {[key: string]: null} = {};
    snapshot.docs.forEach( (doc) => {
      const {eventContext: {formId, uid}} = doc.data();
      forms[`forms/${uid}/${formId}`] = null;
    });

    await rtdb.ref().update(forms);
  });

  console.info("Cleaned actions and forms");
}
