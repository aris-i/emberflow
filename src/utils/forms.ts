import {FormData} from "emberflow-admin-client/lib/types";
import {pubsub, rtdb, SUBMIT_FORM_TOPIC_NAME} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {submitForm} from "emberflow-admin-client/lib";
import {pubsubUtils} from "./pubsub";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";

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
  try {
    const formData = event.data.message.json;
    console.log("Received form submission:", formData);

    // TODO:  Let's make status handler optional
    await submitForm(formData);

    await pubsubUtils.trackProcessedIds(SUBMIT_FORM_TOPIC_NAME, event.id);
    return "Processed form data";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}

export const cleanForms = async (event: ScheduledEvent) => {
  console.info("Running cleanForms");
  const formsRef = rtdb.ref("forms");
  const sevenDaysAgo = new Date(Date.now() - 1000 * 60 * 60 * 24 * 7);

  let i = 0;
  await formsRef.orderByChild("submittedAt").endAt(sevenDaysAgo.getTime())
    .once("value", (snapshot) => {
      snapshot.forEach((form) => {
        form.ref.remove();
        i++;
      });
    });

  console.info(`Cleaned ${i} forms`);
};
