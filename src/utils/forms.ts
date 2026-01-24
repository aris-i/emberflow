import {FormData} from "emberflow-admin-client/lib/types";
import {db, rtdb, SUBMIT_FORM_TOPIC, SUBMIT_FORM_TOPIC_NAME} from "../index";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {submitForm} from "emberflow-admin-client/lib";
import {pubsubUtils} from "./pubsub";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import {deleteCollection} from "./misc";

export async function queueSubmitForm(formData: FormData) {
  try {
    const messageId = await SUBMIT_FORM_TOPIC.publishMessage({json: formData});
    console.log(`queueSubmitForm: Message ${messageId} published.`);
    console.debug(`queueSubmitForm: ${formData}`);
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
  let formData = event.data.message.json as FormData;
  console.log("Received form submission:", formData);

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
  const status = formData["@status"];
  const messages = formData["@messages"];
  console.debug("Form submission status:", status, messages);

  await pubsubUtils.trackProcessedIds(SUBMIT_FORM_TOPIC_NAME, event.id);
  return "Processed form data";
}

export async function cleanActionsAndForms(event: ScheduledEvent) {
  console.info("Running cleanActionsAndForms");
  const query = db.collection("@actions")
    .where("timeCreated", "<", new Date(Date.now() - 1000 * 60 * 60 * 24 * 7));

  await deleteCollection(query, async (snapshot) => {
    const forms: {[key: string]: null} = {};
    for (const doc of snapshot.docs) {
      const logicResultsQuery = db.collection(`${doc.ref.path}/logicResults`);

      await deleteCollection(logicResultsQuery, async (logicResultSnapshot) => {
        for (const logicResultDoc of logicResultSnapshot.docs) {
          const documentsQuery = db.collection(`${logicResultDoc.ref.path}/documents`);

          await deleteCollection(documentsQuery);
        }
      });

      const data = doc.data();
      if (data.eventContext) {
        const {formId, uid} = data.eventContext;
        forms[`forms/${uid}/${formId}`] = null;
      }
    }

    const rtdbBatchSize = 100;
    const formKeys = Object.keys(forms);
    for (let i = 0; i < formKeys.length; i += rtdbBatchSize) {
      const batch: {[key: string]: null} = {};
      formKeys.slice(i, i + rtdbBatchSize).forEach((key) => {
        batch[key] = forms[key];
      });
      await rtdb.ref().update(batch);
    }
  });

  console.info("Cleaned actions and forms");
}
