import {db, pubSubTopics} from "../index";
import {deleteCollection} from "./misc";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";

async function trackProcessedIds(topicName: string, id: string) {
  const docRef = db.doc(`@topics/${topicName}/processedIds/${id}`);
  await docRef.set({timestamp: new Date()});
}

async function isProcessed(topicName: string, id: string) {
  const docRef = db.doc(`@topics/${topicName}/processedIds/${id}`);
  const doc = await docRef.get();
  return doc.exists;
}

export const pubsubUtils = {
  trackProcessedIds,
  isProcessed,
};

export async function cleanPubSubProcessedIds(event: ScheduledEvent) {
  console.info("Running cleanPubSubProcessedIds");
  let i = 0;
  for (const pubSubTopic of pubSubTopics) {
    const query = db
      .collection(`@topics/${pubSubTopic}/processedIds`)
      .where("timestamp", "<", new Date(Date.now() - 1000 * 60 * 60 * 24 * 7));

    await deleteCollection(query, (snapshot) => {
      i += snapshot.size;
    });
  }
  console.info(`Cleaned ${i} topics of processedIds`);
}
