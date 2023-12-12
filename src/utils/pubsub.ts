import {db} from "../index";
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
  const topicsSnapshot = await db.collection("@topics").get();
  let i = 0;
  for (const topicDoc of topicsSnapshot.docs) {
    const query = topicDoc.ref.collection("processedIds")
      .where("timestamp", "<", new Date(Date.now() - 1000 * 60 * 60 * 24 * 7));
    await deleteCollection(query);
    i++;
  }
  console.info(`Cleaned ${i} topics of processedIds`);
}
