import {db} from "../index";

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
