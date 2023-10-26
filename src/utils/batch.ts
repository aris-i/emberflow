import {DocumentData, DocumentReference} from "firebase-admin/lib/firestore";
import {db} from "../index";
import {firestore} from "firebase-admin";
import WriteBatch = firestore.WriteBatch;

export const BATCH_SIZE = 490;
export let _batch: WriteBatch | undefined;
export let writeCount = 0;

function getBatch() {
  if (!_batch) {
    _batch = db.batch();
  }
  return _batch;
}

export async function commit() {
  await getBatch().commit();
  _batch = undefined;
  writeCount = 0;
}

export async function set<T extends DocumentData>(
  docRef: DocumentReference<T>,
  document: T,
): Promise<void> {
  getBatch().set(docRef, document, {merge: true});
  writeCount++;

  if (writeCount >= BATCH_SIZE) {
    await commit();
  }
}


export async function deleteDoc<T extends DocumentData>(
  docRef: DocumentReference<T>,
): Promise<void> {
  getBatch().delete(docRef);
  writeCount++;

  if (writeCount >= BATCH_SIZE) {
    await commit();
  }
}
