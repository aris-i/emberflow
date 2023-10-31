import {DocumentData, DocumentReference} from "firebase-admin/lib/firestore";
import {db} from "../index";
import {firestore} from "firebase-admin";
import WriteBatch = firestore.WriteBatch;
import {sleep} from "./misc";

export const BATCH_SIZE = 490;
export let _oldBatch: WriteBatch | undefined;
export let _batch: WriteBatch | undefined;
export let writeCount = 0;

async function getBatch() {
  if (!_batch) {
    do {
      _batch = db.batch();
      await sleep(100);
    } while (_batch === _oldBatch);
  }
  return _batch;
}

export async function commit() {
  await (await getBatch()).commit();
  _oldBatch = _batch;
  _batch = undefined;
  writeCount = 0;
}

export async function set<T extends DocumentData>(
  docRef: DocumentReference<T>,
  document: T,
): Promise<void> {
  (await getBatch()).set(docRef, document, {merge: true});
  writeCount++;

  if (writeCount >= BATCH_SIZE) {
    await commit();
  }
}


export async function deleteDoc<T extends DocumentData>(
  docRef: DocumentReference<T>,
): Promise<void> {
  (await getBatch()).delete(docRef);
  writeCount++;

  if (writeCount >= BATCH_SIZE) {
    await commit();
  }
}
