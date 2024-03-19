import {DocumentData, DocumentReference} from "firebase-admin/lib/firestore";
import {db} from "../index";
import {firestore} from "firebase-admin";
import WriteBatch = firestore.WriteBatch;
import UpdateData = firestore.UpdateData;


export class BatchUtil {
  BATCH_SIZE: number;
  writeCount: number;
  _batch: WriteBatch | undefined;

  private constructor() {
    this.BATCH_SIZE = 490;
    this.writeCount = 0;
  }

  public static getInstance(): BatchUtil {
    return new BatchUtil();
  }

  async getBatch() {
    if (!this._batch) {
      this._batch = db.batch();
    }
    return this._batch;
  }

  async commit() {
    await (await this.getBatch()).commit();
    this._batch = undefined;
    this.writeCount = 0;
  }

  async set<T extends DocumentData>(
    docRef: DocumentReference<T>,
    document: T,
  ): Promise<void> {
    (await this.getBatch()).set(docRef, document, {merge: true});
    this.writeCount++;

    if (this.writeCount >= this.BATCH_SIZE) {
      await this.commit();
    }
  }

  async update<T extends DocumentData>(
    docRef: DocumentReference<T>,
    document: UpdateData<T>,
  ): Promise<void> {
    (await this.getBatch()).update(docRef, document);
    this.writeCount++;

    if (this.writeCount >= this.BATCH_SIZE) {
      await this.commit();
    }
  }

  async deleteDoc<T extends DocumentData>(
    docRef: DocumentReference<T>,
  ): Promise<void> {
    (await this.getBatch()).delete(docRef);
    this.writeCount++;

    if (this.writeCount >= this.BATCH_SIZE) {
      await this.commit();
    }
  }
}
