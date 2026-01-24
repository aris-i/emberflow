import {DocumentData, DocumentReference} from "firebase-admin/lib/firestore";
import {db} from "../index";
import {firestore} from "firebase-admin";
import WriteBatch = firestore.WriteBatch;
import UpdateData = firestore.UpdateData;
import SetOptions = firestore.SetOptions;


export class BatchUtil {
  BATCH_SIZE: number;
  writeCount: number;
  _batch: WriteBatch | undefined;

  private committing: Promise<void> | null = null;

  private constructor() {
    this.BATCH_SIZE = 100;
    this.writeCount = 0;
  }

  public static getInstance(): BatchUtil {
    return new BatchUtil();
  }

  async getBatch() {
    if (this.committing) {
      await this.committing;
    }
    if (!this._batch) {
      this._batch = db.batch();
    }
    return this._batch;
  }

  async commit() {
    if (this.committing) {
      return this.committing;
    }
    if (!this._batch || this.writeCount === 0) {
      return;
    }
    const batchToCommit = this._batch;
    this._batch = undefined;
    this.writeCount = 0;
    this.committing = batchToCommit.commit().then(() => {
      this.committing = null;
    });
    return this.committing;
  }

  async set<T extends DocumentData>(
    docRef: DocumentReference<T>,
    document: T,
    options?: SetOptions
  ): Promise<void> {
    const batch = await this.getBatch();
    if (options) {
      batch.set(docRef, document, options);
    } else {
      batch.set(docRef, document);
    }
    this.writeCount++;

    if (this.writeCount >= this.BATCH_SIZE) {
      await this.commit();
    }
  }

  async update<T extends DocumentData>(
    docRef: DocumentReference<T>,
    document: UpdateData<T>,
  ): Promise<void> {
    const batch = await this.getBatch();
    batch.update(docRef, document);
    this.writeCount++;

    if (this.writeCount >= this.BATCH_SIZE) {
      await this.commit();
    }
  }

  async deleteDoc<T extends DocumentData>(
    docRef: DocumentReference<T>,
  ): Promise<void> {
    const batch = await this.getBatch();
    batch.delete(docRef);
    this.writeCount++;

    if (this.writeCount >= this.BATCH_SIZE) {
      await this.commit();
    }
  }
}
