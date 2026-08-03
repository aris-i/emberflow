import {DocumentData, DocumentReference} from "firebase-admin/lib/firestore";
import {db} from "../index";
import {firestore} from "firebase-admin";
import WriteBatch = firestore.WriteBatch;
import UpdateData = firestore.UpdateData;
import SetOptions = firestore.SetOptions;

/**
 * A thin wrapper around Firestore's {@link WriteBatch} that buffers writes and
 * auto-commits once {@link BatchUtil.BATCH_SIZE} operations have accumulated.
 *
 * All mutating operations (`set`, `update`, `deleteDoc`, `commit`) are
 * serialized internally through a promise queue, so the instance is safe to use
 * concurrently (e.g. via `Promise.all([...])`) without corrupting the buffered
 * batch or its write count.
 *
 * Note: because auto-commit only triggers on multiples of `BATCH_SIZE`, callers
 * must still flush any remaining writes with {@link BatchUtil.commit} (or the
 * {@link BatchUtil.flush} alias) when they are done.
 */
export class BatchUtil {
  // Firestore allows up to 500 operations per batch; the default of 100 keeps a
  // safe margin. Configurable per instance via `create()` (e.g. for testing).
  readonly BATCH_SIZE: number;

  private _writeCount = 0;
  private _batch: WriteBatch | undefined;

  // Tracks an in-flight commit so its rejection can be surfaced and its state
  // reliably reset (see `commitNow`).
  private committing: Promise<void> | null = null;

  // Tail of the serialization queue. Every public operation chains onto this so
  // that at most one operation touches the buffered batch at a time.
  private queue: Promise<void> = Promise.resolve();

  private constructor(batchSize: number) {
    this.BATCH_SIZE = batchSize;
  }

  /**
   * Creates a new {@link BatchUtil}. This is a factory, not a singleton: every
   * call returns a fresh, independent instance.
   * @param {number} batchSize maximum number of buffered writes before an
   * automatic commit is triggered. Defaults to 100.
   * @return {BatchUtil} a new instance.
   */
  public static create(batchSize = 100): BatchUtil {
    return new BatchUtil(batchSize);
  }

  /**
   * Number of writes buffered in the current, not-yet-committed batch.
   * @return {number} the pending write count.
   */
  get pendingWrites(): number {
    return this._writeCount;
  }

  // Serializes operations by chaining them onto `queue`. The returned promise
  // settles with the task's own result/error, while the queue itself never
  // rejects so a single failed operation does not break later ones.
  private enqueue<T>(task: () => Promise<T>): Promise<T> {
    const run = this.queue.then(task, task);
    this.queue = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async getBatch(): Promise<WriteBatch> {
    if (this.committing) {
      await this.committing;
    }
    if (!this._batch) {
      this._batch = db.batch();
    }
    return this._batch;
  }

  /**
   * Commits any buffered writes. Safe to call when nothing is pending (no-op).
   * @return {Promise<void>} resolves once the commit completes.
   */
  commit(): Promise<void> {
    return this.enqueue(() => this.commitNow());
  }

  /**
   * Alias for {@link BatchUtil.commit}. Provided so call sites can express the
   * intent of flushing the trailing, sub-`BATCH_SIZE` writes.
   * @return {Promise<void>} resolves once the flush completes.
   */
  flush(): Promise<void> {
    return this.commit();
  }

  private async commitNow(): Promise<void> {
    if (this.committing) {
      return this.committing;
    }
    if (!this._batch || this._writeCount === 0) {
      return;
    }
    const batchToCommit = this._batch;
    this._batch = undefined;
    this._writeCount = 0;
    // Reset `committing` even if the commit rejects, so a single failed commit
    // never leaves the instance stuck on a permanently-rejected promise.
    const commitPromise = (async () => {
      try {
        await batchToCommit.commit();
      } finally {
        this.committing = null;
      }
    })();
    this.committing = commitPromise;
    return commitPromise;
  }

  async set<T extends DocumentData>(
    docRef: DocumentReference<T>,
    document: T,
    options?: SetOptions
  ): Promise<void> {
    return this.enqueue(async () => {
      const batch = await this.getBatch();
      if (options) {
        batch.set(docRef, document, options);
      } else {
        batch.set(docRef, document);
      }
      this._writeCount++;

      if (this._writeCount >= this.BATCH_SIZE) {
        await this.commitNow();
      }
    });
  }

  async update<T extends DocumentData>(
    docRef: DocumentReference<T>,
    document: UpdateData<T>,
  ): Promise<void> {
    return this.enqueue(async () => {
      const batch = await this.getBatch();
      batch.update(docRef, document);
      this._writeCount++;

      if (this._writeCount >= this.BATCH_SIZE) {
        await this.commitNow();
      }
    });
  }

  // Named `deleteDoc` rather than `delete` because `delete` is a reserved word.
  async deleteDoc<T extends DocumentData>(
    docRef: DocumentReference<T>,
  ): Promise<void> {
    return this.enqueue(async () => {
      const batch = await this.getBatch();
      batch.delete(docRef);
      this._writeCount++;

      if (this._writeCount >= this.BATCH_SIZE) {
        await this.commitNow();
      }
    });
  }
}
