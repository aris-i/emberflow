import {db} from "../../index";
import {BatchUtil} from "../../utils/batch";

// Mock Firestore and necessary functions
const setMock = jest.fn();
const updateMock = jest.fn();
const commitMock = jest.fn();
const deleteMock = jest.fn();
jest.mock("../../index", () => ({
  db: {
    batch: jest.fn().mockImplementation(() =>{
      return {
        set: setMock,
        update: updateMock,
        commit: commitMock.mockResolvedValue(undefined),
        delete: deleteMock,
      };
    }),
    collection: jest.fn().mockReturnThis(),
    doc: jest.fn().mockReturnThis(),
  },
}));

describe("Batch", () => {
  const batch = BatchUtil.create();
  const collectionRef = db.collection("example-collection");
  const documentRef = collectionRef.doc("example-document");
  const documentData = {name: "John Doe", age: 30};

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("create", () => {
    it("should return a fresh instance on every call (factory, not singleton)", () => {
      expect(BatchUtil.create()).not.toBe(BatchUtil.create());
    });

    it("should default BATCH_SIZE to 100", () => {
      expect(BatchUtil.create().BATCH_SIZE).toBe(100);
    });

    it("should honor a custom batch size", () => {
      expect(BatchUtil.create(5).BATCH_SIZE).toBe(5);
    });
  });

  describe("commit", () => {
    it("should commit the current batch", async () => {
      await batch.set(documentRef, documentData);
      await batch.commit();

      expect(db.batch().commit).toHaveBeenCalled();
      expect(batch.pendingWrites).toBe(0);
    });

    it("should be a no-op when there is nothing to flush", async () => {
      const emptyBatch = BatchUtil.create();
      await emptyBatch.commit();

      expect(commitMock).not.toHaveBeenCalled();
      expect(emptyBatch.pendingWrites).toBe(0);
    });

    it("should stay usable after an underlying commit failure", async () => {
      const resilientBatch = BatchUtil.create();
      commitMock.mockRejectedValueOnce(new Error("commit failed"));

      await resilientBatch.set(documentRef, documentData);
      await expect(resilientBatch.commit()).rejects.toThrow("commit failed");

      // The instance must not be bricked by a single failed commit.
      await resilientBatch.set(documentRef, documentData);
      await expect(resilientBatch.commit()).resolves.toBeUndefined();
      expect(resilientBatch.pendingWrites).toBe(0);
    });
  });

  describe("flush", () => {
    it("should commit any pending writes", async () => {
      const flushableBatch = BatchUtil.create();
      await flushableBatch.set(documentRef, documentData);
      await flushableBatch.flush();

      expect(commitMock).toHaveBeenCalledTimes(1);
      expect(flushableBatch.pendingWrites).toBe(0);
    });
  });

  describe("set", () => {
    it("should add a set operation to the batch", async () => {
      try {
        expect(batch.pendingWrites).toBe(0);
        await batch.set(documentRef, documentData);

        expect(db.batch().set).toHaveBeenCalledWith(documentRef, documentData);
        expect(batch.pendingWrites).toBe(1);
      } finally {
        await batch.commit();
      }
    });

    it("should commit the batch if write count reaches the batch size", async () => {
      try {
        const writeCount = batch.BATCH_SIZE - 1; // Reach one less than the batch size

        for (let i = 0; i < writeCount; i++) {
          await batch.set(documentRef, documentData);
        }

        expect(db.batch().set).toHaveBeenCalledTimes(writeCount);
        expect(db.batch().commit).not.toHaveBeenCalled();
        expect(batch.pendingWrites).toBe(writeCount);

        await batch.set(documentRef, documentData);

        expect(db.batch().set).toHaveBeenCalledTimes(writeCount + 1);
        expect(db.batch().commit).toHaveBeenCalled();
        expect(batch.pendingWrites).toBe(0);
      } finally {
        await batch.commit();
      }
    });
  });

  describe("delete", () => {
    it("should add a delete operation to the batch", async () => {
      try {
        expect(batch.pendingWrites).toBe(0);
        await batch.deleteDoc(documentRef);

        expect(db.batch().delete).toHaveBeenCalled();
        expect(batch.pendingWrites).toBe(1);
      } finally {
        await batch.commit();
      }
    });

    it("should commit the batch if write count reaches the batch size", async () => {
      try {
        const writeCount = batch.BATCH_SIZE - 1; // Reach one less than the batch size

        for (let i = 0; i < writeCount; i++) {
          await batch.deleteDoc(documentRef);
        }

        expect(db.batch().delete).toHaveBeenCalledTimes(writeCount);
        expect(db.batch().commit).not.toHaveBeenCalled();
        expect(batch.pendingWrites).toBe(writeCount);

        await batch.deleteDoc(documentRef);

        expect(db.batch().delete).toHaveBeenCalledTimes(writeCount + 1);
        expect(db.batch().commit).toHaveBeenCalled();
        expect(batch.pendingWrites).toBe(0);
      } finally {
        await batch.commit();
      }
    });
  });

  describe("concurrency", () => {
    it("should serialize concurrent writes without corrupting the batch", async () => {
      const concurrentBatch = BatchUtil.create(3);

      await Promise.all([
        concurrentBatch.set(documentRef, documentData),
        concurrentBatch.set(documentRef, documentData),
        concurrentBatch.set(documentRef, documentData),
        concurrentBatch.set(documentRef, documentData),
        concurrentBatch.set(documentRef, documentData),
      ]);

      // All five writes are applied, and the auto-commit fires exactly once
      // (at the third write); the remaining two stay buffered.
      expect(setMock).toHaveBeenCalledTimes(5);
      expect(commitMock).toHaveBeenCalledTimes(1);
      expect(concurrentBatch.pendingWrites).toBe(2);

      await concurrentBatch.commit();
      expect(commitMock).toHaveBeenCalledTimes(2);
      expect(concurrentBatch.pendingWrites).toBe(0);
    });
  });
});
