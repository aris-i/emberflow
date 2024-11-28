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
  const batch = BatchUtil.getInstance();
  const collectionRef = db.collection("example-collection");
  const documentRef = collectionRef.doc("example-document");
  const documentData = {name: "John Doe", age: 30};

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("commit", () => {
    it("should commit the current batch", async () => {
      await batch.set(documentRef, documentData);
      await batch.commit();

      expect(db.batch().commit).toHaveBeenCalled();
      expect(batch.writeCount).toBe(0);
      expect(batch._batch).toBeUndefined();
    });
  });

  describe("set", () => {
    it("should add a set operation to the batch", async () => {
      try {
        expect(batch._batch).toBeUndefined();
        expect(batch.writeCount).toBe(0);
        await batch.set(documentRef, documentData);

        expect(db.batch().set).toHaveBeenCalledWith(documentRef, documentData);
        expect(batch._batch).not.toBe(1);
        expect(batch.writeCount).toBe(1);
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
        expect(batch.writeCount).toBe(writeCount);
        expect(batch._batch).toBeDefined();

        await batch.set(documentRef, documentData);

        expect(db.batch().set).toHaveBeenCalledTimes(writeCount + 1);
        expect(db.batch().commit).toHaveBeenCalled();
        expect(batch.writeCount).toBe(0);
        expect(batch._batch).toBeUndefined();
      } finally {
        await batch.commit();
      }
    });
  });

  describe("delete", () => {
    it("should add a delete operation to the batch", async () => {
      try {
        expect(batch.writeCount).toBe(0);
        expect(batch._batch).not.toBeDefined();
        await batch.deleteDoc(documentRef);

        expect(db.batch().delete).toHaveBeenCalled();
        expect(batch.writeCount).toBe(1);
        expect(batch._batch).toBeDefined();
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
        expect(batch.writeCount).toBe(writeCount);
        expect(batch._batch).toBeDefined();

        await batch.deleteDoc(documentRef);

        expect(db.batch().delete).toHaveBeenCalledTimes(writeCount + 1);
        expect(db.batch().commit).toHaveBeenCalled();
        expect(batch.writeCount).toBe(0);
        expect(batch._batch).not.toBeDefined();
      } finally {
        await batch.commit();
      }
    });
  });
});
