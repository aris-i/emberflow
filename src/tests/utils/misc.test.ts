import {computeHashCode, convertBase64ToJSON, deepEqual, deleteActionCollection, deleteCollection, LimitedSet} from "../../utils/misc";
import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;
import GeoPoint = firestore.GeoPoint;
import Query = firestore.Query;
import {BatchUtil} from "../../utils/batch";
import {db, initializeEmberFlow} from "../../index";
import {ProjectConfig} from "../../types";
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfig} from "../../sample-custom/security";
import {validatorConfig} from "../../sample-custom/validators";

const projectConfig: ProjectConfig = {
  projectId: "your-project-id",
  region: "asia-southeast1",
  rtdbName: "your-rtdb-name",
  budgetAlertTopicName: "budget-alerts",
  maxCostLimitPerFunction: 100,
  specialCostLimitPerFunction: {
    function1: 50,
    function2: 75,
    function3: 120,
  },
};
admin.initializeApp({
  databaseURL: "https://test-project.firebaseio.com",
});
initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, []);

describe("deepEqual", () => {
  it("should correctly compare Firestore Field types", () => {
    const ts1 = Timestamp.fromDate(new Date(2023, 3, 1));
    const ts2 = Timestamp.fromDate(new Date(2023, 3, 1));
    const ts3 = Timestamp.fromDate(new Date(2023, 3, 2));

    const geo1 = new GeoPoint(37.422, -122.084);
    const geo2 = new GeoPoint(37.422, -122.084);
    const geo3 = new GeoPoint(37.426, -122.081);

    const array1 = [ts1, geo1];
    const array2 = [ts2, geo2];
    const array3 = [ts1, geo3];

    const obj1 = {timestamp: ts1, geoPoint: geo1};
    const obj2 = {timestamp: ts2, geoPoint: geo2};
    const obj3 = {timestamp: ts1, geoPoint: geo3};

    expect(deepEqual(ts1, ts2)).toBe(true);
    expect(deepEqual(ts1, ts3)).toBe(false);

    expect(deepEqual(geo1, geo2)).toBe(true);
    expect(deepEqual(geo1, geo3)).toBe(false);

    expect(deepEqual(array1, array2)).toBe(true);
    expect(deepEqual(array1, array3)).toBe(false);

    expect(deepEqual(obj1, obj2)).toBe(true);
    expect(deepEqual(obj1, obj3)).toBe(false);
  });
});

describe("computeHashCode", () => {
  it("should compute the hash code correctly for an empty string", () => {
    const input = "";
    const expectedOutput = "00000000";
    const result = computeHashCode(input);
    expect(result).toBe(expectedOutput);
    expect(result.length).toBe(8); // Assert the length of the output string
  });

  it("should compute the hash code correctly for a very long string", () => {
    // Generate a very long string
    const str = "a".repeat(1000000); // 1 million 'a' characters
    // Manually calculate the expected hash code using the same logic
    let hashCode = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hashCode = ((hashCode << 5) - hashCode + char) >>> 0; // Convert to unsigned 32-bit integer
    }
    const hexString = hashCode.toString(16);
    const expectedOutput = hexString.padStart(8, "0");
    const result = computeHashCode(str);
    expect(result).toBe(expectedOutput);
    expect(result.length).toBe(8); // Assert the length of the output string
  });
});

describe("LimitedSet", () => {
  let limitedSet: LimitedSet<any>;
  const collectionRef = db.collection("example-collection");
  const documentRef = collectionRef.doc("example-document");

  beforeEach(() => {
    limitedSet = new LimitedSet(10);
    jest.clearAllMocks();
  });

  describe("add", () => {
    it("should add a document to the set", () => {
      limitedSet.add(documentRef);
      expect(limitedSet.set.size).toBe(1);
      expect(limitedSet.queue.length).toBe(1);
      expect(limitedSet.has(documentRef)).toBe(true);
    });

    it("should remove oldest item when max length is reached", () => {
      limitedSet.add(documentRef);
      for (let i = 0; i < 10; i++) {
        limitedSet.add(collectionRef.doc(`document-${i}`));
      }
      expect(limitedSet.set.size).toBe(10);
      expect(limitedSet.queue.length).toBe(10);
      expect(limitedSet.has(documentRef)).toBe(false);
    });
  });

  describe("has", () => {
    it("should return true if the item is in the set", () => {
      limitedSet.add(documentRef);
      expect(limitedSet.has(documentRef)).toBe(true);
    });

    it("should return false if the item is not in the set", () => {
      expect(limitedSet.has(documentRef)).toBe(false);
    });
  });

  describe("delete", () => {
    it("should delete a document from the set", () => {
      limitedSet.add(documentRef);
      expect(limitedSet.set.size).toBe(1);
      expect(limitedSet.queue.length).toBe(1);
      expect(limitedSet.has(documentRef)).toBe(true);
      limitedSet.delete(documentRef);
      expect(limitedSet.set.size).toBe(0);
      expect(limitedSet.queue.length).toBe(0);
      expect(limitedSet.has(documentRef)).toBe(false);
    });
  });
});

describe("deleteCollection", () => {
  const batch = BatchUtil.getInstance();
  let batchDeleteSpy: jest.SpyInstance;
  let batchCommitSpy: jest.SpyInstance;
  let limitMock: jest.Mock;

  beforeEach(() => {
    jest.spyOn(BatchUtil, "getInstance").mockImplementation(() => batch);
    batchDeleteSpy = jest.spyOn(batch, "deleteDoc").mockResolvedValue(undefined);
    batchCommitSpy = jest.spyOn(batch, "commit").mockResolvedValue(undefined);
  });

  it("should return when snapshot size is 0", async () => {
    limitMock = jest.fn().mockReturnValue({
      get: jest.fn().mockResolvedValue({
        size: 0,
      }),
    });
    await deleteCollection({
      limit: limitMock,
    } as unknown as Query);

    expect(limitMock).toHaveBeenCalledTimes(1);
    expect(limitMock).toHaveBeenCalledWith(500);
    expect(batchDeleteSpy).not.toHaveBeenCalled();
    expect(batchCommitSpy).not.toHaveBeenCalled();
  });

  it("should delete all documents in a collection", async () => {
    const docs = [];
    for (let i = 0; i < 100; i++) {
      docs.push({ref: i});
    }
    const getMock = jest.fn().mockResolvedValue({
      size: 0,
      docs: [],
    }).mockResolvedValueOnce({
      size: 100,
      docs: docs,
    });
    limitMock = jest.fn().mockReturnValue({
      get: getMock,
    });
    await deleteCollection({
      limit: limitMock,
    } as unknown as Query);

    expect(limitMock).toHaveBeenCalledTimes(1);
    expect(limitMock).toHaveBeenCalledWith(500);
    expect(batchDeleteSpy).toHaveBeenCalledTimes(100);
    expect(batchCommitSpy).toHaveBeenCalled();
  });
});

describe("deleteActionCollection", () => {
  const batch = BatchUtil.getInstance();
  let batchDeleteSpy: jest.SpyInstance;
  let batchCommitSpy: jest.SpyInstance;
  let formRefSpy: jest.SpyInstance;
  let formRemoveMock: jest.Mock;
  let limitMock: jest.Mock;

  beforeEach(() => {
    jest.spyOn(BatchUtil, "getInstance").mockImplementation(() => batch);
    batchDeleteSpy = jest.spyOn(batch, "deleteDoc").mockResolvedValue(undefined);
    batchCommitSpy = jest.spyOn(batch, "commit").mockResolvedValue(undefined);
    formRemoveMock = jest.fn();
    formRefSpy = jest.spyOn(admin.database(), "ref").mockReturnValue({
      remove: formRemoveMock,
    } as unknown as admin.database.Reference);
  });

  it("should return when snapshot size is 0", async () => {
    limitMock = jest.fn().mockReturnValue({
      get: jest.fn().mockResolvedValue({
        size: 0,
      }),
    });
    await deleteActionCollection({
      limit: limitMock,
    } as unknown as Query);

    expect(limitMock).toHaveBeenCalledTimes(1);
    expect(limitMock).toHaveBeenCalledWith(500);
    expect(formRefSpy).not.toHaveBeenCalled();
    expect(formRemoveMock).not.toHaveBeenCalled();
    expect(batchDeleteSpy).not.toHaveBeenCalled();
    expect(batchCommitSpy).not.toHaveBeenCalled();
  });

  it("should delete all documents in a collection", async () => {
    const docs = [];
    for (let i = 0; i < 100; i++) {
      docs.push({
        ref: i,
        data: () => ({
          eventContext: {
            formId: i,
            uid: "test-user-id",
          },
        }),
      });
    }
    const getMock = jest.fn().mockResolvedValue({
      size: 0,
      docs: [],
    }).mockResolvedValueOnce({
      size: 100,
      docs: docs,
    });
    limitMock = jest.fn().mockReturnValue({
      get: getMock,
    });
    await deleteActionCollection({
      limit: limitMock,
    } as unknown as Query);

    expect(limitMock).toHaveBeenCalledTimes(1);
    expect(limitMock).toHaveBeenCalledWith(500);
    expect(formRefSpy).toHaveBeenCalledTimes(100);
    expect(formRemoveMock).toHaveBeenCalledTimes(100);
    expect(batchDeleteSpy).toHaveBeenCalledTimes(100);
    expect(batchCommitSpy).toHaveBeenCalled();
  });
});

describe("convertBase64ToJSON", () => {
  it("should convert a base64 string to JSON with every data type intact", () => {
    const data = {
      "@allowedUsers": [
        "user1",
        "user2",
      ],
      "title": "Sample Title",
      "createdAt": new Date(),
      "createdBy": {
        "@id": "user1",
        "name": "User 1",
        "date": new Date(),
      },
      "private": false,
      "completedTodos": 0,
    };
    const json = JSON.stringify(data);
    const base64 = Buffer.from(json).toString("base64");

    const result = convertBase64ToJSON(base64);
    expect(result).toEqual(data);
  });
});
