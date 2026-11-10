import * as admin from "firebase-admin";
import {FieldValue, Timestamp} from "firebase-admin/firestore";
import {
  convertInstructionsToDbValues,
  sentinelToInstruction,
} from "../../utils/distribution";
import * as emberflow from "../../index";
import {warnIfSplitFirestoreAdmin} from "../../utils/firestore-admin";

// firebase-tools 12.9.1–15.35.0 Proxied.getOriginal binds admin.firestore and
// drops FieldValue/Timestamp/FieldPath. Modular imports must not use that path.
jest.mock("firebase-admin", () => {
  const actual = jest.requireActual("firebase-admin");
  const firestore = actual.firestore.bind(actual);
  return new Proxy(actual, {
    get(target, prop, receiver) {
      if (prop === "__esModule") {
        return true;
      }
      if (prop === "firestore") {
        return firestore;
      }
      return Reflect.get(target, prop, receiver);
    },
  });
});

describe("distribution under bound admin.firestore", () => {
  beforeAll(() => {
    (emberflow as unknown as {db: {doc: jest.Mock}}).db = {
      doc: jest.fn((path: string) => ({path})),
    };
  });

  it("does not see FieldValue, Timestamp, or FieldPath on admin.firestore", () => {
    expect(admin.firestore.FieldValue).toBeUndefined();
    expect(admin.firestore.Timestamp).toBeUndefined();
    expect(admin.firestore.FieldPath).toBeUndefined();
  });

  it("recognizes modular FieldValue sentinels via instanceof", () => {
    expect(sentinelToInstruction(FieldValue.increment(1))).toBe("++");
    expect(sentinelToInstruction(FieldValue.increment(-1))).toBe("--");
    expect(sentinelToInstruction(FieldValue.increment(5))).toBe("+5");
    expect(sentinelToInstruction(FieldValue.increment(-3))).toBe("-3");
    expect(sentinelToInstruction(FieldValue.arrayUnion("Earth", "Mars"))).toBe("arr(+Earth,+Mars)");
    expect(sentinelToInstruction(FieldValue.arrayRemove("Pluto"))).toBe("arr(-Pluto)");
    expect(sentinelToInstruction(FieldValue.delete())).toBe("del");
    expect(sentinelToInstruction(FieldValue.serverTimestamp())).toBe("serverTimestamp");
    expect(sentinelToInstruction({not: "a sentinel"})).toBeUndefined();
  });

  it("converts instructions with modular FieldValue and Timestamp.now", async () => {
    const txn = {
      get: jest.fn().mockResolvedValue({data: () => undefined}),
      set: jest.fn(),
      update: jest.fn(),
    };
    const {updateData, removeData} = await convertInstructionsToDbValues(txn as never, {
      count: "++",
      losses: "--",
      score: "+5",
      penalty: "-3",
      tags: "arr(+Earth,+Mars)",
      oldTags: "arr(-Pluto)",
      obsolete: "del",
      updatedAt: "serverTimestamp",
      queueNumber: "globalCounter(queueNumber)",
    });

    expect(updateData).toEqual({
      count: FieldValue.increment(1),
      losses: FieldValue.increment(-1),
      score: FieldValue.increment(5),
      penalty: FieldValue.increment(-3),
      tags: FieldValue.arrayUnion("Earth", "Mars"),
      obsolete: FieldValue.delete(),
      updatedAt: FieldValue.serverTimestamp(),
      queueNumber: 1,
    });
    expect(removeData).toEqual({
      oldTags: FieldValue.arrayRemove("Pluto"),
    });
    for (const value of Object.values(updateData)) {
      if (typeof value === "object" && value !== null) {
        expect(value).toBeInstanceOf(FieldValue);
      }
    }
    expect(removeData.oldTags).toBeInstanceOf(FieldValue);

    expect(txn.set).toHaveBeenCalledTimes(1);
    const counterDoc = txn.set.mock.calls[0][1] as {lastUpdatedAt: Timestamp};
    expect(counterDoc.lastUpdatedAt).toBeInstanceOf(Timestamp);
    expect(emberflow.db.doc).toHaveBeenCalledWith("@counters/queueNumber");
  });
});

describe("warnIfSplitFirestoreAdmin", () => {
  it("warns once when admin.firestore.FieldValue is a different copy", () => {
    const warn = jest.spyOn(console, "warn").mockImplementation(() => undefined);
    warnIfSplitFirestoreAdmin({firestore: {}});
    warnIfSplitFirestoreAdmin({firestore: {FieldValue}});
    expect(warn).not.toHaveBeenCalled();

    const other = function DuplicateFieldValue() {
      return undefined;
    };
    warnIfSplitFirestoreAdmin({firestore: {FieldValue: other}});
    warnIfSplitFirestoreAdmin({firestore: {FieldValue: other}});
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0][0])).toContain("Hoist a single firebase-admin");
    warn.mockRestore();
  });
});
