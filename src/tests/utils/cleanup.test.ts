import {ProjectConfig, CleanupConfig} from "../../types";
import * as admin from "firebase-admin";
import {initializeEmberFlow} from "../../index";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import {patchLogicConfigs} from "../../sample-custom/patch-logics";
import * as misc from "../../utils/misc";
import type {ScheduledEvent} from "firebase-functions/v2/scheduler";
import {
  cleanupCollections,
  computeCutoffDate,
  getInternalCleanupConfigs,
} from "../../utils/cleanup";

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

const projectCleanupConfigs: CleanupConfig[] = [
  {
    collectionPath: "askJaris",
    isCollectionGroup: true,
    timestampField: "createdAt",
    olderThan: {value: 1, unit: "months"},
    conditions: [
      {fieldName: "hasTopic", operator: "==", value: false},
    ],
  },
];

function initWithCleanupConfigs(cleanupConfigs: CleanupConfig[]) {
  initializeEmberFlow({
    projectConfig,
    admin,
    dbStructure,
    Entity,
    securityConfigs,
    validatorConfigs,
    logicConfigs: [],
    patchLogicConfigs,
    cleanupConfigs,
  });
}

describe("computeCutoffDate", () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("subtracts hours via millisecond math", () => {
    jest.setSystemTime(new Date("2023-06-15T12:00:00.000Z"));
    const result = computeCutoffDate(2, "hours");
    expect(result.getTime()).toBe(Date.now() - 2 * 3600_000);
  });

  it("subtracts days via millisecond math", () => {
    jest.setSystemTime(new Date("2023-06-15T12:00:00.000Z"));
    const result = computeCutoffDate(3, "days");
    expect(result.getTime()).toBe(Date.now() - 3 * 86_400_000);
  });

  it("subtracts months using calendar arithmetic", () => {
    jest.setSystemTime(new Date(2023, 5, 15, 12, 0, 0));
    const result = computeCutoffDate(1, "months");
    expect(result.getFullYear()).toBe(2023);
    expect(result.getMonth()).toBe(4); // May
    expect(result.getDate()).toBe(15);
  });

  it("uses calendar arithmetic (not fixed 30 days) for month overflow", () => {
    // Mar 31 minus 1 month => Feb 31 which rolls forward into March,
    // demonstrating Date.setMonth calendar arithmetic rather than fixed ms.
    jest.setSystemTime(new Date(2023, 2, 31, 12, 0, 0));
    const result = computeCutoffDate(1, "months");
    expect(result.getMonth()).toBe(2); // rolled back into March
  });
});

describe("getInternalCleanupConfigs", () => {
  it("returns the five built-in entries", () => {
    const configs = getInternalCleanupConfigs();
    expect(configs).toHaveLength(5);

    const byPath = (path: string) => configs.find((c) => c.collectionPath === path);

    expect(byPath("processedIds")).toMatchObject({
      isCollectionGroup: true,
      timestampField: "timestamp",
      olderThan: {value: 7, unit: "days"},
      recursive: false,
    });
    expect(byPath("executions")).toMatchObject({
      isCollectionGroup: true,
      timestampField: "execDate",
      olderThan: {value: 7, unit: "days"},
      recursive: false,
    });
    expect(byPath("computations")).toMatchObject({
      isCollectionGroup: true,
      timestampField: "createdAt",
      olderThan: {value: 30, unit: "days"},
      recursive: false,
    });
    expect(byPath("@emberflow/internal/viewLogicExecutions")).toMatchObject({
      timestampField: "execDate",
      olderThan: {value: 7, unit: "days"},
    });
    const actionsEntry = byPath("@actions");
    expect(actionsEntry).toMatchObject({
      timestampField: "timeCreated",
      olderThan: {value: 7, unit: "days"},
    });
    expect(typeof actionsEntry?.onBatchDeleted).toBe("function");
  });
});

describe("cleanupCollections", () => {
  let query: {where: jest.Mock};
  let collectionMock: jest.Mock;
  let collectionGroupMock: jest.Mock;
  let deleteCollectionSpy: jest.SpyInstance;
  let deleteCollectionRecursiveSpy: jest.SpyInstance;
  let rtdbUpdateMock: jest.Mock;

  const snapshot = {
    size: 2,
    docs: [
      {data: () => ({eventContext: {formId: "f1", uid: "u1"}})},
      {data: () => ({eventContext: {formId: "f2", uid: "u2"}})},
    ],
  } as unknown as FirebaseFirestore.QuerySnapshot;

  beforeEach(() => {
    initWithCleanupConfigs(projectCleanupConfigs);

    query = {where: jest.fn()};
    query.where.mockReturnValue(query);
    collectionMock = jest.fn().mockReturnValue(query);
    collectionGroupMock = jest.fn().mockReturnValue(query);
    jest.spyOn(admin.firestore(), "collection").mockImplementation(collectionMock as any);
    jest.spyOn(admin.firestore(), "collectionGroup").mockImplementation(collectionGroupMock as any);

    rtdbUpdateMock = jest.fn().mockResolvedValue(undefined);
    jest.spyOn(admin.database(), "ref").mockReturnValue({
      update: rtdbUpdateMock,
    } as unknown as admin.database.Reference);

    deleteCollectionSpy = jest.spyOn(misc, "deleteCollection")
      .mockImplementation(async (q, callback) => {
        if (callback) {
          await callback(snapshot);
        }
      });
    deleteCollectionRecursiveSpy = jest.spyOn(misc, "deleteCollectionRecursive")
      .mockImplementation(async (q, callback) => {
        if (callback) {
          await callback(snapshot);
        }
      });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("processes both internal and project entries in one pass", async () => {
    await cleanupCollections({} as ScheduledEvent);

    // Internal collection-group entries
    expect(collectionGroupMock).toHaveBeenCalledWith("processedIds");
    expect(collectionGroupMock).toHaveBeenCalledWith("executions");
    expect(collectionGroupMock).toHaveBeenCalledWith("computations");
    // Internal exact-path entries
    expect(collectionMock).toHaveBeenCalledWith("@emberflow/internal/viewLogicExecutions");
    expect(collectionMock).toHaveBeenCalledWith("@actions");
    // Project entry (collection group)
    expect(collectionGroupMock).toHaveBeenCalledWith("askJaris");
  });

  it("builds the age threshold and extra conditions in one query", async () => {
    await cleanupCollections({} as ScheduledEvent);

    expect(query.where).toHaveBeenCalledWith("createdAt", "<", expect.any(Date));
    expect(query.where).toHaveBeenCalledWith("timeCreated", "<", expect.any(Date));
    // project askJaris condition is chained onto the timestamp filter
    expect(query.where).toHaveBeenCalledWith("hasTopic", "==", false);
  });

  it("deletes recursively by default and flatly when recursive is false", async () => {
    await cleanupCollections({} as ScheduledEvent);

    // flat: processedIds, executions, computations (recursive: false)
    expect(deleteCollectionSpy).toHaveBeenCalledTimes(3);
    // recursive (default): viewLogicExecutions, @actions, askJaris
    expect(deleteCollectionRecursiveSpy).toHaveBeenCalledTimes(3);
  });

  it("runs the @actions RTDB hook to null forms/{uid}/{formId}", async () => {
    await cleanupCollections({} as ScheduledEvent);

    expect(rtdbUpdateMock).toHaveBeenCalledTimes(1);
    expect(rtdbUpdateMock).toHaveBeenCalledWith({
      "forms/u1/f1": null,
      "forms/u2/f2": null,
    });
  });

  it("logs the number of deleted documents per entry", async () => {
    const infoSpy = jest.spyOn(console, "info").mockImplementation();
    await cleanupCollections({} as ScheduledEvent);

    expect(infoSpy).toHaveBeenCalledWith("cleanupCollections: deleted 2 from @actions");
    expect(infoSpy).toHaveBeenCalledWith("cleanupCollections: deleted 2 from askJaris");
  });

  it("isolates per-entry failures and continues processing", async () => {
    const errorSpy = jest.spyOn(console, "error").mockImplementation();
    collectionGroupMock.mockImplementation((name: string) => {
      if (name === "executions") {
        throw new Error("boom");
      }
      return query;
    });

    await cleanupCollections({} as ScheduledEvent);

    expect(errorSpy).toHaveBeenCalledWith(
      "cleanupCollections failed for executions",
      expect.any(Error),
    );
    // later entries still processed
    expect(collectionMock).toHaveBeenCalledWith("@actions");
    expect(collectionGroupMock).toHaveBeenCalledWith("askJaris");
  });

  it("runs internal entries only when no project configs are supplied", async () => {
    initWithCleanupConfigs([]);
    await cleanupCollections({} as ScheduledEvent);

    expect(collectionGroupMock).not.toHaveBeenCalledWith("askJaris");
    expect(collectionMock).toHaveBeenCalledWith("@actions");
  });
});
