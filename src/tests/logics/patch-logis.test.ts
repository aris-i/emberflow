import {
  onMessageRunPatchLogicsQueue,
  queueRunPatchLogics,
  runPatchLogics,
  versionCompare,
} from "../../logics/patch-logics";
import {firestore} from "firebase-admin";
import * as paths from "../../utils/paths";
import * as admin from "firebase-admin";
import {LogicResult, LogicResultDoc, PatchLogicConfig, ProjectConfig} from "../../types";
import {
  db,
  initializeEmberFlow,
  PATCH_LOGICS_TOPIC,
  PATCH_LOGICS_TOPIC_NAME,
} from "../../index";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import Transaction = firestore.Transaction;
import Timestamp = firestore.Timestamp;
import * as viewLogics from "../../logics/view-logics";
import * as patchLogics from "../../logics/patch-logics";
import * as indexUtils from "../../index-utils";
import {CloudEvent} from "firebase-functions/core";
import {MessagePublishedData} from "firebase-functions/pubsub";
import {pubsubUtils} from "../../utils/pubsub";

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

initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfigs, validatorConfigs, [], []);

describe("queueRunPatchLogics", () => {
  const messageId = "test-message-id";
  let publishMessageSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(PATCH_LOGICS_TOPIC, "publishMessage")
      .mockResolvedValue(messageId as never);
  });

  it("should queue docs to run patch logics", async () => {
    const appVersion = "3.0.0";
    const dstPath = "users/userId";
    await queueRunPatchLogics(appVersion, dstPath);

    expect(publishMessageSpy).toHaveBeenCalledWith({json: {appVersion, dstPath}});
  });

  it("should be able to queue multiple dstPaths", async () => {
    const appVersion = "3.0.0";
    const dstPathList = [
      "users/userId",
      "users/userId2",
      "users/userId3",
    ];
    await queueRunPatchLogics(appVersion, ...dstPathList);

    expect(publishMessageSpy).toHaveBeenNthCalledWith(1, {json: {appVersion, dstPath: dstPathList[0]}});
    expect(publishMessageSpy).toHaveBeenNthCalledWith(2, {json: {appVersion, dstPath: dstPathList[1]}});
    expect(publishMessageSpy).toHaveBeenNthCalledWith(3, {json: {appVersion, dstPath: dstPathList[2]}});
  });
});

describe("versionCompare", () => {
  it("returns 0 when versions are identical", () => {
    expect(versionCompare("1.2.3", "1.2.3")).toBe(0);
    expect(versionCompare("1", "1")).toBe(0);
  });

  it("returns a negative number when first is lower", () => {
    expect(versionCompare("1.2.3", "1.2.4")).toBe(-1);
    expect(versionCompare("1.2", "1.2.1")).toBe(-1);
    expect(versionCompare("1", "1.0.1")).toBe(-1);
  });

  it("returns a positive number when first is higher", () => {
    expect(versionCompare("1.2.5", "1.2.4")).toBe(1);
    expect(versionCompare("2.0", "1.9.9")).toBe(1);
    expect(versionCompare("1.0.1", "1")).toBe(1);
  });

  it("handles leading zeros correctly", () => {
    expect(versionCompare("01.02.03", "1.2.3")).toBe(0);
    expect(versionCompare("01.10", "1.2")).toBe(1);
  });

  it("handles different segment lengths gracefully", () => {
    expect(versionCompare("1.2", "1.2.0.0")).toBe(0);
    expect(versionCompare("1.2.0.1", "1.2")).toBe(1);
  });

  it("treats non-numeric segments as 0", () => {
    // Because parseInt of non-numeric yields NaN, then || 0 → 0
    expect(versionCompare("1.a.3", "1.0.3")).toBe(0);
    expect(versionCompare("1.a.4", "1.0.3")).toBe(1);
    expect(versionCompare("1.a.4", "1.0.3")).toBe(1);
    expect(versionCompare("1.a.4", "1.0.3")).toBe(1);
  });
});

describe("runPatchLogics", () => {
  const logicFn1 = jest.fn().mockResolvedValue({
    status: "finished",
    documents: [],
  });
  const logicFn2 = jest.fn().mockResolvedValue({
    status: "finished",
    documents: [],
  });
  const logicFn2Point5 = jest.fn().mockResolvedValue({
    status: "finished",
    documents: [],
  });
  const logicFn3 = jest.fn().mockResolvedValue({
    status: "finished",
    documents: [],
  });
  const patchLogics: PatchLogicConfig[] = [
    {
      name: "Patch Version 1",
      entity: "user",
      patchLogicFn: logicFn1,
      version: "1.0.0",
    },
    {
      name: "Patch Version 2",
      entity: "user",
      patchLogicFn: logicFn2,
      version: "2.0.0",
    },
    {
      name: "Patch Version 2.5",
      entity: "user",
      patchLogicFn: logicFn2Point5,
      version: "2.5.0",
    },
    {
      name: "Patch Version 3",
      entity: "user",
      patchLogicFn: logicFn3,
      version: "3.0.0",
    },
  ];

  const dataVersion = "1.0.0";
  const appVersion = "2.9.0";

  const txn = {
    get: jest.fn().mockResolvedValue({
      data: jest.fn().mockReturnValue({
        "userId": "userId",
        "@dataVersion": dataVersion,
      }),
    }),
  } as unknown as Transaction;
  jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({
    entity: "user",
    regex: /users/,
  });

  it("should run the patch logics between dataVersion and appVersion then " +
    "update the version of the data", async () => {
    const dstPath = "users/userId";

    initializeEmberFlow(
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs,
      validatorConfigs,
      [],
      patchLogics
    );
    const result = await runPatchLogics(appVersion, dstPath, txn);

    // should run the appropriate logics
    expect(logicFn1).not.toHaveBeenCalled();
    expect(logicFn2).toHaveBeenCalled();
    expect(logicFn2Point5).toHaveBeenCalled();
    expect(logicFn3).not.toHaveBeenCalled();

    // should run the logics in the correct order
    expect(logicFn2.mock.invocationCallOrder[0])
      .toBeLessThan(logicFn2Point5.mock.invocationCallOrder[0]);

    // should update the data version
    expect(result).toEqual([
      {
        status: "finished",
        documents: [
          {
            "action": "merge",
            "doc": {
              "@dataVersion": "2.0.0",
            },
            "dstPath": "users/userId",
          },
        ],
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      },
      {
        status: "finished",
        documents: [
          {
            "action": "merge",
            "doc": {
              "@dataVersion": "2.5.0",
            },
            "dstPath": "users/userId",
          },
        ],
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      },
    ]);
  });
});

describe("onMessageRunPatchLogicsQueue", () => {
  let isProcessedSpy: jest.SpyInstance;
  let trackProcessedIdsSpy: jest.SpyInstance;
  let queueRunViewLogicsSpy: jest.SpyInstance;
  let runPatchLogicsSpy: jest.SpyInstance;
  let runTransactionSpy: jest.SpyInstance;
  let expandConsolidateAndGroupByDstPathSpy: jest.SpyInstance;
  let distributeDocSpy: jest.SpyInstance;

  const dstPath = "users/userId";
  const patchLogicResult1: LogicResult = {
    name: "logic1",
    status: "finished",
    documents: [
      {
        action: "merge",
        doc: {"@dataVersion": "2.0.0"},
        dstPath,
      },
      {
        action: "create",
        doc: {
          "@id": "userId",
          "firstName": "Maria",
          "lastName": "Doe",
        },
        dstPath: "users/userId/collaborators/collaboratorId",
      },
    ],
  };
  const patchLogicResult2: LogicResult = {
    name: "logic2",
    status: "finished",
    documents: [
      {
        action: "merge",
        doc: {
          firstName: "john",
          lastName: "doe",
        },
        instructions: {
          fullName: "del",
        },
        dstPath,
      },
      {
        action: "merge",
        doc: {"@dataVersion": "2.5.0"},
        dstPath,
      },
    ],
  };
  const patchLogicResults: LogicResult[] = [patchLogicResult1, patchLogicResult2];
  const txnGet = jest.fn();
  const fakeTxn = {
    get: txnGet,
  } as unknown as FirebaseFirestore.Transaction;

  const expandConsolidateResult = new Map<string, LogicResultDoc[]>([
    ["users/doc1", [
      {
        action: "merge",
        priority: "normal",
        dstPath: "users/doc1",
        doc: {
          firstName: "john",
          lastName: "doe",
        },
        instructions: {field2: "--", field4: "--"},
      }],
    ],
    ["users/userId/collaborators/collaboratorId", [
      {
        action: "create",
        doc: {
          "@id": "userId",
          "firstName": "Maria",
          "lastName": "Doe",
        },
        dstPath: "users/userId/collaborators/collaboratorId",
      }]],
  ]);

  const targetVersion = "2.9.0";
  const event = {
    data: {
      message: {
        json: {
          appVersion: targetVersion,
          dstPath: "users/userId",
        },
      },
    },
  } as CloudEvent<MessagePublishedData>;

  beforeEach(() => {
    isProcessedSpy = jest.spyOn(pubsubUtils, "isProcessed").mockResolvedValue(false);
    trackProcessedIdsSpy = jest.spyOn(pubsubUtils, "trackProcessedIds").mockResolvedValue();
    queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
    runPatchLogicsSpy = jest.spyOn(patchLogics, "runPatchLogics").mockResolvedValue(patchLogicResults);
    runTransactionSpy = jest.spyOn(db, "runTransaction").mockImplementation(async (callback: any) => callback(fakeTxn));
    expandConsolidateAndGroupByDstPathSpy = jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath")
      .mockResolvedValue(expandConsolidateResult);
    distributeDocSpy = jest.spyOn(indexUtils, "distributeDoc").mockResolvedValue();
  });
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should skip duplicate message", async () => {
    isProcessedSpy = jest.spyOn(pubsubUtils, "isProcessed").mockResolvedValue(true);
    jest.spyOn(console, "log").mockImplementation();
    await patchLogics.onMessageRunPatchLogicsQueue(event);

    expect(isProcessedSpy).toHaveBeenCalledWith(PATCH_LOGICS_TOPIC_NAME, event.id);
    expect(console.log).toHaveBeenCalledWith("Skipping duplicate message");
  });

  it("should distribute patch logic result docs", async () => {
    jest.spyOn(indexUtils._mockable, "createMetricExecution").mockResolvedValue();
    const result = await onMessageRunPatchLogicsQueue(event);

    expect(isProcessedSpy).toHaveBeenCalledWith(PATCH_LOGICS_TOPIC_NAME, event.id);
    expect(runTransactionSpy).toHaveBeenCalledTimes(1);
    expect(runPatchLogicsSpy).toHaveBeenCalledWith(targetVersion, dstPath, fakeTxn);

    const flattenedPatchLogicResults = patchLogicResults.flatMap((result) => result.documents);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalledWith(flattenedPatchLogicResults);

    const consolidatedLogicResultDoc1 = expandConsolidateResult.get("users/doc1")?.[0];
    expect(distributeDocSpy).toHaveBeenNthCalledWith(1, consolidatedLogicResultDoc1, undefined, fakeTxn);
    const consolidatedLogicResultDoc2 = expandConsolidateResult.get("users/userId/collaborators/collaboratorId")?.[0];
    expect(distributeDocSpy).toHaveBeenNthCalledWith(2, consolidatedLogicResultDoc2, undefined, fakeTxn);

    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith(
      targetVersion,
      consolidatedLogicResultDoc1,
      consolidatedLogicResultDoc2,
    );

    expect(trackProcessedIdsSpy).toHaveBeenCalledWith(PATCH_LOGICS_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed patch logics");
  });
});
