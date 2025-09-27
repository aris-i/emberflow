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
import {db, initializeEmberFlow, PATCH_LOGICS_TOPIC} from "../../index";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import Transaction = firestore.Transaction;
import Timestamp = firestore.Timestamp;
import {pubsubUtils} from "../../utils/pubsub";
import * as ViewLogics from "../../logics/view-logics";
import * as PatchLogics from "../../logics/patch-logics";
import * as indexUtils from "../../index-utils";
import {CloudEvent} from "firebase-functions/core";
import {MessagePublishedData} from "firebase-functions/pubsub";

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
    expect(versionCompare("1.2.3", "1.2.4")).toBeLessThan(0);
    expect(versionCompare("1.2", "1.2.1")).toBeLessThan(0);
    expect(versionCompare("1", "1.0.1")).toBeLessThan(0);
  });

  it("returns a positive number when first is higher", () => {
    expect(versionCompare("1.2.5", "1.2.4")).toBeGreaterThan(0);
    expect(versionCompare("2.0", "1.9.9")).toBeGreaterThan(0);
    expect(versionCompare("1.0.1", "1")).toBeGreaterThan(0);
  });

  it("handles leading zeros correctly", () => {
    expect(versionCompare("01.02.03", "1.2.3")).toBe(0);
    expect(versionCompare("01.10", "1.2")).toBeGreaterThan(0);
  });

  it("handles different segment lengths gracefully", () => {
    expect(versionCompare("1.2", "1.2.0.0")).toBe(0);
    expect(versionCompare("1.2.0.1", "1.2")).toBeGreaterThan(0);
  });

  it("treats non-numeric segments as 0", () => {
    // Because parseInt of non-numeric yields NaN, then || 0 → 0
    expect(versionCompare("1.a.3", "1.0.3")).toBe(0);
    expect(versionCompare("1.a.4", "1.0.3")).toBeGreaterThan(0);
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
  const isProcessedSpy =
    jest.spyOn(pubsubUtils, "isProcessed").mockResolvedValue(false);
  const trackProcessedIdsSpy =
    jest.spyOn(pubsubUtils, "trackProcessedIds").mockResolvedValue();
  const queueRunViewLogicsSpy =
    jest.spyOn(ViewLogics, "queueRunViewLogics").mockResolvedValue();

  const patchLogicResult1: LogicResult = {
    name: "logic1",
    status: "finished",
    documents: [
      {
        action: "merge",
        doc: {"@dataVersion": "2.0.0"},
        dstPath: "users/userId",
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
        dstPath: "users/userId",
      },
      {
        action: "merge",
        doc: {"@dataVersion": "2.5.0"},
        dstPath: "users/userId",
      },
    ],
  };

  const patchLogicResults: LogicResult[] = [patchLogicResult1, patchLogicResult2];

  const runPatchLogicsSpy =
    jest.spyOn(PatchLogics, "runPatchLogics").mockResolvedValue(patchLogicResults);

  const txnGet = jest.fn();
  const fakeTxn = {
    get: txnGet,
  } as unknown as FirebaseFirestore.Transaction;

  const runTxnSpy = jest
    .spyOn(db, "runTransaction")
    .mockImplementation(async (callback: any) => callback(fakeTxn));

  const expandConsolidateResult = new Map<string, LogicResultDoc[]>([
    ["users/doc1", [{
      action: "merge",
      priority: "normal",
      dstPath: "users/doc1",
      doc: {
        firstName: "john",
        lastName: "doe",
      },
      instructions: {field2: "--", field4: "--"},
    }]],
  ]);
  const expandConsolidateAndGroupByDstPathSpy =
    jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath")
      .mockResolvedValue(expandConsolidateResult);

  const event = {
    data: {
      message: {
        json: {
          appVersion: "2.9.0",
          dstPath: "users/userId",
        },
      },
    },
  } as CloudEvent<MessagePublishedData>;

  it("should properly run the function", async () => {
    const result = await onMessageRunPatchLogicsQueue(event);
    expect(runTxnSpy).toHaveBeenCalledTimes(1);
    expect(isProcessedSpy).toHaveBeenCalled();
    expect(trackProcessedIdsSpy).toHaveBeenCalled();
    expect(trackProcessedIdsSpy).toHaveBeenCalled();
    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith("users/userId");
    expect(runPatchLogicsSpy).toHaveBeenCalledWith("2.9.0", "users/userId", fakeTxn);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalledWith();
    expect(result).toEqual("Processed patch logics");
  });
});
