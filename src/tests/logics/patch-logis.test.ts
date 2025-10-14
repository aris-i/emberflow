import {firestore} from "firebase-admin";
import * as paths from "../../utils/paths";
import * as admin from "firebase-admin";
import {PatchLogicConfig, ProjectConfig} from "../../types";
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
import * as viewLogics from "../../logics/view-logics";
import * as patchLogics from "../../logics/patch-logics";
import * as indexUtils from "../../index-utils";
import {CloudEvent} from "firebase-functions/core";
import {MessagePublishedData} from "firebase-functions/pubsub";
import {pubsubUtils} from "../../utils/pubsub";
import Timestamp = firestore.Timestamp;

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
  let findMatchingPatchLogicsSpy: jest.SpyInstance;

  const patchLogicConfigs: PatchLogicConfig[] = [
    {
      name: "User Patch Version 1",
      entity: "user",
      patchLogicFn: jest.fn(),
      version: "1.0.0",
    },
  ];

  beforeEach(() => {
    publishMessageSpy = jest.spyOn(PATCH_LOGICS_TOPIC, "publishMessage")
      .mockResolvedValue(messageId as never);
    findMatchingPatchLogicsSpy = jest.spyOn(patchLogics, "findMatchingPatchLogics");
  });
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should queue docs to run patch logics", async () => {
    const appVersion = "3.0.0";
    const dstPath = "users/userId";
    findMatchingPatchLogicsSpy.mockResolvedValue({patchLogicConfigs, dataVersion: "1.0.0"});
    await patchLogics.queueRunPatchLogics(appVersion, dstPath);

    expect(findMatchingPatchLogicsSpy).toHaveBeenCalledTimes(1);
    expect(publishMessageSpy).toHaveBeenCalledWith({json: {appVersion, dstPath}});
  });

  it("should be able to queue multiple dstPaths", async () => {
    const appVersion = "3.0.0";
    const dstPathList = [
      "users/userId",
      "users/userId2",
      "users/userId3",
    ];
    findMatchingPatchLogicsSpy.mockResolvedValue({patchLogicConfigs, dataVersion: "1.0.0"});
    await patchLogics.queueRunPatchLogics(appVersion, ...dstPathList);

    expect(findMatchingPatchLogicsSpy).toHaveBeenCalledTimes(3);
    expect(publishMessageSpy).toHaveBeenNthCalledWith(1, {json: {appVersion, dstPath: dstPathList[0]}});
    expect(publishMessageSpy).toHaveBeenNthCalledWith(2, {json: {appVersion, dstPath: dstPathList[1]}});
    expect(publishMessageSpy).toHaveBeenNthCalledWith(3, {json: {appVersion, dstPath: dstPathList[2]}});
  });

  it("should not  queue dstPaths if no matching patch logics", async () => {
    const appVersion = "3.0.0";
    const dstPathList = [
      "users/userId",
      "users/userId2",
      "users/userId3",
    ];
    findMatchingPatchLogicsSpy.mockResolvedValue({patchLogicConfigs: undefined, dataVersion: undefined});
    await patchLogics.queueRunPatchLogics(appVersion, ...dstPathList);

    expect(findMatchingPatchLogicsSpy).toHaveBeenCalledTimes(3);
    expect(publishMessageSpy).toHaveBeenCalledTimes(0);
  });
});

describe("versionCompare", () => {
  it("returns 0 when versions are identical", () => {
    expect(patchLogics.versionCompare("1.2.3", "1.2.3")).toBe(0);
    expect(patchLogics.versionCompare("1", "1")).toBe(0);
  });

  it("returns a negative number when first is lower", () => {
    expect(patchLogics.versionCompare("1.2.3", "1.2.4")).toBe(-1);
    expect(patchLogics.versionCompare("1.2", "1.2.1")).toBe(-1);
    expect(patchLogics.versionCompare("1", "1.0.1")).toBe(-1);
  });

  it("returns a positive number when first is higher", () => {
    expect(patchLogics.versionCompare("1.2.5", "1.2.4")).toBe(1);
    expect(patchLogics.versionCompare("2.0", "1.9.9")).toBe(1);
    expect(patchLogics.versionCompare("1.0.1", "1")).toBe(1);
  });

  it("handles leading zeros correctly", () => {
    expect(patchLogics.versionCompare("01.02.03", "1.2.3")).toBe(0);
    expect(patchLogics.versionCompare("01.10", "1.2")).toBe(1);
  });

  it("handles different segment lengths gracefully", () => {
    expect(patchLogics.versionCompare("1.2", "1.2.0.0")).toBe(0);
    expect(patchLogics.versionCompare("1.2.0.1", "1.2")).toBe(1);
  });

  it("treats non-numeric segments as 0", () => {
    // Because parseInt of non-numeric yields NaN, then || 0 → 0
    expect(patchLogics.versionCompare("1.a.3", "1.0.3")).toBe(0);
    expect(patchLogics.versionCompare("1.a.4", "1.0.3")).toBe(1);
    expect(patchLogics.versionCompare("1.a.4", "1.0.3")).toBe(1);
    expect(patchLogics.versionCompare("1.a.4", "1.0.3")).toBe(1);
  });
});

describe("runPatchLogics", () => {
  let queueRunViewLogicsSpy : jest.SpyInstance;
  let runTransactionSpy: jest.SpyInstance;
  let createMetricExecutionSpy: jest.SpyInstance;
  let distributeFnTransactionalSpy: jest.SpyInstance;

  const userLogicFn1 = jest.fn().mockResolvedValue({
    status: "finished",
    documents: [],
  });
  const userLogicFn2Result = {
    status: "finished",
    documents: [{
      "action": "merge",
      "doc": {
        firstName: "John",
      },
      "dstPath": "users/userId",
    }],
  };
  const userLogicFn2 = jest.fn().mockResolvedValue(userLogicFn2Result);
  const additionalUserLogicFn2Result = {
    status: "finished",
    documents: [
      {
        "action": "merge",
        "doc": {
          lastName: "Doe",
        },
        "dstPath": "users/userId",
      },
    ],
  };
  const additionalUserLogicFn2 = jest.fn().mockResolvedValue(additionalUserLogicFn2Result);
  const userLogicFn2p5Result = {
    status: "finished",
    documents: [{
      "action": "merge",
      "doc": {
        fullName: "John Doe",
      },
      "dstPath": "users/userId",
    }],
  };
  const userLogicFn2p5 = jest.fn().mockResolvedValue(userLogicFn2p5Result);
  const topicLogicFn1 = jest.fn().mockResolvedValue({
    status: "finished",
    documents: [],
  });
  const userLogicFn3 = jest.fn().mockResolvedValue({
    status: "finished",
    documents: [],
  });

  const patchLogicConfigs: PatchLogicConfig[] = [
    {
      name: "User Patch Version 1",
      entity: "user",
      patchLogicFn: userLogicFn1,
      version: "1.0.0",
    },
    {
      name: "User Patch Version 2",
      entity: "user",
      patchLogicFn: userLogicFn2,
      version: "2.0.0",
    },
    {
      name: "Additional User Patch Version 2",
      entity: "user",
      patchLogicFn: additionalUserLogicFn2,
      version: "2.0.0",
    },
    {
      name: "User Patch Version 2.5",
      entity: "user",
      patchLogicFn: userLogicFn2p5,
      version: "2.5.0",
    },
    {
      name: "Topic Patch Version 1",
      entity: "topic",
      patchLogicFn: topicLogicFn1,
      version: "1.0.0",
    },
    {
      name: "User Patch Version 3",
      entity: "user",
      patchLogicFn: userLogicFn3,
      version: "3.0.0",
    },
  ];

  const dataVersion = "1.0.0";
  const appVersion = "2.9.0";

  const txnResult1 = {
    get: jest.fn().mockResolvedValue({
      data: jest.fn().mockReturnValue({
        "userId": "userId",
        "@dataVersion": dataVersion,
      }),
    }),
  } as unknown as Transaction;
  const txnResult2 = {
    get: jest.fn().mockResolvedValue({
      data: jest.fn().mockReturnValue({
        "userId": "userId",
        "@dataVersion": "2.0.0",
        "firstName": "John",
        "lastName": "Doe",
      }),
    }),
  };

  beforeEach(() => {
    jest.spyOn(indexUtils, "createMetricLogicDoc").mockResolvedValue();
    jest.spyOn(admin.firestore(), "doc").mockReturnValue(({
      get: jest.fn().mockResolvedValue({
        data: jest.fn().mockReturnValue({
          "userId": "userId",
          "@dataVersion": dataVersion,
        }),
      }),
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>);
    jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({
      entity: "user",
      regex: /users/,
    });
    createMetricExecutionSpy = jest.spyOn(indexUtils._mockable, "createMetricExecution").mockResolvedValue();
    queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
    runTransactionSpy = jest.spyOn(db, "runTransaction")
      .mockImplementationOnce(async (callback: any) => callback(txnResult1))
      .mockImplementationOnce(async (callback: any) => callback(txnResult2));
    distributeFnTransactionalSpy = jest.spyOn(indexUtils, "distributeFnTransactional")
      .mockResolvedValueOnce([
        {
          action: "merge",
          dstPath: "users/userId",
          doc: {
            "firstName": "John",
            "lastName": "Doe",
            "@dataVersion": "2.0.0",
          },
        },
      ]).mockResolvedValueOnce([
        {
          action: "merge",
          dstPath: "users/userId",
          doc: {
            "fullName": "John Doe",
            "@dataVersion": "2.5.0",
          },
        },
      ]);
  });
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should appropriately group the matched logics by version then run transactions for each group", async () => {
    const dstPath = "users/userId";
    initializeEmberFlow(
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs,
      validatorConfigs,
      [],
      patchLogicConfigs
    );
    await patchLogics.runPatchLogics(appVersion, dstPath);
    expect(runTransactionSpy).toHaveBeenCalledTimes(2); // for version 2.0.0 and 2.5.0
  });

  it("should run all patch logics between dataVersion and appVersion", async () => {
    const dstPath = "users/userId";

    initializeEmberFlow(
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs,
      validatorConfigs,
      [],
      patchLogicConfigs
    );
    await patchLogics.runPatchLogics(appVersion, dstPath);

    expect(userLogicFn1).not.toHaveBeenCalled(); // below data version
    expect(userLogicFn2).toHaveBeenCalled();
    expect(additionalUserLogicFn2).toHaveBeenCalled();
    expect(userLogicFn2p5).toHaveBeenCalled();
    expect(topicLogicFn1).not.toHaveBeenCalled(); // wrong entity
    expect(userLogicFn3).not.toHaveBeenCalled(); // ahead the app version
  });

  it("should distribute all consolidated logic result docs", async () => {
    const dstPath = "users/userId";

    initializeEmberFlow(
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs,
      validatorConfigs,
      [],
      patchLogicConfigs
    );
    await patchLogics.runPatchLogics(appVersion, dstPath);

    expect(distributeFnTransactionalSpy).toHaveBeenCalledTimes(2);
    expect(distributeFnTransactionalSpy).toHaveBeenNthCalledWith(1, txnResult1, [
      {
        ...userLogicFn2Result,
        documents: [{
          ...userLogicFn2Result.documents[0],
          "doc": {
            ...userLogicFn2Result.documents[0].doc,
            "@dataVersion": "2.0.0",
          },
        }],
        transactional: true,
        execTime: expect.any(Number),
      },
      {
        ...additionalUserLogicFn2Result,
        documents: [
          {
            ...additionalUserLogicFn2Result.documents[0],
            "doc": {
              ...additionalUserLogicFn2Result.documents[0].doc,
              "@dataVersion": "2.0.0",
            },
          },
        ],
        transactional: true,
        execTime: expect.any(Number),
      },
    ]);
    expect(distributeFnTransactionalSpy).toHaveBeenNthCalledWith(2, txnResult2, [
      {
        ...userLogicFn2p5Result,
        documents: [{
          ...userLogicFn2p5Result.documents[0],
          "doc": {
            ...userLogicFn2p5Result.documents[0].doc,
            "@dataVersion": "2.5.0",
          },
        }],
        execTime: expect.any(Number),
        transactional: true,
      },
    ]);
    expect(queueRunViewLogicsSpy).toHaveBeenCalledTimes(2);
  });

  it("should run createMetricExecution per group patch", async () => {
    const dstPath = "users/userId";

    initializeEmberFlow(
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs,
      validatorConfigs,
      [],
      patchLogicConfigs
    );
    await patchLogics.runPatchLogics(appVersion, dstPath);
    expect(createMetricExecutionSpy).toHaveBeenCalledTimes(2);
    // version 2.0.0
    expect(createMetricExecutionSpy).toHaveBeenNthCalledWith(1,
      [{
        execTime: expect.any(Number),
        status: "finished",
        timeFinished: expect.any(Timestamp),
        documents: [{"action": "merge", "doc": {"firstName": "John", "@dataVersion": "2.0.0"}, "dstPath": "users/userId"}],
        transactional: true,
      },
      {
        execTime: expect.any(Number),
        status: "finished",
        timeFinished: expect.any(Timestamp),
        documents: [{action: "merge", doc: {"lastName": "Doe", "@dataVersion": "2.0.0"}, dstPath: "users/userId"}],
        transactional: true,
      },
      {
        execTime: expect.any(Number),
        status: "finished",
        name: "runPatchLogics",
        documents: [],
      },
      ]
    );
    // version 2.5.0
    expect(createMetricExecutionSpy).toHaveBeenNthCalledWith(2,
      [{
        execTime: expect.any(Number),
        status: "finished",
        timeFinished: expect.any(Timestamp),
        documents: [{action: "merge", doc: {"fullName": "John Doe", "@dataVersion": "2.5.0"}, dstPath: "users/userId"}],
        transactional: true,
      },
      {
        execTime: expect.any(Number),
        status: "finished",
        name: "runPatchLogics",
        documents: [],
      },
      ]);
  });
});

describe("onMessageRunPatchLogicsQueue", () => {
  let isProcessedSpy: jest.SpyInstance;
  let trackProcessedIdsSpy: jest.SpyInstance;
  let runPatchLogicsSpy: jest.SpyInstance;

  const dstPath = "users/userId";

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
    runPatchLogicsSpy = jest.spyOn(patchLogics, "runPatchLogics").mockResolvedValue();
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
    const result = await patchLogics.onMessageRunPatchLogicsQueue(event);

    expect(isProcessedSpy).toHaveBeenCalledWith(PATCH_LOGICS_TOPIC_NAME, event.id);
    expect(runPatchLogicsSpy).toHaveBeenCalledWith(targetVersion, dstPath);

    expect(trackProcessedIdsSpy).toHaveBeenCalledWith(PATCH_LOGICS_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed patch logics");
  });
});
