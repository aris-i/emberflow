import {runPatchLogics, versionCompare} from "../../logics/patch-logics";
import {firestore} from "firebase-admin";
import * as paths from "../../utils/paths";
import * as admin from "firebase-admin";
import {PatchLogicConfig, ProjectConfig} from "../../types";
import {initializeEmberFlow} from "../../index";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import Transaction = firestore.Transaction;
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

  it("should run the patch logics between dataVersion and appVersion", async () => {
    const dstPath = "users/userId";

    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfigs, validatorConfigs, [], patchLogics);
    await runPatchLogics(appVersion, dstPath, txn);

    expect(logicFn1).not.toHaveBeenCalled();
    expect(logicFn2).toHaveBeenCalled();
    expect(logicFn2Point5).toHaveBeenCalled();
    expect(logicFn3).not.toHaveBeenCalled();

    expect(logicFn2.mock.invocationCallOrder[0])
      .toBeLessThan(logicFn2Point5.mock.invocationCallOrder[0]);
  });

  it("should run the patch logics between dataVersion and appVersion", async () => {
    const dstPath = "users/userId";

    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfigs, validatorConfigs, [], patchLogics);
    const result = await runPatchLogics(appVersion, dstPath, txn);

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
        documents: [],
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      },
    ]);
  });
});
