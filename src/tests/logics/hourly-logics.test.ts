import * as admin from "firebase-admin";
import {HourlyLogicConfig, LogicResult, LogicResultDoc, ProjectConfig} from "../../types";
import {
  initializeEmberFlow,
} from "../../index";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import * as hourlyLogics from "../../logics/hourly-logics";
import type {ScheduledEvent} from "firebase-functions/v2/scheduler";

const projectConfig: ProjectConfig = {
  projectId: "your-project-id",
  region: "asia-southeast1",
  rtdbName: "your-rtdb-name",
  budgetAlertTopicName: "budget-alerts",
  maxCostLimitPerFunction: 100,
  specialCostLimitPerFunction: {},
};

if (!admin.apps.length) {
  admin.initializeApp({
    databaseURL: "https://test-project.firebaseio.com",
  });
}

describe("hourlyBusinessLogics", () => {
  const dummyEvent: ScheduledEvent = {
    jobName: "firebase-schedule-hourlyBusinessLogics",
    scheduleTime: "2026-10-09T08:00:00Z",
  };

  let getHourlyLogicConfigsSpy: jest.SpyInstance;
  let saveMetricExecutionSpy: jest.SpyInstance;
  let distributeAndCascadeLogicResultDocsSpy: jest.SpyInstance;

  beforeEach(() => {
    initializeEmberFlow({
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs,
      validatorConfigs,
      logicConfigs: [],
      patchLogicConfigs: [],
      hourlyLogicConfigs: [],
    });

    getHourlyLogicConfigsSpy = jest.spyOn(hourlyLogics._mockable, "getHourlyLogicConfigs");
    saveMetricExecutionSpy = jest.spyOn(hourlyLogics._mockable, "saveMetricExecution").mockResolvedValue(undefined as never);
    distributeAndCascadeLogicResultDocsSpy = jest.spyOn(hourlyLogics._mockable, "distributeAndCascadeLogicResultDocs").mockResolvedValue({
      distributedDocs: [],
      docsForViewLogics: [],
      pathsForPatchLogics: new Set(),
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should return empty array when no hourly logic configs exist", async () => {
    getHourlyLogicConfigsSpy.mockReturnValue([]);
    const results = await hourlyLogics.runHourlyBusinessLogics(dummyEvent);

    expect(results).toEqual([]);
    expect(saveMetricExecutionSpy).not.toHaveBeenCalled();
  });

  it("should execute registered hourly logic functions and record metrics", async () => {
    const mockHourlyFn = jest.fn().mockResolvedValue({
      name: "MockHourlyLogic",
      status: "finished",
      documents: [],
    } as LogicResult);

    const configs: HourlyLogicConfig[] = [
      {
        name: "MockHourlyLogic",
        hourlyLogicFn: mockHourlyFn,
      },
    ];
    getHourlyLogicConfigsSpy.mockReturnValue(configs);

    const results = await hourlyLogics.runHourlyBusinessLogics(dummyEvent);

    expect(mockHourlyFn).toHaveBeenCalledWith(dummyEvent);
    expect(results.length).toBe(1);
    expect(results[0].name).toBe("MockHourlyLogic");
    expect(results[0].status).toBe("finished");
    expect(results[0].execTime).toBeDefined();
    expect(results[0].timeFinished).toBeDefined();
    expect(saveMetricExecutionSpy).toHaveBeenCalledWith([
      expect.objectContaining({name: "MockHourlyLogic"}),
    ]);
  });

  it("should catch errors in hourly logic functions and continue with other logics", async () => {
    const failingHourlyFn = jest.fn().mockRejectedValue(new Error("Hourly logic failed"));
    const succeedingHourlyFn = jest.fn().mockResolvedValue({
      name: "SucceedingLogic",
      status: "finished",
      documents: [],
    } as LogicResult);

    const configs: HourlyLogicConfig[] = [
      {
        name: "FailingLogic",
        hourlyLogicFn: failingHourlyFn,
      },
      {
        name: "SucceedingLogic",
        hourlyLogicFn: succeedingHourlyFn,
      },
    ];
    getHourlyLogicConfigsSpy.mockReturnValue(configs);

    const results = await hourlyLogics.runHourlyBusinessLogics(dummyEvent);

    expect(results.length).toBe(2);
    expect(results[0].name).toBe("FailingLogic");
    expect(results[0].status).toBe("error");
    expect(results[0].message).toBe("Hourly logic failed");

    expect(results[1].name).toBe("SucceedingLogic");
    expect(results[1].status).toBe("finished");

    expect(saveMetricExecutionSpy).toHaveBeenCalledWith([
      expect.objectContaining({name: "FailingLogic"}),
      expect.objectContaining({name: "SucceedingLogic"}),
    ]);
  });

  it("should distribute and cascade documents returned by hourly logics", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      dstPath: "users/user1",
      doc: {lastHourlySync: "2026-10-09T08:00:00Z"},
    };

    const mockHourlyFn = jest.fn().mockResolvedValue({
      name: "SyncLogic",
      status: "finished",
      documents: [doc1],
    } as LogicResult);

    const configs: HourlyLogicConfig[] = [
      {
        name: "SyncLogic",
        hourlyLogicFn: mockHourlyFn,
      },
    ];
    getHourlyLogicConfigsSpy.mockReturnValue(configs);

    const results = await hourlyLogics.runHourlyBusinessLogics(dummyEvent);

    expect(results.length).toBe(1);
    expect(distributeAndCascadeLogicResultDocsSpy).toHaveBeenCalledWith([doc1], "no_app_version", "no_app_version");
  });

  it("should call runHourlyBusinessLogics from hourlyBusinessLogics Cloud Function handler", async () => {
    const runHourlyBusinessLogicsSpy = jest.spyOn(hourlyLogics, "runHourlyBusinessLogics").mockResolvedValue([]);
    await hourlyLogics.hourlyBusinessLogics(dummyEvent);
    expect(runHourlyBusinessLogicsSpy).toHaveBeenCalledWith(dummyEvent);
  });
});
