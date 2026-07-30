import {ProjectConfig} from "../../types";
import * as admin from "firebase-admin";
import {initializeEmberFlow} from "../../index";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
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
initializeEmberFlow({
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs,
      validatorConfigs,
      logicConfigs: [],
      patchLogicConfigs: [],
    });

describe("pubsubUtils", () => {
  let docSetMock: jest.Mock;
  let docGetMock: jest.Mock;

  beforeEach(() => {
    docSetMock = jest.fn().mockResolvedValue({});
    docGetMock = jest.fn().mockResolvedValue({
      exists: true,
    });
    const dbDoc = ({
      set: docSetMock,
      get: docGetMock,
      id: "test-doc-id",
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should track processed ids", async () => {
    await pubsubUtils.trackProcessedIds("test-topic", "test-id");

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("@topics/test-topic/processedIds/test-id");
    expect(docSetMock).toHaveBeenCalledTimes(1);
    expect(docSetMock).toHaveBeenCalledWith({timestamp: expect.any(Date)});
  });

  it("should check if id is processed", async () => {
    const result = await pubsubUtils.isProcessed("test-topic", "test-id");

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("@topics/test-topic/processedIds/test-id");
    expect(docGetMock).toHaveBeenCalledTimes(1);
    expect(result).toBe(true);
  });
});
