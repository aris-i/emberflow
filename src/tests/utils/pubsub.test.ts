import {ProjectConfig} from "../../types";
import * as admin from "firebase-admin";
import {initializeEmberFlow} from "../../index";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfig} from "../../sample-custom/security";
import {validatorConfig} from "../../sample-custom/validators";
import {cleanPubSubProcessedIds, pubsubUtils} from "../../utils/pubsub";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import * as misc from "../../utils/misc";

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

describe("pubsubUtils", () => {
  let colGetMock: jest.Mock;
  let deleteCollectionSpy: jest.SpyInstance;
  let docSetMock: jest.Mock;
  let docGetMock: jest.Mock;

  beforeEach(() => {
    colGetMock = jest.fn().mockResolvedValue({
      docs: [
        {
          ref: {
            collection: jest.fn().mockReturnValue({
              where: jest.fn().mockReturnValue({}),
            }),
          },
        },
      ],
    });
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
    jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      get: colGetMock,
    } as any);
    deleteCollectionSpy = jest.spyOn(misc, "deleteCollection").mockImplementation(() => Promise.resolve());
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

  it("should clean processed ids", async () => {
    jest.spyOn(console, "info").mockImplementation();
    await cleanPubSubProcessedIds({} as ScheduledEvent);

    expect(console.info).toHaveBeenCalledWith("Running cleanPubSubProcessedIds");
    expect(admin.firestore().collection).toHaveBeenCalledTimes(1);
    expect(admin.firestore().collection).toHaveBeenCalledWith("@topics");
    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(deleteCollectionSpy).toHaveBeenCalled();
    expect(console.info).toHaveBeenCalledWith("Cleaned 1 topics of processedIds");
  });
});
