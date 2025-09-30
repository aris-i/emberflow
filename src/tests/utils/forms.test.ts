import {FormData} from "emberflow-admin-client/lib/types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
const isProcessedMock = jest.fn();
const trackProcessedIdsMock = jest.fn();
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import * as adminClient from "emberflow-admin-client/lib";
import * as forms from "../../utils/forms";
import {initializeEmberFlow, SUBMIT_FORM_TOPIC, SUBMIT_FORM_TOPIC_NAME} from "../../index";
import {ProjectConfig} from "../../types";
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import spyOn = jest.spyOn;
import * as misc from "../../utils/misc";
import {firestore} from "firebase-admin";

jest.mock("../../utils/pubsub", () => {
  return {
    pubsubUtils: {
      isProcessed: isProcessedMock,
      trackProcessedIds: trackProcessedIdsMock,
    },
  };
});

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

describe("queueSubmitForm", () => {
  let publishMessageSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(SUBMIT_FORM_TOPIC, "publishMessage")
      .mockImplementation(() => {
        return "message-id";
      });
  });

  it("should queue docs for distribution later", async () => {
    const formData: FormData = {
      "@docPath": "users/test-uid",
      "@actionType": "create",
    };
    const result = await forms.queueSubmitForm(formData);

    expect(publishMessageSpy).toHaveBeenCalledWith({json: formData});
    expect(result).toEqual("message-id");
  });
});

describe("onMessageSubmitFormQueue", () => {
  let submitFormSpy: jest.SpyInstance;

  beforeEach(() => {
    submitFormSpy = jest.spyOn(adminClient, "submitForm").mockImplementation((formData: FormData) => Promise.resolve(formData));
  });

  it("should skip duplicate event", async () => {
    isProcessedMock.mockResolvedValueOnce(true);
    jest.spyOn(console, "log").mockImplementation();

    const formData: FormData = {
      "@docPath": "users/test-uid",
      "@actionType": "create",
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: formData,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await forms.onMessageSubmitFormQueue(event);

    expect(isProcessedMock).toHaveBeenCalledWith(SUBMIT_FORM_TOPIC_NAME, event.id);
    expect(console.log).toHaveBeenCalledWith("Skipping duplicate event");
  });

  it("should submit doc", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const submitFormAs = "test-user";
    const formData: FormData = {
      "@docPath": "users/test-uid",
      "@actionType": "create",
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            ...formData,
            "@submitFormAs": submitFormAs,
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await forms.onMessageSubmitFormQueue(event);

    expect(submitFormSpy).toHaveBeenCalledWith(formData, submitFormAs);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(SUBMIT_FORM_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed form data");
  });
});

describe("cleanActionsAndForms", () => {
  let deleteCollectionSpy: jest.SpyInstance;
  let formRefSpy: jest.SpyInstance;
  let formUpdateMock: jest.Mock;
  const snapshot = jest.fn();

  const actionSnapshot = {
    docs: [
      {
        ref: {
          path: "@actions/test-form-id-1",
        },
        data: () => ({
          eventContext: {
            formId: "test-form-id-1",
            uid: "test-uid-1",
          },
        }),
      },
      {
        ref: {
          path: "@actions/test-form-id-2",
        },
        data: () => ({
          eventContext: {
            formId: "test-form-id-2",
            uid: "test-uid-1",
          },
        }),
      },
      {
        ref: {
          path: "@actions/test-form-id-3",
        },
        data: () => ({
          eventContext: {
            formId: "test-form-id-3",
            uid: "test-uid-2",
          },
        }),
      },
    ],
  };

  const logicResultsSnapshot = {
    docs: [
      {
        ref: {
          path: "@actions/test-form-id-1/logicResults/test-form-id-1-0-0",
        },
      },
    ],
  };

  snapshot.mockReturnValue(logicResultsSnapshot).mockReturnValueOnce(actionSnapshot);

  beforeEach(() => {
    deleteCollectionSpy = jest.spyOn(misc, "deleteCollection")
      .mockImplementation(async (query, callback) => {
        if (callback) {
          await callback(snapshot() as unknown as firestore.QuerySnapshot);
        }
        return Promise.resolve();
      });

    formUpdateMock = jest.fn();
    formRefSpy = jest.spyOn(admin.database(), "ref").mockReturnValue({
      update: formUpdateMock,
    } as unknown as admin.database.Reference);
  });

  it("should clean forms", async () => {
    spyOn(console, "info").mockImplementation();
    const event = {} as ScheduledEvent;
    await forms.cleanActionsAndForms(event);

    expect(console.info).toHaveBeenCalledWith("Running cleanActionsAndForms");
    expect(deleteCollectionSpy).toHaveBeenCalled();
    expect(deleteCollectionSpy).toHaveBeenCalledTimes(7);
    expect(formRefSpy).toHaveBeenCalled();
    expect(formRefSpy).toHaveBeenCalledTimes(1);
    expect(formUpdateMock).toHaveBeenCalledWith({
      "forms/test-uid-1/test-form-id-1": null,
      "forms/test-uid-1/test-form-id-2": null,
      "forms/test-uid-2/test-form-id-3": null,
    });
    expect(formUpdateMock).toHaveBeenCalledTimes(1);
    expect(console.info).toHaveBeenCalledWith("Cleaned actions and forms");
  });
});
