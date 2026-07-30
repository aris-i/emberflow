import {FormData} from "emberflow-admin-client/lib/types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
const isProcessedMock = jest.fn();
const trackProcessedIdsMock = jest.fn();
import type {MessagePublishedData} from "firebase-functions/v2/pubsub";
import * as adminClient from "emberflow-admin-client/lib";
import * as forms from "../../utils/forms";
import {initializeEmberFlow, SUBMIT_FORM_TOPIC, SUBMIT_FORM_TOPIC_NAME} from "../../index";
import {ProjectConfig} from "../../types";
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";

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
      "@appVersion": "0.0.1",
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
  });

  it("should submit doc", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const submitFormAs = "test-user";
    const appVersion = "0.0.1";
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
            "@appVersion": appVersion,
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await forms.onMessageSubmitFormQueue(event);

    expect(submitFormSpy).toHaveBeenCalledWith(formData, {uid: submitFormAs, appVersion});
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(SUBMIT_FORM_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed form data");
  });
});
