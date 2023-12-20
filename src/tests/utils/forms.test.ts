import {FormData} from "emberflow-admin-client/lib/types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
const isProcessedMock = jest.fn();
const trackProcessedIdsMock = jest.fn();
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {PubSub, Topic} from "@google-cloud/pubsub";
import * as adminClient from "emberflow-admin-client/lib";
import * as forms from "../../utils/forms";
import {initializeEmberFlow, SUBMIT_FORM_TOPIC_NAME} from "../../index";
import {ProjectConfig} from "../../types";
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfig} from "../../sample-custom/security";
import {validatorConfig} from "../../sample-custom/validators";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import spyOn = jest.spyOn;
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
jest.mock("../../utils/pubsub", () => {
  return {
    pubsubUtils: {
      isProcessed: isProcessedMock,
      trackProcessedIds: trackProcessedIdsMock,
    },
  };
});

describe("queueSubmitForm", () => {
  let publishMessageMock: jest.Mock;
  let topicSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageMock = jest.fn().mockResolvedValue("message-id");
    topicSpy = jest.spyOn(PubSub.prototype, "topic").mockImplementation(() => {
      return {
        publishMessage: publishMessageMock,
      } as unknown as Topic;
    });
  });

  it("should queue docs for distribution later", async () => {
    const formData: FormData = {
      "@docPath": "users/test-uid",
      "@actionType": "create",
    };
    const result = await forms.queueSubmitForm(formData);

    expect(topicSpy).toHaveBeenCalledWith(SUBMIT_FORM_TOPIC_NAME);
    expect(publishMessageMock).toHaveBeenCalledWith({json: formData});
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
    const result = await forms.onMessageSubmitFormQueue(event);

    expect(submitFormSpy).toHaveBeenCalledWith(formData);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(SUBMIT_FORM_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed form data");
  });
});

describe("cleanActionsAndForms", () => {
  let deleteActionCollectionSpy: jest.SpyInstance;
  beforeEach(() => {
    deleteActionCollectionSpy = jest.spyOn(misc, "deleteActionCollection")
      .mockImplementation(() => Promise.resolve());
  });

  it("should clean forms", async () => {
    spyOn(console, "info").mockImplementation();
    const event = {} as ScheduledEvent;
    await forms.cleanActionsAndForms(event);

    expect(console.info).toHaveBeenCalledWith("Running cleanActionsAndForms");
    expect(deleteActionCollectionSpy).toHaveBeenCalled();
    expect(console.info).toHaveBeenCalledWith("Cleaned actions and forms");
  });
});
