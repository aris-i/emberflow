import {FormData} from "emberflow-admin-client/lib/types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {PubSub, Topic} from "@google-cloud/pubsub";
import * as adminClient from "emberflow-admin-client/lib";
import * as forms from "../../utils/forms";
import {SUBMIT_FORM_TOPIC_NAME} from "../../index";

jest.mock("../../index", () => {
  return {
    pubsub: new PubSub(),
    SUBMIT_FORM_TOPIC_NAME: "submit-form-queue",
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

  it("should submit doc", async () => {
    const formData: FormData = {
      "@docPath": "users/test-uid",
      "@actionType": "create",
    };
    const event = {
      data: {
        message: {
          json: formData,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await forms.onMessageSubmitFormQueue(event);

    expect(submitFormSpy).toHaveBeenCalledWith(formData);
    expect(result).toEqual("Processed form data");
  });
});
