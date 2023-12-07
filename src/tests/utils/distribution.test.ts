import {LogicResultDoc} from "../../types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {PubSub, Topic} from "@google-cloud/pubsub";
import * as distribution from "../../utils/distribution";
import * as indexUtils from "../../index-utils";
import {FOR_DISTRIBUTION_TOPIC_NAME} from "../../index";

jest.mock("../../index", () => {
  return {
    pubsub: new PubSub(),
    FOR_DISTRIBUTION_TOPIC_NAME: "for-distribution-queue",
  };
});

describe("queueForDistributionLater", () => {
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
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    await distribution.queueForDistributionLater(doc1);

    expect(topicSpy).toHaveBeenCalledWith(FOR_DISTRIBUTION_TOPIC_NAME);
    expect(publishMessageMock).toHaveBeenCalledWith({json: doc1});
  });
});

describe("onMessageForDistributionQueue", () => {
  let distributeDocSpy: jest.SpyInstance;
  let queueForDistributionLaterSpy: jest.SpyInstance;

  beforeEach(() => {
    distributeDocSpy = jest.spyOn(indexUtils, "distributeDoc").mockResolvedValue();
    queueForDistributionLaterSpy = jest.spyOn(distribution, "queueForDistributionLater").mockResolvedValue();
  });

  it("should distribute high priority doc", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "high",
      doc: {name: "test-doc-name-updated"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    const event = {
      data: {
        message: {
          json: doc1,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await distribution.onMessageForDistributionQueue(event);

    expect(distributeDocSpy).toHaveBeenCalledWith(doc1);
    expect(result).toEqual("Processed for distribution later");
  });

  it("should update priority from normal to high and queue for distribution", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    const event = {
      data: {
        message: {
          json: doc1,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await distribution.onMessageForDistributionQueue(event);

    expect(queueForDistributionLaterSpy).toHaveBeenCalledWith({...doc1, priority: "high"});
    expect(result).toEqual("Processed for distribution later");
  });
});
