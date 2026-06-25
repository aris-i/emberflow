import {LogicResult, LogicResultDoc} from "../types";
import {pubsubUtils} from "../utils/pubsub";

jest.mock("../utils/pubsub", () => ({
  pubsubUtils: {
    isProcessed: jest.fn(),
    trackProcessedIds: jest.fn(),
  },
}));

import * as viewLogics from "../logics/view-logics";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import type {MessagePublishedData} from "firebase-functions/v2/pubsub";
import * as indexUtils from "../index-utils";
import {firestore} from "firebase-admin";

jest.mock("../index", () => ({
  db: {
    collection: jest.fn().mockReturnThis(),
    doc: jest.fn().mockReturnThis(),
    batch: jest.fn().mockReturnValue({
      set: jest.fn(),
      commit: jest.fn().mockResolvedValue(null),
    }),
  },
  VIEW_LOGICS_TOPIC_NAME: "view-logics",
}));

describe("onMessageViewLogicsQueue Error Handling", () => {
  let runViewLogicsSpy: jest.SpyInstance;

  const doc1: LogicResultDoc = {
    action: "merge",
    priority: "normal",
    doc: {name: "test"},
    dstPath: "users/doc1",
  };
  const targetVersion = "1.0.0";
  const appVersion = "1.0.0";

  const eventWithNoJson = {
    id: "event-id",
    data: {
      message: {
        json: null,
      },
    },
  } as CloudEvent<MessagePublishedData>;

  const eventWithData = {
    id: "event-id-2",
    data: {
      message: {
        json: {
          doc: doc1,
          targetVersion,
          appVersion,
        },
      },
    },
  } as CloudEvent<MessagePublishedData>;

  beforeEach(() => {
    jest.clearAllMocks();
    runViewLogicsSpy = jest.spyOn(viewLogics, "runViewLogics");
    (pubsubUtils.isProcessed as jest.Mock).mockResolvedValue(false);
  });

  afterEach(() => {
    runViewLogicsSpy.mockRestore();
  });

  it("should throw 'No json in message' when json is missing", async () => {
    await expect(viewLogics.onMessageViewLogicsQueue(eventWithNoJson)).rejects.toThrow("No json in message");
  });

  it("should handle runViewLogics returning a result with undefined message", async () => {
    const resultWithUndefinedMessage: LogicResult = {
      name: "test-logic",
      status: "error",
      message: undefined as any,
      documents: [],
      execTime: 10,
      timeFinished: firestore.Timestamp.now(),
    };

    runViewLogicsSpy.mockResolvedValue([resultWithUndefinedMessage]);

    // Mock expand and distribute to avoid unnecessary work
    jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(new Map());
    jest.spyOn(indexUtils, "distributeFnNonTransactional").mockResolvedValue([]);
    jest.spyOn(indexUtils._mockable, "saveMetricExecution").mockResolvedValue();

    // This should not throw Firestore validation error because we handle undefined message
    await viewLogics.onMessageViewLogicsQueue(eventWithData);

    // If we reached here, it didn't crash.
  });
});
