import {Instructions, LogicResultDoc, ProjectConfig} from "../../types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
const isProcessedMock = jest.fn();
const trackProcessedIdsMock = jest.fn();
import * as distribution from "../../utils/distribution";
import * as indexUtils from "../../index-utils";
import {
  FOR_DISTRIBUTION_TOPIC,
  FOR_DISTRIBUTION_TOPIC_NAME,
  initializeEmberFlow,
  INSTRUCTIONS_TOPIC, INSTRUCTIONS_TOPIC_NAME,
} from "../../index";
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfig} from "../../sample-custom/security";
import {validatorConfig} from "../../sample-custom/validators";
import {PubSub, Subscription} from "@google-cloud/pubsub";

jest.mock("../../utils/pubsub", () => {
  return {
    pubsubUtils: {
      isProcessed: isProcessedMock,
      trackProcessedIds: trackProcessedIdsMock,
    },
    createPubSubTopics: jest.fn().mockResolvedValue({}),
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
initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, []);

describe("queueForDistributionLater", () => {
  let publishMessageSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(FOR_DISTRIBUTION_TOPIC, "publishMessage")
      .mockImplementation(() => {
        return "message-id";
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

    expect(publishMessageSpy).toHaveBeenCalledWith({json: doc1});
  });
});

describe("onMessageForDistributionQueue", () => {
  let distributeDocSpy: jest.SpyInstance;
  let queueForDistributionLaterSpy: jest.SpyInstance;

  beforeEach(() => {
    distributeDocSpy = jest.spyOn(indexUtils, "distributeDoc").mockResolvedValue();
    queueForDistributionLaterSpy = jest.spyOn(distribution, "queueForDistributionLater").mockResolvedValue();
  });

  it("should skip duplicate message", async () => {
    isProcessedMock.mockResolvedValueOnce(true);
    jest.spyOn(console, "log").mockImplementation();
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
    await distribution.onMessageForDistributionQueue(event);

    expect(isProcessedMock).toHaveBeenCalledWith(FOR_DISTRIBUTION_TOPIC_NAME, event.id);
    expect(console.log).toHaveBeenCalledWith("Skipping duplicate message");
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
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(FOR_DISTRIBUTION_TOPIC_NAME, event.id);
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
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(FOR_DISTRIBUTION_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed for distribution later");
  });

  it("should update priority from low to normal and queue for distribution", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "low",
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

    expect(queueForDistributionLaterSpy).toHaveBeenCalledWith({...doc1, priority: "normal"});
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(FOR_DISTRIBUTION_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed for distribution later");
  });
});

describe("queueInstructions", () => {
  let publishMessageSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(INSTRUCTIONS_TOPIC, "publishMessage")
      .mockImplementation(() => {
        return "message-id";
      });
  });

  it("should queue instructions", async () => {
    const dstPath = "/users/test-user-id/documents/test-doc-id";
    const instructions = {
      "count": "++",
      "score": "+5",
      "minusCount": "--",
      "minusScore": "-3",
    };
    await distribution.queueInstructions(dstPath, instructions);

    expect(publishMessageSpy).toHaveBeenCalledWith({json: {dstPath, instructions}});
  });
});

describe("onMessageInstructionsQueue", () => {
  let dbSpy: jest.SpyInstance;
  let docSetMock: jest.Mock;

  beforeEach(() => {
    docSetMock = jest.fn().mockResolvedValue({});
    const dbDoc = ({
      set: docSetMock,
      id: "test-doc-id",
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);
    jest.spyOn(console, "log").mockImplementation();
  });

  afterEach(() => {
    dbSpy.mockRestore();
  });

  it("should log invalid operation", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "planets": "arr~(Earth)",
              "continents": "arr-[Asia]",
              "countries": "arr+[Philippines]",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(console.log).toHaveBeenCalledWith("Invalid instruction arr~(Earth) for property planets");
    expect(console.log).toHaveBeenCalledWith("Invalid instruction arr-[Asia] for property continents");
    expect(console.log).toHaveBeenCalledWith("Invalid instruction arr+[Philippines] for property countries");
    expect(docSetMock.mock.calls[0][0]).toStrictEqual({});
  });

  it("should log no values found", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "continents": "arr-()",
              "countries": "arr+()",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(console.log).toHaveBeenCalledWith("No values found in instruction arr-() for property continents");
    expect(console.log).toHaveBeenCalledWith("No values found in instruction arr+() for property countries");
    expect(docSetMock.mock.calls[0][0]).toStrictEqual({});
  });

  it("should convert array union instructions correctly", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "planets": admin.firestore.FieldValue.arrayUnion("Earth"),
      "continents": admin.firestore.FieldValue.arrayUnion("Asia", "Europe", "Africa"),
      "countries": admin.firestore.FieldValue.arrayUnion("Japan", "Philippines", "Singapore"),
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "planets": "arr+(Earth)",
              "continents": "arr+(Asia,Europe,Africa)",
              "countries": "arr+(Japan, Philippines, Singapore)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(docSetMock.mock.calls[0][0]).toStrictEqual(expectedData);
  });

  it("should convert array remove instructions correctly", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "planets": admin.firestore.FieldValue.arrayRemove("Earth"),
      "continents": admin.firestore.FieldValue.arrayRemove("Asia", "Europe", "Africa"),
      "countries": admin.firestore.FieldValue.arrayRemove("Japan", "Philippines", "Singapore"),
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "planets": "arr-(Earth)",
              "continents": "arr-(Asia,Europe,Africa)",
              "countries": "arr-(Japan, Philippines, Singapore)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(docSetMock.mock.calls[0][0]).toStrictEqual(expectedData);
  });

  it("should skip duplicate message", async () => {
    isProcessedMock.mockResolvedValueOnce(true);
    jest.spyOn(console, "log").mockImplementation();
    const event = {
      id: "test-event",
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(isProcessedMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
    expect(console.log).toHaveBeenCalledWith("Skipping duplicate message");
  });

  it("should process instructions correctly", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "count": admin.firestore.FieldValue.increment(1),
      "score": admin.firestore.FieldValue.increment(5),
      "minusCount": admin.firestore.FieldValue.increment(-1),
      "minusScore": admin.firestore.FieldValue.increment(-3),
      "arrayUnion": admin.firestore.FieldValue.arrayUnion("add-this"),
      "arrayRemove": admin.firestore.FieldValue.arrayRemove("remove-this"),
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "count": "++",
              "score": "+5",
              "minusCount": "--",
              "minusScore": "-3",
              "arrayUnion": "arr+(add-this)",
              "arrayRemove": "arr-(remove-this)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await distribution.onMessageInstructionsQueue(event);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(docSetMock).toHaveBeenCalledTimes(1);
    expect(docSetMock).toHaveBeenCalledWith(expectedData, {merge: true});
    expect(docSetMock.mock.calls[0][0]).toStrictEqual(expectedData);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed instructions");
  });
});

describe("mergeInstructions", () => {
  beforeEach(() => {
    jest.spyOn(console, "warn").mockImplementation();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should merge values correctly", () => {
    const instructions1 = {
      "count": "++",
      "score": "+5",
      "minusCount": "--",
      "minusScore": "-3",
    };
    const instructions2 = {
      "count": "--",
      "score": "-5",
      "minusCount": "++",
      "minusScore": "+3",
    };
    const instructions3 = {
      "count": "-1",
      "score": "-5",
      "minusCount": "+1",
      "minusScore": "+3",
    };
    const existingInstructions = {};

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "count": "++",
      "score": "+5",
      "minusCount": "--",
      "minusScore": "-3",
    });

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "count": "+2",
      "score": "+10",
      "minusCount": "-2",
      "minusScore": "-6",
    });

    distribution.mergeInstructions(existingInstructions, instructions2);
    expect(existingInstructions).toStrictEqual({
      "count": "+1",
      "score": "+5",
      "minusCount": "-1",
      "minusScore": "-3",
    });

    distribution.mergeInstructions(existingInstructions, instructions3);
    expect(existingInstructions).toStrictEqual({});
  });

  it("should merge array values correctly", () => {
    const instructions1 = {
      "planets": "arr(+Earth,+Mars,+Pluto)",
    };
    const instructions2 = {
      "planets": "arr(-Pluto)",
    };
    const existingInstructions = {};

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "planets": "arr(+Earth,+Mars,+Pluto)",
    });

    distribution.mergeInstructions(existingInstructions, instructions2);
    expect(existingInstructions).toStrictEqual({
      "planets": "arr(+Earth,+Mars)",
    });
  });

  it("should override instruction with del", () => {
    const instructions1 = {
      "count": "++",
    };
    const instructions2 = {
      "count": "del",
    };
    const existingInstructions = {};

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "count": "++",
    });
    distribution.mergeInstructions(existingInstructions, instructions2);
    expect(existingInstructions).toStrictEqual({
      "count": "del",
    });
  });

  it("should warn when existing instruction is del", () => {
    const instructions1 = {
      "count": "del",
    };
    const instructions2 = {
      "count": "++",
    };
    const existingInstructions = {};

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "count": "del",
    });
    distribution.mergeInstructions(existingInstructions, instructions2);
    expect(console.warn).toHaveBeenCalledWith("Property count is set to be deleted. Skipping..");
  });

  it("should warn when instructions has conflicts", () => {
    const instructions1 = {
      "count": "++",
    };
    const instructions2 = {
      "count": "arr(+value)",
    };
    const existingInstructions = {};

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "count": "++",
    });
    distribution.mergeInstructions(existingInstructions, instructions2);
    expect(console.warn).toHaveBeenCalledWith("Property count has conflicting instructions ++ and arr(+value). Skipping..");
  });
});

describe("reduceInstructions", () => {
  let queueInstructionsSpy: jest.SpyInstance;
  let closeMock: jest.Mock;
  let onMock: jest.Mock;
  let rejectMock: jest.Mock;
  let resolveMock: jest.Mock;

  beforeEach(() => {
    jest.spyOn(console, "log").mockImplementation();
    jest.spyOn(console, "error").mockImplementation();
    rejectMock = jest.fn();
    resolveMock = jest.fn();
    queueInstructionsSpy = jest.spyOn(distribution, "queueInstructions");
    closeMock = jest.fn();
    onMock = jest.fn();
    jest.spyOn(PubSub.prototype, "subscription").mockImplementation(() => {
      return {
        close: closeMock,
        on: onMock,
      } as unknown as Subscription;
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should resolve when time is up", async () => {
    jest.spyOn(global, "Promise").mockImplementation((callback) => {
      callback(resolveMock, rejectMock);
      return {} as unknown as Promise<Map<string, Instructions>>;
    });
    jest.spyOn(global, "setTimeout").mockImplementation((callback) => {
      callback();
      return 1 as unknown as NodeJS.Timeout;
    });

    await distribution.reduceInstructions();
    expect(onMock).toHaveBeenCalledWith("message", expect.any(Function));
    expect(closeMock).toHaveBeenCalledTimes(1);
    expect(console.log).toHaveBeenCalledWith("Time is up, stopping message reception");
    expect(resolveMock).toHaveBeenCalledTimes(1);
  });

  it("should reject when received an error", async () => {
    jest.spyOn(global, "Promise").mockImplementation((callback) => {
      callback(resolveMock, rejectMock);
      return {} as unknown as Promise<Map<string, Instructions>>;
    });
    const clearTimeoutSpy = jest.spyOn(global, "clearTimeout").mockImplementation();
    const error = new Error("test error");
    onMock.mockImplementation((event, callback) => {
      if (event === "error") {
        callback(error);
      }
    });
    await distribution.reduceInstructions();
    expect(onMock).toHaveBeenCalledWith("error", expect.any(Function));
    expect(clearTimeoutSpy).toHaveBeenCalledTimes(1);
    expect(console.error).toHaveBeenCalledWith(`Received error: ${error}`);
    expect(rejectMock).toHaveBeenCalledTimes(1);
  });

  it("should queue instructions", async () => {
    jest.spyOn(global, "Promise").mockImplementation((callback) => {
      callback(resolveMock, rejectMock);
      return resolveMock.mock.calls[0][0] as unknown as Promise<Map<string, Instructions>>;
    });
    jest.spyOn(global, "setTimeout").mockImplementation((callback) => {
      callback();
      return 1 as unknown as NodeJS.Timeout;
    });
    const ackMock = jest.fn();
    const data1 = {
      dstPath: "/users/test-user-id/documents/test-doc-id",
      instructions: {
        "count": "++",
      },
    };
    const message1 = {
      ack: ackMock,
      data: Buffer.from(JSON.stringify(data1)),
      id: "message1",
    };
    const data2 = {
      dstPath: "/users/test-user-id/documents/test-doc-id",
      instructions: {
        "count": "+2",
      },
    };
    const message2 = {
      ack: ackMock,
      data: Buffer.from(JSON.stringify(data2)),
      id: "message2",
    };
    onMock.mockImplementation((event, callback) => {
      if (event === "message") {
        callback(message1);
        callback(message2);
      }
    });

    await distribution.reduceInstructions();
    expect(console.log).toHaveBeenCalledWith("Received message message1.");
    expect(console.log).toHaveBeenCalledWith("Received message message2.");
    expect(ackMock).toHaveBeenCalledTimes(2);
    expect(console.log).toHaveBeenCalledWith("Received 1 messages within 3 seconds.");
    expect(queueInstructionsSpy).toHaveBeenCalledTimes(1);
    expect(queueInstructionsSpy).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id", {
      "count": "+3",
    });
  });
});
