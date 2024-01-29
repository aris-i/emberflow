import {LogicResultDoc, ProjectConfig} from "../../types";
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

  it("should log invalid instruction when parenthesis is not found", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "planets": "arr[Earth]",
              "continents": "arr{Asia}",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(console.log).toHaveBeenCalledWith("Invalid instruction arr[Earth] for property planets");
    expect(console.log).toHaveBeenCalledWith("Invalid instruction arr{Asia} for property continents");
    expect(docSetMock.mock.calls[0][0]).toStrictEqual({});
  });

  it("should log no values found when parenthesis is empty", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "planets": "arr()",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(console.log).toHaveBeenCalledWith("No values found in instruction arr() for property planets");
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
              "planets": "arr(+Earth)",
              "continents": "arr(Asia,Europe,Africa)",
              "countries": "arr(+Japan, Philippines, +Singapore)",
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
              "planets": "arr(-Earth)",
              "continents": "arr(-Asia,-Europe,-Africa)",
              "countries": "arr(-Japan, -Philippines, -Singapore)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(docSetMock.mock.calls[0][0]).toStrictEqual({});
    expect(docSetMock.mock.calls[1][0]).toStrictEqual(expectedData);
  });

  it("should convert array union and remove instructions correctly in a single field", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "planets": admin.firestore.FieldValue.arrayUnion("Earth", "Mars", "Venus"),
    };
    const expectedRemoveData = {
      "planets": admin.firestore.FieldValue.arrayRemove("Pluto"),
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "planets": "arr(+Earth,Mars,+Venus,-Pluto)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(docSetMock.mock.calls[0][0]).toStrictEqual(expectedData);
    expect(docSetMock.mock.calls[1][0]).toStrictEqual(expectedRemoveData);
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
      "optionalField": admin.firestore.FieldValue.delete(),
      "arrayUnion": admin.firestore.FieldValue.arrayUnion("add-this"),
    };
    const expectedRemoveData = {
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
              "optionalField": "del",
              "arrayUnion": "arr(+add-this)",
              "arrayRemove": "arr(-remove-this)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await distribution.onMessageInstructionsQueue(event);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(docSetMock).toHaveBeenCalledTimes(2);
    expect(docSetMock).toHaveBeenCalledWith(expectedData, {merge: true});
    expect(docSetMock).toHaveBeenCalledWith(expectedRemoveData, {merge: true});
    expect(docSetMock.mock.calls[0][0]).toStrictEqual(expectedData);
    expect(docSetMock.mock.calls[1][0]).toStrictEqual(expectedRemoveData);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed instructions");
  });
});
