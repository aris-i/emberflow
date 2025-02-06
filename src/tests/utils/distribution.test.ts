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

jest.mock("../../utils/pubsub", () => {
  return {
    pubsubUtils: {
      isProcessed: isProcessedMock,
      trackProcessedIds: trackProcessedIdsMock,
    },
  };
});
const transactionUpdateMock = jest.fn();
const transactionSetMock = jest.fn();
const transactionGetMock = jest.fn();
const transactionMock = {
  get: transactionGetMock,
  update: transactionUpdateMock,
  set: transactionSetMock,
} as unknown as admin.firestore.Transaction;

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
      id: "test-event",
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
      id: "test-event",
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
      id: "test-event",
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
      id: "test-event",
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

describe("convertInstructionsToDbValues", () => {
  beforeEach(() => {
    const queueNumberCounterDoc = {
      "id": "queueNumber",
      "data": () => {
        return {
          "count": 10,
          "lastUpdatedAt": admin.firestore.Timestamp.now(),
        };
      },
    };
    transactionGetMock.mockResolvedValue(queueNumberCounterDoc);
    jest.spyOn(admin.firestore(), "runTransaction").mockImplementation(async (transactionFn) => {
      return transactionFn(transactionMock);
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe("Global counter instruction", () => {
    it("should convert global counter instructions to db values correctly", async () => {
      const instructions = {
        "queueNumber": "globalCounter(queueNumber,20)",
      };
      const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions);

      expect(transactionUpdateMock).toHaveBeenCalledTimes(1);
      expect(result.updateData).toStrictEqual({
        "queueNumber": 11,
      });
    });

    it("should initiate and create the counter document if it is not existing", async () => {
      transactionGetMock.mockResolvedValueOnce(undefined);
      const instructions = {
        "newCounter": "globalCounter(newCounter,20)",
      };
      const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions);

      expect(transactionSetMock).toHaveBeenCalledTimes(1);
      expect(result.updateData).toStrictEqual({
        "newCounter": 1,
      });
    });

    it("should reset the counter to 1 if max value has been reached", async () => {
      transactionGetMock.mockResolvedValueOnce({
        "id": "queueNumber",
        "data": () => {
          return {
            "count": 20,
            "lastUpdatedAt": admin.firestore.Timestamp.now(),
          };
        },
      });
      const instructions = {
        "queueNumber": "globalCounter(queueNumber,20)",
      };
      const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions);

      expect(result.updateData).toStrictEqual({
        "queueNumber": 1,
      });
    });

    it("should convert global counter instructions to db values correctly even without max value provided", async () => {
      transactionGetMock.mockResolvedValueOnce({
        "id": "queueNumber",
        "data": () => {
          return {
            "count": 10,
            "lastUpdatedAt": admin.firestore.Timestamp.now(),
          };
        },
      });
      const instructions = {
        "queueNumber": "globalCounter(queueNumber)",
      };
      const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions);

      expect(result.updateData).toStrictEqual({
        "queueNumber": 11,
      });
    });
  });
});

describe("onMessageInstructionsQueue", () => {
  let dbSpy: jest.SpyInstance;
  let docUpdateMock: jest.Mock;
  const transactionSetMock = jest.fn();
  const transactionUpdateMock = jest.fn();
  const transactionGetMock = jest.fn();

  beforeEach(() => {
    docUpdateMock = jest.fn().mockResolvedValue({});
    const dbDoc = ({
      update: docUpdateMock,
      id: "test-doc-id",
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    const queueDocRef = ({
      id: "queueNumber",
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockImplementation((docPath)=> {
      const docId = docPath.split("/").pop();
      if (docId === "queueNumber") {
        return queueDocRef;
      } else {
        return dbDoc;
      }
    });
    jest.spyOn(console, "log").mockImplementation();

    const queueNumberCounterDoc = {
      "id": "queueNumber",
      "data": () => {
        return {
          "count": 10,
          "lastUpdatedAt": admin.firestore.Timestamp.now(),
        };
      },
    };
    transactionGetMock.mockResolvedValue(queueNumberCounterDoc);
    jest.spyOn(admin.firestore(), "runTransaction").mockImplementation(async (transactionFn) => {
      const transaction = {
        get: transactionGetMock,
        update: transactionUpdateMock,
        set: transactionSetMock,
      } as unknown as admin.firestore.Transaction;

      return transactionFn(transaction);
    });
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
    expect(docUpdateMock).not.toHaveBeenCalled();
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
    expect(docUpdateMock).not.toHaveBeenCalled();
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

    expect(docUpdateMock.mock.calls[0][0]).toStrictEqual(expectedData);
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

    expect(docUpdateMock).toHaveBeenCalledTimes(1);
    expect(docUpdateMock.mock.calls[0][0]).toStrictEqual(expectedData);
  });

  it("should convert global counter instruction correctly", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "queueNumber": 11,
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id",
            instructions: {
              "queueNumber": "globalCounter(queueNumber,20)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(docUpdateMock).toHaveBeenCalledTimes(1);
    expect(docUpdateMock.mock.calls[0][0]).toStrictEqual(expectedData);
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

    expect(docUpdateMock.mock.calls[0][0]).toStrictEqual(expectedData);
    expect(docUpdateMock.mock.calls[1][0]).toStrictEqual(expectedRemoveData);
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

  it("should process event instructions correctly", async () => {
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "count": admin.firestore.FieldValue.increment(1),
      "score": admin.firestore.FieldValue.increment(5),
      "minusCount": admin.firestore.FieldValue.increment(-1),
      "minusScore": admin.firestore.FieldValue.increment(-3),
      "optionalField": admin.firestore.FieldValue.delete(),
      "arrayUnion": admin.firestore.FieldValue.arrayUnion("add-this"),
      "queueNumber": 11,
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
              "queueNumber": "globalCounter(queueNumber,20)",
            },
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageInstructionsQueue(event);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(2);
    expect(admin.firestore().doc).toHaveBeenNthCalledWith(1, "@counters/queueNumber");
    expect(admin.firestore().doc).toHaveBeenNthCalledWith(2, "/users/test-user-id/documents/test-doc-id");
    expect(docUpdateMock).toHaveBeenCalledTimes(2);
    expect(docUpdateMock).toHaveBeenCalledWith(expectedData);
    expect(docUpdateMock).toHaveBeenCalledWith(expectedRemoveData);
    expect(docUpdateMock.mock.calls[0][0]).toStrictEqual(expectedData);
    expect(docUpdateMock.mock.calls[1][0]).toStrictEqual(expectedRemoveData);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
  });

  it("should process map instructions correctly", async () => {
    const expectedData = {
      "count": admin.firestore.FieldValue.increment(1),
      "score": admin.firestore.FieldValue.increment(5),
      "minusCount": admin.firestore.FieldValue.increment(-1),
      "minusScore": admin.firestore.FieldValue.increment(-3),
      "optionalField": admin.firestore.FieldValue.delete(),
      "arrayUnion": admin.firestore.FieldValue.arrayUnion("add-this"),
      "queueNumber": 11,
    };
    const expectedRemoveData = {
      "arrayRemove": admin.firestore.FieldValue.arrayRemove("remove-this"),
    };
    const instructions: Map<string, Instructions> = new Map();
    instructions.set("/users/test-user-id/documents/test-doc-id", {
      "count": "++",
      "score": "+5",
      "minusCount": "--",
      "minusScore": "-3",
      "optionalField": "del",
      "arrayUnion": "arr(+add-this)",
      "arrayRemove": "arr(-remove-this)",
      "queueNumber": "globalCounter(queueNumber,20)",
    });

    await distribution.onMessageInstructionsQueue(instructions);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(2);
    expect(admin.firestore().doc).toHaveBeenNthCalledWith(1, "@counters/queueNumber");
    expect(admin.firestore().doc).toHaveBeenNthCalledWith(2, "/users/test-user-id/documents/test-doc-id");
    expect(docUpdateMock).toHaveBeenCalledTimes(2);
    expect(docUpdateMock).toHaveBeenCalledWith(expectedData);
    expect(docUpdateMock).toHaveBeenCalledWith(expectedRemoveData);
    expect(docUpdateMock.mock.calls[0][0]).toStrictEqual(expectedData);
    expect(docUpdateMock.mock.calls[1][0]).toStrictEqual(expectedRemoveData);
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

describe("instructionsReducer", () => {
  let mergeInstructionsSpy: jest.SpyInstance;

  beforeEach(() => {
    mergeInstructionsSpy = jest.spyOn(distribution, "mergeInstructions");
  });

  it("should skip duplicate message", async () => {
    isProcessedMock.mockResolvedValueOnce(true);
    jest.spyOn(console, "log").mockImplementation();
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "high",
      instructions: {"sample": "++"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: doc1,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const reducedInstructions: Map<string, Instructions> = new Map();
    await distribution.instructionsReducer(reducedInstructions, event);

    expect(isProcessedMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
    expect(console.log).toHaveBeenCalledWith("Skipping duplicate message");
  });

  it("should reduce instructions", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "high",
      instructions: {"sample": "++"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: doc1,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const reducedInstructions: Map<string, Instructions> = new Map();
    await distribution.instructionsReducer(reducedInstructions, event);

    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
    expect(reducedInstructions.get(doc1.dstPath)).toStrictEqual(doc1.instructions);
  });

  it("should merge instructions", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      instructions: {"sample": "++"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: doc1,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const reducedInstructions: Map<string, Instructions> = new Map();
    const existingInstructions = {"sample": "++"};
    reducedInstructions.set(doc1.dstPath, existingInstructions);
    const expectedReducedInstructions = {"sample": "+2"};
    await distribution.instructionsReducer(reducedInstructions, event);

    expect(mergeInstructionsSpy).toHaveBeenCalledWith(existingInstructions, doc1.instructions);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
    expect(reducedInstructions.get(doc1.dstPath)).toStrictEqual(expectedReducedInstructions);
  });
});
