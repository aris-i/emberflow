import {Instructions, LogicResultDoc, ProjectConfig} from "../../types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import type {MessagePublishedData} from "firebase-functions/v2/pubsub";
const isProcessedMock = jest.fn();
const trackProcessedIdsMock = jest.fn();
import * as distribution from "../../utils/distribution";
import * as indexUtils from "../../index-utils";
import {
  db,
  FOR_DISTRIBUTION_TOPIC,
  FOR_DISTRIBUTION_TOPIC_NAME,
  GROUP_PATCH_TOPIC,
  GROUP_PATCH_TOPIC_NAME,
  initializeEmberFlow,
  INSTRUCTIONS_TOPIC, INSTRUCTIONS_TOPIC_NAME,
} from "../../index";
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import {getDestPropAndDestPropId} from "../../utils/paths";
import {firestore} from "firebase-admin";
import FieldValue = firestore.FieldValue;
import * as viewLogics from "../../logics/view-logics";
import * as patchLogics from "../../logics/patch-logics";
import * as paths from "../../utils/paths";

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
jest.spyOn(paths._mockable, "doesPathExists").mockResolvedValue(true);
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

describe("queueForDistributionLater", () => {
  let publishMessageSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(FOR_DISTRIBUTION_TOPIC, "publishMessage")
      .mockImplementation(() => {
        return "message-id";
      });
  });
  const targetVersion = "1.0.0";
  const appVersion = "1.0.0";

  it("should queue docs for distribution later", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    await distribution.queueForDistributionLater(appVersion, targetVersion, doc1);

    expect(publishMessageSpy).toHaveBeenCalledWith({json: {doc: doc1, targetVersion, appVersion}});
  });
});

describe("onMessageForDistributionQueue", () => {
  let distributeDocSpy: jest.SpyInstance;
  let queueForDistributionLaterSpy: jest.SpyInstance;
  let queueRunViewLogicsSpy: jest.SpyInstance;
  let queueRunPatchLogicsSpy: jest.SpyInstance;
  beforeEach(() => {
    distributeDocSpy = jest.spyOn(indexUtils, "distributeDoc").mockResolvedValue();
    queueForDistributionLaterSpy = jest.spyOn(distribution, "queueForDistributionLater").mockResolvedValue();
    queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
    queueRunPatchLogicsSpy = jest.spyOn(patchLogics, "queueRunPatchLogics").mockResolvedValue();
    jest.spyOn(viewLogics, "findMatchingViewLogics").mockReturnValue(new Map([["test", {} as any]]));
    jest.spyOn(patchLogics, "findMatchingPatchLogicsByEntity").mockReturnValue([{} as any]);
    jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({entity: "test-entity", regex: /test/});
  });
  const targetVersion = "1.0.0";
  const appVersion = "1.0.0";

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
          json: {
            doc: doc1,
            targetVersion,
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    await distribution.onMessageForDistributionQueue(event);

    expect(isProcessedMock).toHaveBeenCalledWith(FOR_DISTRIBUTION_TOPIC_NAME, event.id);
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
          json: {
            doc: doc1,
            targetVersion,
            appVersion,
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await distribution.onMessageForDistributionQueue(event);

    expect(distributeDocSpy).toHaveBeenCalledWith(doc1, appVersion);
    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith(targetVersion, appVersion, [doc1]);
    expect(queueRunPatchLogicsSpy).toHaveBeenCalledWith(appVersion, doc1.dstPath);
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
          json: {
            doc: doc1,
            targetVersion,
            appVersion,
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await distribution.onMessageForDistributionQueue(event);

    expect(queueForDistributionLaterSpy).toHaveBeenCalledWith(appVersion, targetVersion, {...doc1, priority: "high"});
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
          json: {
            doc: doc1,
            targetVersion,
            appVersion,
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await distribution.onMessageForDistributionQueue(event);

    expect(queueForDistributionLaterSpy).toHaveBeenCalledWith(appVersion, targetVersion, {...doc1, priority: "normal"});
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

  describe("Nested instructions", () => {
    it("should handle nested instructions correctly", async () => {
      const instructions = {
        "user": {
          "score": "++",
          "counters": {
            "games": "+5",
            "wins": "--",
          },
        },
      };
      const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions);

      expect(result.updateData).toStrictEqual({
        "user.score": FieldValue.increment(1),
        "user.counters.games": FieldValue.increment(5),
        "user.counters.wins": FieldValue.increment(-1),
      });
      expect(result.removeData).toStrictEqual({});
    });

    it("should handle mixed nested instructions and array operations", async () => {
      const instructions = {
        "tags": "arr(+tag1, -tag2)",
        "meta": {
          "items": "arr(+item1)",
          "count": "++",
        },
      };
      const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions);

      expect(result.updateData).toStrictEqual({
        "tags": FieldValue.arrayUnion("tag1"),
        "meta.items": FieldValue.arrayUnion("item1"),
        "meta.count": FieldValue.increment(1),
      });
      expect(result.removeData).toStrictEqual({
        "tags": FieldValue.arrayRemove("tag2"),
      });
    });
  });

  it("should add parsed instructions to destProp if has destProp", async () => {
    const dstPath = "/users/test-user-id/documents/test-doc-id#counters";
    const {destProp, destPropId} = getDestPropAndDestPropId(dstPath);

    const instructions = {
      "salesCount": "+12.6",
      "staffCount": "-1",
    };
    const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions, destProp, destPropId);

    const expectedInstructions = {
      "counters": {
        "salesCount": FieldValue.increment(12.6),
        "staffCount": FieldValue.increment(-1),
      },
    };

    expect(result.updateData).toEqual(expectedInstructions);
    expect(result.removeData).toEqual({});
  });

  it("should add parsed instructions to destPropId if has destPropId", async () => {
    const dstPath = "/users/test-user-id/documents/test-doc-id#counters[nestedCount]";
    const {destProp, destPropId} = getDestPropAndDestPropId(dstPath);

    const instructions = {
      "salesCount": "+12.6",
      "staffCount": "-1",
    };
    const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions, destProp, destPropId);

    const expectedInstructions = {
      "counters": {
        "nestedCount": {
          "salesCount": FieldValue.increment(12.6),
          "staffCount": FieldValue.increment(-1),
        },
      },
    };

    expect(result.updateData).toEqual(expectedInstructions);
    expect(result.removeData).toEqual({});
  });

  it("should correctly parsed instructions to destPropId if has destPropId", async () => {
    const dstPath = "/users/test-user-id/documents/test-doc-id#counters[nestedCount]";
    const {destProp, destPropId} = getDestPropAndDestPropId(dstPath);

    const instructions = {
      "salesCount": "+12.6",
      "staffCount": "-1",
      "minusCount": "--",
      "plusCount": "++",
      "planets": "arr(+Earth)",
      "continents": "arr(-Asia)",
    };
    const result = await distribution.convertInstructionsToDbValues(transactionMock, instructions, destProp, destPropId);

    const expectedUpdateData = {
      "counters": {
        "nestedCount": {
          "salesCount": FieldValue.increment(12.6),
          "staffCount": FieldValue.increment(-1),
          "minusCount": admin.firestore.FieldValue.increment(-1),
          "plusCount": admin.firestore.FieldValue.increment(+1),
          "planets": admin.firestore.FieldValue.arrayUnion("Earth"),
        },
      },
    };

    expect(result.updateData).toEqual(expectedUpdateData);

    const expectedRemoveData = {
      "counters": {
        "nestedCount": {
          "continents": admin.firestore.FieldValue.arrayRemove("Asia"),
        },
      },
    };
    expect(result.removeData).toEqual(expectedRemoveData);
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
    const dstDocRef = db.doc(event.data.message.json.dstPath);
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
  });

  it("should convert array remove instructions correctly", async () => {
    transactionSetMock.mockReset();
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

    expect(transactionSetMock).toHaveBeenCalledTimes(1);
    const dstDocRef = db.doc(event.data.message.json.dstPath);
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
  });

  it("should convert global counter instruction correctly", async () => {
    transactionSetMock.mockReset();
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

    expect(transactionSetMock).toHaveBeenCalledTimes(1);
    const dstDocRef = db.doc(event.data.message.json.dstPath);
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
  });

  it("should convert array union and remove instructions correctly in a single field", async () => {
    transactionSetMock.mockReset();
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

    const dstDocRef = db.doc(event.data.message.json.dstPath);
    expect(transactionSetMock).toHaveBeenCalledTimes(2);
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedRemoveData, {merge: true});
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
    transactionSetMock.mockReset();
    transactionUpdateMock.mockReset();
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
    expect(transactionSetMock).toHaveBeenCalledTimes(2);
    expect(transactionUpdateMock).toHaveBeenCalledTimes(1);

    const dstDocRef = db.doc(event.data.message.json.dstPath);
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedRemoveData, {merge: true});
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
  });

  it("should process map instructions correctly", async () => {
    transactionSetMock.mockReset();
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
    expect(transactionSetMock).toHaveBeenCalledTimes(2);

    const dstDocRef = db.doc("/users/test-user-id/documents/test-doc-id");
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedRemoveData, {merge: true});
  });

  it("should process event instructions correctly with destprop and destpropid", async () => {
    transactionSetMock.mockReset();
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "hello": {
        "world": {
          "count": admin.firestore.FieldValue.increment(1),
          "score": admin.firestore.FieldValue.increment(5),
          "minusCount": admin.firestore.FieldValue.increment(-1),
          "minusScore": admin.firestore.FieldValue.increment(-3),
          "optionalField": admin.firestore.FieldValue.delete(),
          "arrayUnion": admin.firestore.FieldValue.arrayUnion("add-this"),
          "queueNumber": 11,
        },
      },
    };

    const expectedRemoveData = {
      "hello": {
        "world": {
          "arrayRemove": admin.firestore.FieldValue.arrayRemove("remove-this"),
        },
      },
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id#hello[world]",
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
    expect(transactionSetMock).toHaveBeenCalledTimes(2);

    const dstDocRef = db.doc(event.data.message.json.dstPath.split("#")[0]);
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedRemoveData, {merge: true});
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
  });

  it("should process event instructions correctly with destprop only", async () => {
    transactionSetMock.mockReset();
    isProcessedMock.mockResolvedValueOnce(false);
    const expectedData = {
      "hello": {
        "count": admin.firestore.FieldValue.increment(1),
        "score": admin.firestore.FieldValue.increment(5),
        "minusCount": admin.firestore.FieldValue.increment(-1),
        "minusScore": admin.firestore.FieldValue.increment(-3),
        "optionalField": admin.firestore.FieldValue.delete(),
        "arrayUnion": admin.firestore.FieldValue.arrayUnion("add-this"),
        "queueNumber": 11,
      },
    };

    const expectedRemoveData = {
      "hello": {
        "arrayRemove": admin.firestore.FieldValue.arrayRemove("remove-this"),
      },
    };
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            dstPath: "/users/test-user-id/documents/test-doc-id#hello",
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
    expect(transactionSetMock).toHaveBeenCalledTimes(2);

    const dstDocRef = db.doc(event.data.message.json.dstPath.split("#")[0]);
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedData, {merge: true});
    expect(transactionSetMock).toHaveBeenCalledWith(dstDocRef, expectedRemoveData, {merge: true});
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(INSTRUCTIONS_TOPIC_NAME, event.id);
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
      "withDecimal": "+12.6",
    };
    const instructions2 = {
      "count": "--",
      "score": "-5",
      "minusCount": "++",
      "minusScore": "+3",
      "withDecimal": "-12.6",
    };
    const instructions3 = {
      "count": "-1",
      "score": "-5",
      "minusCount": "+1",
      "minusScore": "+3",
      "withDecimal": "-12.6",
    };
    const existingInstructions = {};

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "count": "++",
      "score": "+5",
      "minusCount": "--",
      "minusScore": "-3",
      "withDecimal": "+12.6",
    });

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "count": "+2",
      "score": "+10",
      "minusCount": "-2",
      "minusScore": "-6",
      "withDecimal": "+25.2",
    });

    distribution.mergeInstructions(existingInstructions, instructions2);
    expect(existingInstructions).toStrictEqual({
      "count": "+1",
      "score": "+5",
      "minusCount": "-1",
      "minusScore": "-3",
      "withDecimal": "+12.6",
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

  it("should merge nested object instructions correctly", () => {
    const instructions1 = {
      "user": {
        "score": "++",
        "counters": {
          "games": "+5",
        },
      },
    };
    const instructions2 = {
      "user": {
        "score": "++",
        "counters": {
          "games": "-2",
          "wins": "+1",
        },
      },
    };
    const existingInstructions: any = {
      "user": {
        "score": "+1",
        "counters": {
          "games": "+1",
        },
      },
    };

    distribution.mergeInstructions(existingInstructions, instructions1);
    expect(existingInstructions).toStrictEqual({
      "user": {
        "score": "+2",
        "counters": {
          "games": "+6",
        },
      },
    });

    distribution.mergeInstructions(existingInstructions, instructions2);
    expect(existingInstructions).toStrictEqual({
      "user": {
        "score": "+3",
        "counters": {
          "games": "+4",
          "wins": "+1",
        },
      },
    });
  });
});

describe("instructionsReducer", () => {
  let mergeInstructionsSpy: jest.SpyInstance;

  beforeEach(() => {
    mergeInstructionsSpy = jest.spyOn(distribution, "mergeInstructions");
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

describe("getGroupPatchStatusPath", () => {
  it("computes independent status paths per patch type/backFillPatchName on the same collection", () => {
    const collectionPath = "/users/user1/feeds";
    const ancestorIdsPath = distribution.getGroupPatchStatusPath(collectionPath, "back-fill", "ancestor-ids");
    const customBackFillPath = distribution.getGroupPatchStatusPath(collectionPath, "back-fill", "custom-back-fill");
    const patchLogicsPath = distribution.getGroupPatchStatusPath(collectionPath, "patch-logics");

    expect(ancestorIdsPath).not.toEqual(customBackFillPath);
    expect(ancestorIdsPath).not.toEqual(patchLogicsPath);
    expect(customBackFillPath).not.toEqual(patchLogicsPath);
    expect(ancestorIdsPath).toContain("back-fill_ancestor-ids");
    expect(customBackFillPath).toContain("back-fill_custom-back-fill");
    expect(patchLogicsPath).toContain("patch-logics");
  });
});

describe("queueGroupPatch", () => {
  let publishMessageSpy: jest.SpyInstance;
  let runTransactionSpy: jest.SpyInstance;
  let txnGetMock: jest.Mock;
  let txnSetMock: jest.Mock;

  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(GROUP_PATCH_TOPIC, "publishMessage")
      .mockImplementation(() => {
        return Promise.resolve("message-id");
      });
    txnGetMock = jest.fn().mockResolvedValue({exists: false});
    txnSetMock = jest.fn();
    runTransactionSpy = jest.spyOn(admin.firestore(), "runTransaction")
      .mockImplementation(async (fn: any) => fn({get: txnGetMock, set: txnSetMock}));
  });

  it("locks the status doc and publishes when there is no lastPatchedId", async () => {
    await distribution.queueGroupPatch({
      path: "/users/user1/feeds/feed1",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });

    expect(runTransactionSpy).toHaveBeenCalledTimes(1);
    expect(txnSetMock).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
      status: "queued",
      collectionPath: "/users/user1/feeds",
      count: 0,
      lastPatchedId: null,
    }), {merge: true});
    expect(publishMessageSpy).toHaveBeenCalledWith({
      json: {
        collectionPath: "/users/user1/feeds",
        patchType: "back-fill",
        backFillPatchName: "ancestor-ids",
        appVersion: undefined,
        lastPatchedId: undefined,
      },
    });
  });

  it("does not publish when a patch is already queued and not in error/reset state", async () => {
    txnGetMock.mockResolvedValue({exists: true, data: () => ({status: "running"})});

    await distribution.queueGroupPatch({
      path: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });

    expect(publishMessageSpy).not.toHaveBeenCalled();
  });

  it("does NOT lock/track a placeholder (wildcard) path and publishes it directly for hydration", async () => {
    // A placeholder path never patches a real document; it only gets hydrated into
    // concrete collections downstream, each of which gets its own status doc + guard.
    // So the lock transaction is skipped entirely and the message is published as-is.
    await distribution.queueGroupPatch({
      path: "/users/{userId}/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });

    expect(runTransactionSpy).not.toHaveBeenCalled();
    expect(txnSetMock).not.toHaveBeenCalled();
    expect(publishMessageSpy).toHaveBeenCalledWith({
      json: {
        collectionPath: "/users/{userId}/feeds",
        patchType: "back-fill",
        backFillPatchName: "ancestor-ids",
        appVersion: undefined,
        lastPatchedId: undefined,
        iteration: undefined,
      },
    });
  });

  it("re-locks and publishes when the previous status is error", async () => {
    txnGetMock.mockResolvedValue({exists: true, data: () => ({status: "error"})});

    await distribution.queueGroupPatch({
      path: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });

    expect(txnSetMock).toHaveBeenCalled();
    expect(publishMessageSpy).toHaveBeenCalled();
  });

  // --- restart via "reset" status ----------------------------------------
  //
  // `force` has been removed. An operator restart is done by stamping the
  // status doc(s) to "reset" (see resetGroupPatchStatuses / onGroupPatchRequest).
  // A "reset" (like "error") is treated as SETTLED here, so it falls through and
  // re-locks + re-publishes. A patch that is still IN FLIGHT (queued / running /
  // hydrating) is always treated as ALREADY QUEUED and never restarted, so a
  // re-trigger cannot spawn an overlapping chain (a runaway backfill).

  describe.each([
    ["running"],
    ["queued"],
    ["hydrating"],
  ])("an in-flight patch is never restarted (status=%s)", (status) => {
    it("does NOT re-lock and does NOT re-publish", async () => {
      txnGetMock.mockResolvedValue({exists: true, data: () => ({status})});

      await distribution.queueGroupPatch({
        path: "/users/user1/feeds",
        patchType: "back-fill",
        backFillPatchName: "ancestor-ids",
      });

      // The in-flight run is left untouched: no re-lock and no second message.
      expect(txnSetMock).not.toHaveBeenCalled();
      expect(publishMessageSpy).not.toHaveBeenCalled();
    });
  });

  describe.each([
    ["error"],
    ["reset"],
  ])("a settled patch is restarted (status=%s)", (status) => {
    it("re-locks the status doc and publishes", async () => {
      txnGetMock.mockResolvedValue({exists: true, data: () => ({status})});

      await distribution.queueGroupPatch({
        path: "/users/user1/feeds",
        patchType: "back-fill",
        backFillPatchName: "ancestor-ids",
      });

      expect(txnSetMock).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
        status: "queued",
        collectionPath: "/users/user1/feeds",
        count: 0,
        lastPatchedId: null,
      }), {merge: true});
      expect(publishMessageSpy).toHaveBeenCalledWith({
        json: {
          collectionPath: "/users/user1/feeds",
          patchType: "back-fill",
          backFillPatchName: "ancestor-ids",
          appVersion: undefined,
          lastPatchedId: undefined,
        },
      });
    });
  });

  it("does not re-lock or publish when already completed", async () => {
    txnGetMock.mockResolvedValue({exists: true, data: () => ({status: "completed"})});

    await distribution.queueGroupPatch({
      path: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });

    expect(txnSetMock).not.toHaveBeenCalled();
    expect(publishMessageSpy).not.toHaveBeenCalled();
  });

  it("skips the lock transaction entirely on a reschedule (lastPatchedId provided)", async () => {
    // When a lastPatchedId is provided (a reschedule), the lock transaction is
    // skipped entirely, so a re-trigger can never re-run the guard mid-chain.
    await distribution.queueGroupPatch({
      path: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
      lastPatchedId: "feed5",
    });

    expect(runTransactionSpy).not.toHaveBeenCalled();
    expect(publishMessageSpy).toHaveBeenCalledTimes(1);
  });

  it("skips locking and publishes directly when lastPatchedId is provided", async () => {
    await distribution.queueGroupPatch({
      path: "/users/user1/feeds",
      patchType: "patch-logics",
      appVersion: "1.0.0",
      lastPatchedId: "feed5",
    });

    expect(runTransactionSpy).not.toHaveBeenCalled();
    expect(publishMessageSpy).toHaveBeenCalledWith({
      json: {
        collectionPath: "/users/user1/feeds",
        patchType: "patch-logics",
        backFillPatchName: undefined,
        appVersion: "1.0.0",
        lastPatchedId: "feed5",
      },
    });
  });

  it("tracks independent locks for different patch types/back-fills on the same collection", async () => {
    const docSpy = jest.spyOn(admin.firestore(), "doc");

    await distribution.queueGroupPatch({
      path: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });
    await distribution.queueGroupPatch({
      path: "/users/user1/feeds",
      patchType: "patch-logics",
      appVersion: "1.0.0",
    });

    const calledPaths = docSpy.mock.calls.map((call) => call[0]);
    expect(new Set(calledPaths).size).toBe(calledPaths.length);
    expect(publishMessageSpy).toHaveBeenCalledTimes(2);
  });
});

describe("onMessageGroupPatchQueue", () => {
  let patchGroupDocsSpy: jest.SpyInstance;

  beforeEach(() => {
    isProcessedMock.mockResolvedValue(false);
    patchGroupDocsSpy = jest.spyOn(indexUtils, "patchGroupDocs").mockResolvedValue();
  });

  it("skips already processed messages", async () => {
    isProcessedMock.mockResolvedValue(true);
    const event = {
      id: "test-event",
      data: {message: {json: {collectionPath: "/users/user1/feeds", patchType: "back-fill", backFillPatchName: "ancestor-ids"}}},
    } as unknown as CloudEvent<MessagePublishedData>;

    await distribution.onMessageGroupPatchQueue(event);

    expect(patchGroupDocsSpy).not.toHaveBeenCalled();
  });

  it("dispatches to patchGroupDocs for non-placeholder collection paths", async () => {
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            collectionPath: "/users/user1/feeds",
            patchType: "back-fill",
            backFillPatchName: "ancestor-ids",
            lastPatchedId: "feed1",
          },
        },
      },
    } as unknown as CloudEvent<MessagePublishedData>;

    await distribution.onMessageGroupPatchQueue(event);

    expect(patchGroupDocsSpy).toHaveBeenCalledWith({
      collectionPath: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
      appVersion: undefined,
      lastPatchedId: "feed1",
    });
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(GROUP_PATCH_TOPIC_NAME, event.id);
  });

  it("dispatches to patchGroupDocs for patch-logics messages", async () => {
    const event = {
      id: "test-event",
      data: {
        message: {
          json: {
            collectionPath: "/users/user1/feeds",
            patchType: "patch-logics",
            appVersion: "1.0.0",
          },
        },
      },
    } as unknown as CloudEvent<MessagePublishedData>;

    await distribution.onMessageGroupPatchQueue(event);

    expect(patchGroupDocsSpy).toHaveBeenCalledWith({
      collectionPath: "/users/user1/feeds",
      patchType: "patch-logics",
      backFillPatchName: undefined,
      appVersion: "1.0.0",
      lastPatchedId: undefined,
    });
  });

  it("does NOT write a placeholder status doc while hydration is in progress", async () => {
    const hydratePathSpy = jest.spyOn(paths, "hydratePath").mockResolvedValue({
      documentPaths: [],
      hydrationState: {batch: 2} as any,
    });
    const docSetMock = jest.fn();
    jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      set: docSetMock,
    } as unknown as admin.firestore.DocumentReference);
    const publishMessageSpy = jest.spyOn(GROUP_PATCH_TOPIC, "publishMessage")
      .mockImplementation(() => Promise.resolve("message-id"));

    const event = {
      id: "test-event",
      data: {message: {json: {collectionPath: "/users/{userId}/feeds", patchType: "back-fill", backFillPatchName: "ancestor-ids"}}},
    } as unknown as CloudEvent<MessagePublishedData>;

    await distribution.onMessageGroupPatchQueue(event);

    expect(hydratePathSpy).toHaveBeenCalled();
    // A re-queue message is still published to continue hydration...
    expect(publishMessageSpy).toHaveBeenCalledWith({
      json: expect.objectContaining({
        collectionPath: "/users/{userId}/feeds",
        hydrationState: {batch: 2},
      }),
    });
    // ...but the wildcard/placeholder path is NOT tracked with a status doc.
    expect(docSetMock).not.toHaveBeenCalled();
  });

  it("does NOT write a placeholder status doc when hydration completes", async () => {
    const hydratePathSpy = jest.spyOn(paths, "hydratePath").mockResolvedValue({
      documentPaths: [],
      hydrationState: undefined,
    });
    const docSetMock = jest.fn();
    jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      set: docSetMock,
    } as unknown as admin.firestore.DocumentReference);
    jest.spyOn(GROUP_PATCH_TOPIC, "publishMessage").mockImplementation(() => Promise.resolve("message-id"));

    const event = {
      id: "test-event",
      data: {message: {json: {collectionPath: "/users/{userId}/feeds", patchType: "back-fill", backFillPatchName: "ancestor-ids"}}},
    } as unknown as CloudEvent<MessagePublishedData>;

    await distribution.onMessageGroupPatchQueue(event);

    expect(hydratePathSpy).toHaveBeenCalled();
    // The placeholder path is never stamped "hydrating"/"completed"...
    expect(docSetMock).not.toHaveBeenCalled();
    // ...and the message is still marked processed.
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(GROUP_PATCH_TOPIC_NAME, event.id);
  });

  it("does NOT re-queue a hydrated path that still contains a placeholder (breaks the infinite loop)", async () => {
    // Regression test for the backfill infinite loop: a templated path
    // ("/topics/{topicId}/orders") is hydrated, but one of the returned paths
    // still contains "{" because a real document id literally starts with "{"
    // (bad data). Re-queueing that path would make onMessageGroupPatchQueue treat
    // it as a template again (collectionPath.includes("{") === true) and hydrate
    // it forever. The concrete path must still be re-queued as normal.
    const hydratePathSpy = jest.spyOn(paths, "hydratePath").mockResolvedValue({
      documentPaths: ["/topics/t1/orders", "/topics/{viewTopicId}/orders"],
      hydrationState: undefined,
    });
    // queueGroupPatch (invoked per re-queued concrete path) locks + publishes.
    jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      set: jest.fn(),
    } as unknown as admin.firestore.DocumentReference);
    jest.spyOn(admin.firestore(), "runTransaction")
      .mockImplementation(async (fn: any) => fn({
        get: jest.fn().mockResolvedValue({exists: false}),
        set: jest.fn(),
      }));
    const publishMessageSpy = jest.spyOn(GROUP_PATCH_TOPIC, "publishMessage")
      .mockImplementation(() => Promise.resolve("message-id"));
    const warnSpy = jest.spyOn(console, "warn").mockImplementation();
    // This describe block does not reset spies between tests, and jest.spyOn
    // reuses an already-installed spy, so clear any accumulated calls to keep the
    // call-count assertions below accurate regardless of test ordering.
    publishMessageSpy.mockClear();
    warnSpy.mockClear();

    const event = {
      id: "test-event",
      data: {message: {json: {collectionPath: "/topics/{topicId}/orders", patchType: "back-fill", backFillPatchName: "ancestor-ids"}}},
    } as unknown as CloudEvent<MessagePublishedData>;

    await distribution.onMessageGroupPatchQueue(event);

    expect(hydratePathSpy).toHaveBeenCalled();
    // Only the concrete path is re-queued; the "{" path is skipped, so exactly
    // one message is published (for "/topics/t1/orders").
    expect(publishMessageSpy).toHaveBeenCalledTimes(1);
    expect(publishMessageSpy).toHaveBeenCalledWith({
      json: expect.objectContaining({collectionPath: "/topics/t1/orders"}),
    });
    // The placeholder path is NEVER re-queued...
    expect(publishMessageSpy).not.toHaveBeenCalledWith({
      json: expect.objectContaining({collectionPath: expect.stringContaining("{")}),
    });
    // ...and a warning is emitted so the anomaly is visible instead of silent.
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("/topics/{viewTopicId}/orders"));
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(GROUP_PATCH_TOPIC_NAME, event.id);
  });
});

describe("getGroupPatchProgress", () => {
  let docGetMock: jest.Mock;

  beforeEach(() => {
    docGetMock = jest.fn();
    jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      get: docGetMock,
    } as unknown as admin.firestore.DocumentReference);
  });

  it("returns undefined when the status doc does not exist", async () => {
    docGetMock.mockResolvedValue({exists: false});

    const progress = await distribution.getGroupPatchProgress({
      collectionPath: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });

    expect(progress).toBeUndefined();
  });

  it("returns a running progress", async () => {
    docGetMock.mockResolvedValue({
      exists: true,
      data: () => ({status: "running", count: 500, lastPatchedId: "doc500"}),
    });

    const progress = await distribution.getGroupPatchProgress({
      collectionPath: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });

    expect(progress).toEqual(expect.objectContaining({
      status: "running",
      patchedCount: 500,
      lastPatchedId: "doc500",
      collectionPath: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    }));
  });

  it("returns a completed progress", async () => {
    docGetMock.mockResolvedValue({
      exists: true,
      data: () => ({status: "completed", count: 1000}),
    });

    const progress = await distribution.getGroupPatchProgress({
      collectionPath: "/users/user1/feeds",
      patchType: "patch-logics",
    });

    expect(progress).toEqual(expect.objectContaining({status: "completed", patchedCount: 1000}));
  });

  it("returns an error progress with the error message", async () => {
    docGetMock.mockResolvedValue({
      exists: true,
      data: () => ({status: "error", error: "Something went wrong"}),
    });

    const progress = await distribution.getGroupPatchProgress({
      collectionPath: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "custom-back-fill",
    });

    expect(progress).toEqual(expect.objectContaining({status: "error", error: "Something went wrong"}));
  });

  it("tracks independent progress per patch type on the same collection", async () => {
    const docSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      get: docGetMock,
    } as unknown as admin.firestore.DocumentReference);
    docGetMock.mockResolvedValue({exists: true, data: () => ({status: "running", count: 1})});

    await distribution.getGroupPatchProgress({
      collectionPath: "/users/user1/feeds",
      patchType: "back-fill",
      backFillPatchName: "ancestor-ids",
    });
    await distribution.getGroupPatchProgress({
      collectionPath: "/users/user1/feeds",
      patchType: "patch-logics",
    });

    const calledPaths = docSpy.mock.calls.map((call) => call[0]);
    expect(calledPaths[0]).not.toEqual(calledPaths[1]);
  });
});
