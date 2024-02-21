import {
  LogicResult,
  LogicResultDoc,
  ViewDefinition,
  ProjectConfig,
} from "../../types";
const isProcessedMock = jest.fn();
const trackProcessedIdsMock = jest.fn();
import * as pathsutils from "../../utils/paths";
import * as viewLogics from "../../logics/view-logics";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import * as indexUtils from "../../index-utils";
import {firestore} from "firebase-admin";
import {
  initializeEmberFlow,
  VIEW_LOGICS_TOPIC,
  VIEW_LOGICS_TOPIC_NAME,
} from "../../index";
import * as admin from "firebase-admin";
import {securityConfig} from "../../sample-custom/security";
import {validatorConfig} from "../../sample-custom/validators";
import {dbStructure, Entity} from "../../sample-custom/db-structure";

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
initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, []);

const vd1: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar", "age"],
  destEntity: "friend",
};

const vd2: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar"],
  destEntity: "post",
  destProp: "postedBy",
};

const vd3: ViewDefinition = {
  srcEntity: "users",
  srcProps: ["username", "avatarUrl"],
  destEntity: "server",
  destProp: "createdBy",
};

const testLogicResultDoc: LogicResultDoc = {
  action: "merge",
  dstPath: "users/1234",
  doc: {
    name: "John Doe",
  },
  instructions: {
    age: "++",
  },
  priority: "normal",
};

const testLogicResultDocDelete: LogicResultDoc = {
  action: "delete",
  dstPath: "users/1234",
  priority: "normal",
};

const userLogicResultDoc: LogicResultDoc = {
  action: "merge",
  dstPath: "users/123",
  priority: "normal",
  doc: {
    username: "new_username",
  },
};

describe("createViewLogicFn", () => {
  it("should create a logic function that processes the given logicResultDoc and view definition", async () => {
    const hydrateDocPathSpy = jest.spyOn(pathsutils, "hydrateDocPath");
    hydrateDocPathSpy.mockReset();
    hydrateDocPathSpy
      .mockReturnValueOnce(Promise.resolve([
        "users/456/friends/1234",
        "users/789/friends/1234",
      ]))
      .mockReturnValueOnce(Promise.resolve([
        "users/1234/posts/987",
        "users/1234/posts/654",
        "users/890/posts/987",
        "users/890/posts/654",
      ]))
      .mockReturnValueOnce(Promise.resolve([
        "users/456/friends/1234",
        "users/789/friends/1234",
      ]))
      .mockReturnValueOnce(Promise.resolve([
        "servers/123",
        "servers/456",
      ]));

    // Create the logic function using the viewDefinition
    const logicFn = viewLogics.createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn(testLogicResultDoc);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/456/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe"});
    expect(document.instructions).toEqual({"age": "++"});

    document = result.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/789/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe"});
    expect(document.instructions).toEqual({"age": "++"});

    expect(hydrateDocPathSpy.mock.calls[0][0]).toEqual("users/{userId}/friends/1234");
    expect(hydrateDocPathSpy.mock.calls[0][1]).toEqual({});

    // Create the logic function using the viewDefinition
    const logicFn2 = viewLogics.createViewLogicFn(vd2);

    // Call the logic function with the test action
    const result2 = await logicFn2(testLogicResultDoc);

    // Add your expectations here, e.g., result.documents should have the correct properties and values
    expect(result2).toBeDefined();
    expect(result2.documents).toBeDefined();
    expect(result2.documents.length).toEqual(4);

    document = result2.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/1234/posts/987");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    document = result2.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/1234/posts/654");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    document = result2.documents[2];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/987");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    document = result2.documents[3];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/654");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    expect(hydrateDocPathSpy.mock.calls[1][0]).toEqual("users/{userId}/posts/{postId}");
    expect(hydrateDocPathSpy.mock.calls[1][1]).toEqual({
      post: {
        fieldName: "postedBy.@id",
        operator: "==",
        value: "1234",
      },
    });

    const resultDelete = await logicFn(testLogicResultDocDelete);

    expect(resultDelete).toBeDefined();
    expect(resultDelete.documents).toBeDefined();
    expect(resultDelete.documents.length).toEqual(2);

    document = resultDelete.documents[0];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/456/friends/1234");

    document = resultDelete.documents[1];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/789/friends/1234");

    expect(hydrateDocPathSpy.mock.calls[2][0]).toEqual("users/{userId}/friends/1234");
    expect(hydrateDocPathSpy.mock.calls[2][1]).toEqual({});

    // Create the logic function using the viewDefinition
    const logicFn3 = viewLogics.createViewLogicFn(vd3);

    // Call the logic function with the test action
    const result3 = await logicFn3(userLogicResultDoc);

    expect(result3).toBeDefined();
    expect(result3.documents).toBeDefined();
    expect(result3.documents.length).toEqual(2);

    document = result3.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "servers/123");
    expect(document.doc).toEqual({"createdBy.username": "new_username"});

    document = result3.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "servers/456");
    expect(document.doc).toEqual({"createdBy.username": "new_username"});

    expect(hydrateDocPathSpy.mock.calls[3][0]).toEqual("servers/{serverId}");
    expect(hydrateDocPathSpy.mock.calls[3][1]).toEqual({
      server: {
        fieldName: "createdBy.@id",
        operator: "==",
        value: "123",
      },
    });
  });
});

describe("queueRunViewLogics", () => {
  let publishMessageSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(VIEW_LOGICS_TOPIC, "publishMessage")
      .mockImplementation(() => {
        return "message-id";
      });
  });

  it("should queue docs to run view logics", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-merge"},
      dstPath: "users/test-user-id/documents/doc1",
    };
    const logicResultDocs: LogicResultDoc[] = [doc1];
    await viewLogics.queueRunViewLogics(logicResultDocs);

    expect(publishMessageSpy).toHaveBeenCalledWith({json: doc1});
  });

  it("should not queue view logics when action is create", async () => {
    const doc1: LogicResultDoc = {
      action: "create",
      priority: "normal",
      doc: {name: "test-doc-name-created"},
      dstPath: "users/test-user-id/documents/doc1",
    };
    const logicResultDocs: LogicResultDoc[] = [doc1];
    await viewLogics.queueRunViewLogics(logicResultDocs);

    expect(publishMessageSpy).not.toHaveBeenCalled();
  });
});

describe("onMessageViewLogicsQueue", () => {
  let runViewLogicsSpy: jest.SpyInstance;
  let expandConsolidateAndGroupByDstPathSpy: jest.SpyInstance;
  let distributeSpy: jest.SpyInstance;

  const viewLogicsResult: LogicResult[] = [
    {
      name: "logic 1",
      timeFinished: firestore.Timestamp.now(),
      status: "finished",
      documents: [
        {action: "merge", priority: "normal", dstPath: "users/doc1", doc: {field1: "value1"}, instructions: {field2: "++"}},
        {action: "delete", priority: "normal", dstPath: "users/doc2"},
      ],
    },
    {
      name: "logic 2",
      timeFinished: firestore.Timestamp.now(),
      status: "finished",
      documents: [
        {action: "merge", priority: "normal", dstPath: "users/doc1", doc: {field1: "value1a"}, instructions: {field2: "--"}},
        {action: "merge", priority: "normal", dstPath: "users/doc1", doc: {field3: "value3"}, instructions: {field4: "--"}},
        {action: "copy", priority: "normal", srcPath: "users/doc3", dstPath: "users/doc4"},
        {action: "merge", priority: "normal", dstPath: "users/doc2", doc: {field4: "value4"}},
        {action: "merge", priority: "normal", dstPath: "users/doc7", doc: {field6: "value7"}},
      ],
    },
  ];
  const expandConsolidateResult = new Map<string, LogicResultDoc[]>([
    ["users/doc1", [{action: "merge", priority: "normal", dstPath: "users/doc1", doc: {field1: "value1a", field3: "value3"}, instructions: {field2: "--", field4: "--"}}]],
    ["users/doc2", [{action: "delete", priority: "normal", dstPath: "users/doc2"}]],
    ["users/doc4", [{action: "merge", priority: "normal", dstPath: "users/doc4", doc: {}, instructions: {}}]],
    ["users/doc7", [{action: "delete", priority: "normal", dstPath: "users/doc7"}]],
    ["users/doc6", [{action: "merge", priority: "normal", dstPath: "users/doc6", doc: {}}]],
  ]);

  const userId = "doc1";
  const doc1: LogicResultDoc = {
    action: "merge",
    priority: "normal",
    doc: {name: "test-doc-name-updated"},
    dstPath: `users/${userId}`,
  };
  const event = {
    data: {
      message: {
        json: doc1,
      },
    },
  } as CloudEvent<MessagePublishedData>;

  beforeEach(() => {
    runViewLogicsSpy = jest.spyOn(indexUtils, "runViewLogics").mockResolvedValue(viewLogicsResult);
    expandConsolidateAndGroupByDstPathSpy = jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(expandConsolidateResult);
    distributeSpy = jest.spyOn(indexUtils, "distribute").mockResolvedValue();
  });

  it("should skip duplicate message", async () => {
    isProcessedMock.mockResolvedValueOnce(true);
    jest.spyOn(console, "log").mockImplementation();
    await viewLogics.onMessageViewLogicsQueue(event);

    expect(isProcessedMock).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME, event.id);
    expect(console.log).toHaveBeenCalledWith("Skipping duplicate message");
  });

  it("should distribute queued view logics", async () => {
    const viewLogicsResultDocs = viewLogicsResult.map((logicResult) => logicResult.documents).flat();
    const result = await viewLogics.onMessageViewLogicsQueue(event);

    expect(runViewLogicsSpy).toHaveBeenCalledWith(doc1);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalledWith(viewLogicsResultDocs);
    expect(distributeSpy).toHaveBeenCalledWith(expandConsolidateResult);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed view logics");
  });
});
