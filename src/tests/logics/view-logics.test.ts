import {LogicResult, LogicResultAction, LogicResultDoc, LogicResultDocPriority, ViewDefinition} from "../../types";
import {PubSub, Topic} from "@google-cloud/pubsub";
import * as pathsutils from "../../utils/paths";
import * as viewLogics from "../../logics/view-logics";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import * as indexUtils from "../../index-utils";
import {firestore} from "firebase-admin";
import {PEER_SYNC_TOPIC_NAME, VIEW_LOGICS_TOPIC_NAME} from "../../index";

jest.mock("../../index", () => {
  const originalModule = jest.requireActual("../../index");
  return {
    ...originalModule,
    docPaths: {
      user: "users/{userId}",
      friend: "users/{userId}/friends/{friendId}",
      post: "users/{userId}/posts/{postId}",
      comment: "users/{userId}/posts/{postId}/comments/{commentId}",
    },
    pubsub: new PubSub(),
    VIEW_LOGICS_TOPIC_NAME: "view-logics-queue",
    PEER_SYNC_TOPIC_NAME: "peer-sync-queue",
  };
});

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

const testPeerLogicResultDoc: LogicResultDoc = {
  action: "merge",
  dstPath: "users/1234/posts/5678/comments/9876",
  doc: {
    title: "I like it",
  },
  instructions: {
    likes: "++",
  },
  priority: "normal",
};

const testLogicResultDocDelete: LogicResultDoc = {
  action: "delete",
  dstPath: "users/1234",
  priority: "normal",
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
  });
});

describe("syncPeerViews", () => {
  it("should create a logic function that processes the given logicResultDoc and syncs peer views", async () => {
    const hydrateDocPathSpy = jest.spyOn(pathsutils, "hydrateDocPath");
    hydrateDocPathSpy.mockReset();
    hydrateDocPathSpy
      .mockReturnValueOnce(Promise.resolve([
        "users/456/posts/5678/comments/9876",
        "users/789/posts/5678/comments/9876",
        "users/890/posts/5678/comments/9876",
      ]));
    const findMatchingDocPathRegexSpy = jest.spyOn(pathsutils, "findMatchingDocPathRegex");
    findMatchingDocPathRegexSpy
      .mockReturnValue({entity: "comment", regex: /^users\/([^/]+)\/posts\/([^/]+)\/comments\/([^/]+)$/});

    // Call the syncPeerViews function with the test logic result doc
    // users/1234/posts/5678/comments/9876
    const result = await viewLogics.syncPeerViews(testPeerLogicResultDoc);

    // Add your expectations here, e.g., result.documents should have the correct properties and values
    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(3);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/456/posts/5678/comments/9876");
    expect(document.doc).toEqual({"title": "I like it"});
    expect(document.instructions).toEqual({"likes": "++"});

    document = result.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/789/posts/5678/comments/9876");
    expect(document.doc).toEqual({"title": "I like it"});
    expect(document.instructions).toEqual({"likes": "++"});

    document = result.documents[2];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/5678/comments/9876");
    expect(document.doc).toEqual({"title": "I like it"});
    expect(document.instructions).toEqual({"likes": "++"});

    expect(hydrateDocPathSpy.mock.calls[0][0]).toEqual("users/{userId}/posts/5678/comments/9876");
    expect(hydrateDocPathSpy.mock.calls[0][1]).toEqual({
      "user": {
        fieldName: "@id",
        operator: "!=",
        value: "1234",
      },
    });
  });
});

describe("queueRunViewLogics", () => {
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

  it("should queue docs to run view logics", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-merge"},
      dstPath: "users/test-user-id/documents/doc1",
    };
    const logicResultDocs: LogicResultDoc[] = [doc1];
    await viewLogics.queueRunViewLogics(logicResultDocs);

    expect(topicSpy).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME);
    expect(publishMessageMock).toHaveBeenCalledWith({json: doc1});
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

    expect(topicSpy).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME);
    expect(publishMessageMock).not.toHaveBeenCalled();
  });
});

describe("onMessageViewLogicsQueue", () => {
  let runViewLogicsSpy: jest.SpyInstance;
  let expandConsolidateAndGroupByDstPathSpy: jest.SpyInstance;
  let groupDocsByUserAndDstPathSpy: jest.SpyInstance;
  let distributeSpy: jest.SpyInstance;
  let distributeLaterSpy: jest.SpyInstance;
  let queueForPeerSyncSpy: jest.SpyInstance;

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
  const groupDocsByUserResult = {
    userDocsByDstPath: new Map<string, LogicResultDoc[]>([
      ["users/doc1", [{action: "merge", priority: "normal", dstPath: "users/doc1", doc: {field1: "value1a", field3: "value3"}, instructions: {field2: "--", field4: "--"}}]],
    ]),
    otherUsersDocsByDstPath: new Map<string, LogicResultDoc[]>([
      ["users/doc2", [{action: "delete", priority: "normal", dstPath: "users/doc2"}]],
      ["users/doc4", [{action: "merge", priority: "normal", dstPath: "users/doc4", doc: {}, instructions: {}}]],
      ["users/doc7", [{action: "delete", priority: "normal", dstPath: "users/doc7"}]],
      ["users/doc6", [{action: "merge", priority: "normal", dstPath: "users/doc6", doc: {}}]],
    ]),
  };

  beforeEach(() => {
    runViewLogicsSpy = jest.spyOn(indexUtils, "runViewLogics").mockResolvedValue(viewLogicsResult);
    expandConsolidateAndGroupByDstPathSpy = jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(expandConsolidateResult);
    groupDocsByUserAndDstPathSpy = jest.spyOn(indexUtils, "groupDocsByUserAndDstPath").mockReturnValue(groupDocsByUserResult);
    distributeSpy = jest.spyOn(indexUtils, "distribute").mockResolvedValue();
    distributeLaterSpy = jest.spyOn(indexUtils, "distributeLater").mockResolvedValue();
    queueForPeerSyncSpy = jest.spyOn(viewLogics, "queueForPeerSync").mockResolvedValue();
  });

  it("should distribute queued view logics", async () => {
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
    const viewLogicsResultDocs = viewLogicsResult.map((logicResult) => logicResult.documents).flat();

    await viewLogics.onMessageViewLogicsQueue(event);

    expect(runViewLogicsSpy).toHaveBeenCalledWith(doc1);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalledWith(viewLogicsResultDocs);
    expect(groupDocsByUserAndDstPathSpy).toHaveBeenCalledWith(expandConsolidateResult, userId);
    expect(distributeSpy).toHaveBeenCalledWith(groupDocsByUserResult.userDocsByDstPath);
    expect(distributeLaterSpy).toHaveBeenCalledWith(groupDocsByUserResult.otherUsersDocsByDstPath);
    expect(queueForPeerSyncSpy).toHaveBeenCalledWith(doc1);
  });
});

describe("queueForPeerSync", () => {
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

  it("should queue docs for peer syncing", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      dstPath: "users/test-user-id/documents/doc1",
    };
    await viewLogics.queueForPeerSync(doc1);

    expect(topicSpy).toHaveBeenCalledWith(PEER_SYNC_TOPIC_NAME);
    expect(publishMessageMock).toHaveBeenCalledWith({json: doc1});
  });
});

describe("onMessagePeerSyncQueue", () => {
  let syncPeerViewsSpy: jest.SpyInstance;
  let expandConsolidateAndGroupByDstPathSpy: jest.SpyInstance;
  let distributeLaterSpy: jest.SpyInstance;

  const syncPeerViewsResult = {
    name: "SyncPeerViews",
    status: "finished",
    timeFinished: firestore.Timestamp.now(),
    documents: [
      {action: "merge" as LogicResultAction, priority: "normal" as LogicResultDocPriority, dstPath: "users/random-user-id1/documents/doc1", doc: {name: "test-doc-name-updated"}, instructions: {}},
      {action: "merge" as LogicResultAction, priority: "normal" as LogicResultDocPriority, dstPath: "users/random-user-id2/documents/doc1", doc: {name: "test-doc-name-updated"}, instructions: {}},
    ],
  };
  const expandConsolidateResult = new Map<string, LogicResultDoc[]>([
    ["users/random-user-id1/documents/doc1", [{action: "merge", priority: "normal", dstPath: "users/random-user-id1/documents/doc1", doc: {name: "test-doc-name-updated"}}]],
    ["users/random-user-id2/documents/doc1", [{action: "merge", priority: "normal", dstPath: "users/random-user-id1/documents/doc1", doc: {name: "test-doc-name-updated"}}]],
  ]);

  beforeEach(() => {
    syncPeerViewsSpy = jest.spyOn(viewLogics, "syncPeerViews").mockResolvedValue(syncPeerViewsResult);
    expandConsolidateAndGroupByDstPathSpy = jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(expandConsolidateResult);

    distributeLaterSpy = jest.spyOn(indexUtils, "distributeLater").mockResolvedValue();
  });

  it("should distribute queued view logics", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      dstPath: "users/test-user-id/documents/doc1",
    };
    const event = {
      data: {
        message: {
          json: doc1,
        },
      },
    } as CloudEvent<MessagePublishedData>;
    const result = await viewLogics.onMessagePeerSyncQueue(event);

    expect(syncPeerViewsSpy).toHaveBeenCalledWith(doc1);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalledWith(syncPeerViewsResult.documents);
    expect(distributeLaterSpy).toHaveBeenCalledWith(expandConsolidateResult);
    expect(result).toEqual("Processed peer sync");
  });
});
