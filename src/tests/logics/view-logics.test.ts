import {
  LogicResult,
  LogicResultDoc,
  ViewDefinition,
  ProjectConfig,
} from "../../types";

const isProcessedMock = jest.fn();
const trackProcessedIdsMock = jest.fn();
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
import {DocumentReference} from "firebase-admin/lib/firestore";
import Timestamp = firestore.Timestamp;

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
  destProp: {
    name: "followers",
    type: "array-map",
  },
};

const vd3: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatarUrl", "friendCount", "likes"],
  destEntity: "server",
  destProp: {
    name: "createdBy",
    type: "map",
  },
};

const vd4: ViewDefinition = {
  srcEntity: "friend",
  srcProps: ["name", "avatar", "age"],
  destEntity: "user",
};

const createLogicResultDoc: LogicResultDoc = {
  action: "create",
  dstPath: "users/1234",
  doc: {
    "@id": "1234",
    "name": "John Doe",
    "age": "26",
  },
  priority: "normal",
};

const mergeLogicResultDoc: LogicResultDoc = {
  action: "merge",
  dstPath: "users/1234",
  doc: {
    "@id": "1234",
    "name": "John Doe",
    "pets": 0,
  },
  instructions: {
    age: "++",
  },
  priority: "normal",
};

const deleteLogicResultDoc: LogicResultDoc = {
  action: "delete",
  dstPath: "users/1234",
  priority: "normal",
  doc: {
    "@id": "1234",
  },
};

const userLogicResultDoc: LogicResultDoc = {
  action: "merge",
  dstPath: "users/123",
  priority: "normal",
  doc: {
    name: "new_name",
    friendCount: 0,
    likes: 24,
  },
};

describe("createViewLogicFn", () => {
  let colGetMock: jest.SpyInstance;
  let docGetMock: jest.SpyInstance;
  let docUpdateMock: jest.SpyInstance;
  let docSetMock: jest.SpyInstance;
  let docSpy: jest.SpyInstance;
  beforeEach(() => {
    colGetMock = jest.fn();
    docUpdateMock = jest.fn();
    docSetMock = jest.fn();
    docGetMock = jest.fn().mockResolvedValue({
      data: () => {
        return {
          "@viewsAlreadyBuilt+friend": false,
        };
      },
    });
    docSpy = jest.spyOn(admin.firestore(), "doc").mockImplementation(() => {
      return {
        set: docSetMock,
        update: docUpdateMock,
        get: docGetMock,
        collection: jest.fn().mockReturnValue({
          where: jest.fn().mockReturnValue({
            get: colGetMock,
            where: jest.fn().mockReturnValue({
              get: colGetMock,
              where: jest.fn().mockReturnValue({
                get: colGetMock,
                where: jest.fn().mockReturnValue({
                  get: colGetMock,
                }),
              }),
            }),
          }),
        }),
      } as unknown as DocumentReference;
    });
  });

  it("should log error document does not have @id", async () => {
    jest.spyOn(console, "error").mockImplementation();
    const logicFn = viewLogics.createViewLogicFn(vd4);

    const logicResultDoc: LogicResultDoc = {
      action: "create",
      dstPath: "users/5678/friends/1234",
      doc: {
        name: "John Doe",
      },
      priority: "normal",
    };
    const result = await logicFn[1](logicResultDoc);
    expect(result.documents.length).toEqual(0);
    expect(console.error).toHaveBeenCalledWith("Document does not have an @id attribute");
  });

  it("should log error when srcPath includes placeholder", async () => {
    jest.spyOn(console, "error").mockImplementation();
    const logicFn = viewLogics.createViewLogicFn(vd4);

    const logicResultDoc: LogicResultDoc = {
      action: "create",
      dstPath: "users/5678/friends/1234",
      doc: {
        "@id": "1234",
        "name": "John Doe",
      },
      priority: "normal",
    };
    const result = await logicFn[1](logicResultDoc);
    expect(result.documents.length).toEqual(0);
    expect(console.error).toHaveBeenCalledWith("srcPath should not have a placeholder");
  });

  it("should create @views doc", async () => {
    let logicFn = viewLogics.createViewLogicFn(vd1);

    let result = await logicFn[1](createLogicResultDoc);
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "create");
    expect(result.documents[0]).toHaveProperty("dstPath", "users/1234/@views/users+1234");
    expect(result.documents[0].doc).toEqual({
      "path": "users/1234",
      "srcProps": ["age", "avatar", "name"],
      "destEntity": "friend",
    });

    logicFn = viewLogics.createViewLogicFn(vd2);

    result = await logicFn[1]({
      ...createLogicResultDoc,
      dstPath: "users/1234/posts/987#followers[1234]",
    });
    expect(result.documents.length).toEqual(2);
    expect(result.documents[0]).toHaveProperty("action", "create");
    expect(result.documents[0])
      .toHaveProperty("dstPath", "users/1234/@views/users+1234+posts+987+followers[1234]");
    expect(result.documents[0].doc).toEqual({
      path: "users/1234/posts/987#followers[1234]",
      srcProps: ["avatar", "name"],
      destEntity: "post",
      destProp: "followers",
    });
    expect(result.documents[1]).toHaveProperty("action", "merge");
    expect(result.documents[1]).toHaveProperty("dstPath", "users/1234/posts/987");
    expect(result.documents[1].instructions).toEqual({
      "@followers": "arr+(1234)",
    });

    logicFn = viewLogics.createViewLogicFn(vd3);

    result = await logicFn[1]({
      ...createLogicResultDoc,
      dstPath: "servers/123#createdBy",
    });
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "create");
    expect(result.documents[0])
      .toHaveProperty("dstPath", "users/1234/@views/servers+123+createdBy");
    expect(result.documents[0].doc).toEqual({
      path: "servers/123#createdBy",
      srcProps: ["avatarUrl", "friendCount", "likes", "name"],
      destEntity: "server",
      destProp: "createdBy",
    });
  });

  it("should delete @views doc", async () => {
    let logicFn = viewLogics.createViewLogicFn(vd1);

    let result = await logicFn[1](deleteLogicResultDoc);
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "delete");
    expect(result.documents[0]).toHaveProperty("dstPath", "users/1234/@views/users+1234");

    logicFn = viewLogics.createViewLogicFn(vd2);

    result = await logicFn[1]({
      ...deleteLogicResultDoc,
      dstPath: "users/1234/posts/987#followers[1234]",
    });
    expect(result.documents.length).toEqual(2);
    expect(result.documents[0]).toHaveProperty("action", "delete");
    expect(result.documents[0])
      .toHaveProperty("dstPath", "users/1234/@views/users+1234+posts+987+followers[1234]");
    expect(result.documents[1]).toHaveProperty("action", "merge");
    expect(result.documents[1]).toHaveProperty("dstPath", "users/1234/posts/987");
    expect(result.documents[1].instructions).toEqual({
      "@followers": "arr-(1234)",
    });

    logicFn = viewLogics.createViewLogicFn(vd3);

    result = await logicFn[1]({
      ...deleteLogicResultDoc,
      dstPath: "servers/123#createdBy",
    });
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "delete");
    expect(result.documents[0])
      .toHaveProperty("dstPath", "users/1234/@views/servers+123+createdBy");
  });

  it("should not build @views doc when action is merge and @viewsAlreadyBuilt is true", async () => {
    docGetMock.mockResolvedValue({
      data: () => {
        return {
          "@viewsAlreadyBuilt+friend": true,
        };
      },
    });
    colGetMock.mockResolvedValue({
      docs: [],
    });

    // Create the logic function using the viewDefinition
    const logicFn = viewLogics.createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn[0](mergeLogicResultDoc);

    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(docUpdateMock).not.toHaveBeenCalled();

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(0);
  });

  it("should not build @views doc when action is delete and @viewsAlreadyBuilt is true", async () => {
    docGetMock.mockResolvedValue({
      data: () => {
        return {};
      },
    });
    colGetMock.mockResolvedValue({
      docs: [],
    });
    const newDeleteLogicResultDoc = {
      ...deleteLogicResultDoc,
      doc: {
        "@viewsAlreadyBuilt+friend": true,
      },
    };

    // Create the logic function using the viewDefinition
    const logicFn = viewLogics.createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn[0](newDeleteLogicResultDoc);

    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(docUpdateMock).not.toHaveBeenCalled();

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(0);
  });

  it("should update @views doc when srcProps is not equal", async () => {
    colGetMock.mockResolvedValue({
      docs: [{
        id: "users+456+friends+1234",
        ref: {
          path: "users/1234/@views/users+456+friends+1234",
        },
        data: () => {
          return {
            "path": "users/456/friends/1234",
            "srcProps": ["name", "avatar"],
          };
        },
      }, {
        id: "users+789+friends+1234",
        ref: {
          path: "users/1234/@views/users+789+friends+1234",
        },
        data: () => {
          return {
            "path": "users/789/friends/1234",
            "srcProps": ["name", "avatar"],
          };
        },
      }],
    });

    // Create the logic function using the viewDefinition
    const logicFn = viewLogics.createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn[0](mergeLogicResultDoc);

    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(docSpy).toHaveBeenCalledWith("users/1234/@views/users+456+friends+1234");
    expect(docSpy).toHaveBeenCalledWith("users/1234/@views/users+789+friends+1234");
    expect(docUpdateMock).toHaveBeenCalledTimes(2);
    expect(docUpdateMock).toHaveBeenCalledWith({
      "srcProps": [
        "age", "avatar", "name",
      ],
    });

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);
  });

  it("should recreate @views doc when id is incorrect", async () => {
    const deleteMock = jest.fn();
    colGetMock.mockResolvedValue({
      docs: [{
        id: "users+456+friends+1234",
        ref: {
          path: "users/1234/@views/users+456+friends+1234",
        },
        data: () => {
          return {
            "path": "users/456/friends/1234",
            "srcProps": ["name", "avatar"],
          };
        },
      }, {
        id: "1234+friend",
        ref: {
          delete: deleteMock,
        },
        data: () => {
          return {
            "path": "users/789/friends/1234",
            "srcProps": ["name", "avatar"],
          };
        },
      }],
    });

    // Create the logic function using the viewDefinition
    const logicFn = viewLogics.createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn[0](mergeLogicResultDoc);

    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(docSpy).toHaveBeenCalledWith("users/1234/@views/users+456+friends+1234");
    expect(docUpdateMock).toHaveBeenCalledTimes(1);
    expect(docUpdateMock).toHaveBeenCalledWith({
      "srcProps": [
        "age", "avatar", "name",
      ],
    });

    expect(deleteMock).toHaveBeenCalledTimes(1);
    expect(docSpy).toHaveBeenCalledWith("users/1234/@views/users+789+friends+1234");
    expect(docSetMock).toHaveBeenCalledTimes(1);
    expect(docSetMock).toHaveBeenCalledWith({
      "path": "users/789/friends/1234",
      "srcProps": ["age", "avatar", "name"],
      "destEntity": "friend",
    });

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);
  });

  it("should create a logic function that processes the given logicResultDoc and view definition", async () => {
    colGetMock.mockResolvedValueOnce({
      docs: [{
        id: "users+456+friends+1234",
        ref: {
          path: "users/1234/@views/users+456+friends+1234",
        },
        data: () => {
          return {
            "path": "users/456/friends/1234",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }, {
        id: "users+789+friends+1234",
        ref: {
          path: "users/1234/@views/users+789+friends+1234",
        },
        data: () => {
          return {
            "path": "users/789/friends/1234",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }],
    })
      .mockResolvedValueOnce({
        docs: [{
          id: "users+1234+posts+987+followers[1234]",
          ref: {
            path: "users/1234/@views/users+1234+posts+987+followers",
          },
          data: () => {
            return {
              "path": "users/1234/posts/987+followers[1234]",
              "srcProps": ["name", "avatar"],
            };
          },
        }, {
          id: "users+1234+posts+654+followers[1234]",
          ref: {
            path: "users/1234/@views/users+1234+posts+654+followers[1234]",
          },
          data: () => {
            return {
              "path": "users/1234/posts/654+followers[1234]",
              "srcProps": ["name", "avatar"],
            };
          },
        }, {
          id: "users+890+posts+987+followers[1234]",
          ref: {
            path: "users/1234/@views/users+890+posts+987+followers[1234]",
          },
          data: () => {
            return {
              "path": "users/890/posts/987+followers[1234]",
              "srcProps": ["name", "avatar"],
            };
          },
        }, {
          id: "users+890+posts+654+followers[1234]",
          ref: {
            path: "users/1234/@views/users+890+posts+654+followers[1234]",
          },
          data: () => {
            return {
              "path": "users/890/posts/654+followers[1234]",
              "srcProps": ["name", "avatar"],
            };
          },
        }],
      })
      .mockResolvedValueOnce({
        docs: [{
          id: "users+456+friends+1234",
          ref: {
            path: "users/1234/@views/users+456+friends+1234",
          },
          data: () => {
            return {
              "path": "users/456/friends/1234",
              "srcProps": ["name", "avatarUrl"],
            };
          },
        }, {
          id: "users+789+friends+1234",
          ref: {
            path: "users/1234/@views/users+789+friends+1234",
          },
          data: () => {
            return {
              "path": "users/789/friends/1234",
              "srcProps": ["name", "avatarUrl"],
            };
          },
        }],
      })
      .mockResolvedValueOnce({
        docs: [{
          id: "servers+123+createdBy",
          ref: {
            path: "users/123/@views/servers+123+createdBy",
          },
          data: () => {
            return {
              "path": "servers/123+createdBy",
              "srcProps": ["name", "avatarUrl"],
            };
          },
        }, {
          id: "servers+456+createdBy",
          ref: {
            path: "users/123/@views/servers+456+createdBy",
          },
          data: () => {
            return {
              "path": "servers/456+createdBy",
              "srcProps": ["name", "avatarUrl"],
            };
          },
        }],
      });

    // Create the logic function using the viewDefinition
    const logicFn = viewLogics.createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn[0](mergeLogicResultDoc);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/456/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe", "@updatedByViewDefinitionAt": expect.any(Timestamp)});
    expect(document.instructions).toEqual({"age": "++"});

    document = result.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/789/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe", "@updatedByViewDefinitionAt": expect.any(Timestamp)});
    expect(document.instructions).toEqual({"age": "++"});

    // Create the logic function using the viewDefinition
    const logicFn2 = viewLogics.createViewLogicFn(vd2);

    // Call the logic function with the test action
    const result2 = await logicFn2[0](mergeLogicResultDoc);

    // Add your expectations here, e.g., result.documents should have the correct properties and values
    expect(result2).toBeDefined();
    expect(result2.documents).toBeDefined();
    expect(result2.documents.length).toEqual(4);

    document = result2.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/1234/posts/987+followers[1234]");
    expect(document.doc).toEqual({"name": "John Doe", "@updatedByViewDefinitionAt": expect.any(Timestamp)});

    document = result2.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/1234/posts/654+followers[1234]");
    expect(document.doc).toEqual({"name": "John Doe", "@updatedByViewDefinitionAt": expect.any(Timestamp)});

    document = result2.documents[2];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/987+followers[1234]");
    expect(document.doc).toEqual({"name": "John Doe", "@updatedByViewDefinitionAt": expect.any(Timestamp)});

    document = result2.documents[3];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/654+followers[1234]");
    expect(document.doc).toEqual({"name": "John Doe", "@updatedByViewDefinitionAt": expect.any(Timestamp)});

    const resultDelete = await logicFn[0](deleteLogicResultDoc);

    expect(resultDelete).toBeDefined();
    expect(resultDelete.documents).toBeDefined();
    expect(resultDelete.documents.length).toEqual(2);

    document = resultDelete.documents[0];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/456/friends/1234");

    document = resultDelete.documents[1];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/789/friends/1234");

    // Create the logic function using the viewDefinition
    const logicFn3 = viewLogics.createViewLogicFn(vd3);

    // Call the logic function with the test action
    const result3 = await logicFn3[0](userLogicResultDoc);

    expect(result3).toBeDefined();
    expect(result3.documents).toBeDefined();
    expect(result3.documents.length).toEqual(2);

    document = result3.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "servers/123+createdBy");
    expect(document.doc).toEqual({"name": "new_name", "friendCount": 0, "likes": 24, "@updatedByViewDefinitionAt": expect.any(Timestamp)});

    document = result3.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "servers/456+createdBy");
    expect(document.doc).toEqual({"name": "new_name", "friendCount": 0, "likes": 24, "@updatedByViewDefinitionAt": expect.any(Timestamp)});
  });

  it("should use doc values when placeholders are not in dstPath", async () => {
    const logicFn = viewLogics.createViewLogicFn( {
      srcEntity: "menuItem",
      srcProps: ["name", "avatarUrl"],
      destEntity: "prepAreaMenuItem",
      destProp: {
        name: "createdBy",
        type: "map",
      },
    });

    const dstPath = "topics/topic21/prepAreas/prepArea2/menus/prepAreaMenuItem34";
    const logicResultDoc: LogicResultDoc = {
      action: "create",
      dstPath,
      doc: {
        "name": "Sample Menu",
        "@id": "menuItem789",
        "orderId": "order123",
      },
      priority: "normal",
    };
    const result = await logicFn[1](logicResultDoc);

    expect(result.documents[0].dstPath).toBe("topics/topic21/orders/order123/menus/menuItem789/@views/topics+topic21+prepAreas+prepArea2+menus+prepAreaMenuItem34");
  });

  it("should correctly map placeholders from dstPath into srcPath", async () => {
    const logicFn = viewLogics.createViewLogicFn( {
      srcEntity: "prepAreaMenuItem",
      srcProps: ["name", "avatarUrl"],
      destEntity: "prepAreaMenuItem",
      destProp: {
        name: "createdBy",
        type: "map",
      },
    });

    const dstPath = "topics/topic21/prepAreas/prepArea2/menus/prepAreaMenuItem34#orderItem";
    const logicResultDoc: LogicResultDoc = {
      action: "create",
      dstPath,
      doc: {
        "name": "Sample Menu",
        "@id": "menuItem789",
        "topicId": "topic22",
      },
    };
    const result = await logicFn[1](logicResultDoc);

    console.log(result);
    expect(result.documents[0].dstPath).toBe("topics/topic22/prepAreas/prepArea2/menus/menuItem789/@views/topics+topic21+prepAreas+prepArea2+menus+prepAreaMenuItem34+orderItem");
  });

  it("should return an error if srcPath has a placeholder", async () => {
    const logicFn = viewLogics.createViewLogicFn( {
      srcEntity: "menuItem",
      srcProps: ["name", "avatarUrl"],
      destEntity: "prepAreaMenuItem",
      destProp: {
        name: "createdBy",
        type: "map",
      },
    });

    const dstPath = "topics/topic21/prepAreas/prepArea2/menus/prepAreaMenuItem34#orderItem";
    const logicResultDoc: LogicResultDoc = {
      action: "create",
      dstPath,
      doc: {
        "name": "Sample Menu",
        "@id": "menuItem789",
      },
    };
    const result = await logicFn[1](logicResultDoc);
    expect(result.status).toBe("error");
    expect(result.message).toBe("srcPath should not have a placeholder");
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
    await viewLogics.queueRunViewLogics(doc1);

    expect(publishMessageSpy).toHaveBeenCalledWith({json: doc1});
  });
});

describe("onMessageViewLogicsQueue", () => {
  let runViewLogicsSpy: jest.SpyInstance;
  let expandConsolidateAndGroupByDstPathSpy: jest.SpyInstance;
  let distributeSpy: jest.SpyInstance;
  let createMetricExecutionSpy: jest.SpyInstance;

  const viewLogicsResult: LogicResult[] = [
    {
      name: "logic 1",
      timeFinished: firestore.Timestamp.now(),
      status: "finished",
      execTime: 100,
      documents: [
        {
          action: "merge",
          priority: "normal",
          dstPath: "users/doc1",
          doc: {field1: "value1"},
          instructions: {field2: "++"},
        },
        {action: "delete", priority: "normal", dstPath: "users/doc2"},
      ],
    },
    {
      name: "logic 2",
      timeFinished: firestore.Timestamp.now(),
      status: "finished",
      execTime: 100,
      documents: [
        {
          action: "merge",
          priority: "normal",
          dstPath: "users/doc1",
          doc: {field1: "value1a"},
          instructions: {field2: "--"},
        },
        {
          action: "merge",
          priority: "normal",
          dstPath: "users/doc1",
          doc: {field3: "value3"},
          instructions: {field4: "--"},
        },
        {action: "copy", priority: "normal", srcPath: "users/doc3", dstPath: "users/doc4"},
        {action: "merge", priority: "normal", dstPath: "users/doc2", doc: {field4: "value4"}},
        {action: "merge", priority: "normal", dstPath: "users/doc7", doc: {field6: "value7"}},
      ],
    },
  ];
  const expandConsolidateResult = new Map<string, LogicResultDoc[]>([
    ["users/doc1", [{
      action: "merge",
      priority: "normal",
      dstPath: "users/doc1",
      doc: {field1: "value1a", field3: "value3"},
      instructions: {field2: "--", field4: "--"},
    }]],
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
    createMetricExecutionSpy = jest.spyOn(indexUtils._mockable, "createMetricExecution").mockResolvedValue();
    runViewLogicsSpy = jest.spyOn(indexUtils, "runViewLogics").mockResolvedValue(viewLogicsResult);
    expandConsolidateAndGroupByDstPathSpy = jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(expandConsolidateResult);
    distributeSpy = jest.spyOn(indexUtils, "distributeFnNonTransactional").mockResolvedValue();
  });

  it("should skip duplicate message", async () => {
    isProcessedMock.mockResolvedValueOnce(true);
    jest.spyOn(console, "log").mockImplementation();
    await viewLogics.onMessageViewLogicsQueue(event);

    expect(isProcessedMock).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME, event.id);
    expect(console.log).toHaveBeenCalledWith("Skipping duplicate message");
  });

  it("should distribute queued view logics", async () => {
    const distributeFnLogicResult: LogicResult = {
      name: "runViewLogics",
      status: "finished",
      documents: [],
      execTime: expect.any(Number),
    };
    const viewLogicsResultDocs = viewLogicsResult.map((logicResult) => logicResult.documents).flat();
    const result = await viewLogics.onMessageViewLogicsQueue(event);

    expect(runViewLogicsSpy).toHaveBeenCalledWith(doc1);
    expect(createMetricExecutionSpy).toHaveBeenCalledWith([...viewLogicsResult, distributeFnLogicResult]);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalledWith(viewLogicsResultDocs);
    expect(distributeSpy).toHaveBeenCalledWith(expandConsolidateResult);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed view logics");
  });
});
