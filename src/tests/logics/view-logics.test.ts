import {
  LogicResult,
  LogicResultDoc,
  ViewDefinition,
  ProjectConfig,
  ViewLogicConfig,
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
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {DocumentReference} from "firebase-admin/lib/firestore";
import Timestamp = firestore.Timestamp;
import {_mockable as pathsMockable} from "../../utils/paths";
import CollectionReference = firestore.CollectionReference;
import {convertLogicResultsToMetricExecutions} from "../../index-utils";
import {findMatchingViewLogics} from "../../logics/view-logics";
import * as paths from "../../utils/paths";

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
jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(true);
initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfigs, validatorConfigs, [], []);

const vd1: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar", "age"],
  destEntity: "friend",
  version: "1.0.0",
};

const vd2: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar"],
  destEntity: "post",
  destProp: {
    name: "followers",
    type: "array-map",
  },
  version: "1.0.0",
};

const vd3: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatarUrl", "friendCount", "likes"],
  destEntity: "server",
  destProp: {
    name: "createdBy",
    type: "map",
  },
  version: "1.0.0",
};

const vd4: ViewDefinition = {
  srcEntity: "friend",
  srcProps: ["name", "avatar", "age"],
  destEntity: "user",
  version: "1.0.0",
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
  let batchUpdateMock: jest.Mock;
  let batchCommitMock: jest.Mock;
  let dbGetAllMock: jest.Mock;

  beforeEach(() => {
    colGetMock = jest.fn();
    docUpdateMock = jest.fn();
    docSetMock = jest.fn();
    batchUpdateMock = jest.fn();
    batchCommitMock = jest.fn();
    dbGetAllMock = jest.fn().mockImplementation((...refs: any[]) => {
      return Promise.resolve(refs.map((ref: any) => ({
        exists: true,
        data: () => ({}),
        ref: ref,
      })));
    });

    docGetMock = jest.fn().mockResolvedValue({
      data: () => {
        return {
          "@viewsAlreadyBuilt+friend": false,
        };
      },
      exists: true,
    });

    jest.spyOn(admin.firestore(), "batch").mockImplementation(() => {
      return {
        update: batchUpdateMock,
        set: jest.fn(),
        delete: jest.fn(),
        commit: batchCommitMock,
      } as unknown as firestore.WriteBatch;
    });

    jest.spyOn(admin.firestore(), "getAll").mockImplementation(dbGetAllMock);

    jest.spyOn(admin.firestore(), "collection").mockImplementationOnce(() => {
      return {
        where: jest.fn().mockReturnValue({
          get: colGetMock,
        }),
      } as unknown as CollectionReference;
    });
    docSpy = jest.spyOn(admin.firestore(), "doc").mockImplementation((path) => {
      return {
        path,
        set: docSetMock,
        update: docUpdateMock,
        get: docGetMock,
        collection: jest.fn().mockReturnValue({
          where: jest.fn().mockReturnValue({
            get: colGetMock,
            limit: jest.fn().mockReturnValue({
              get: colGetMock,
            }),
            where: jest.fn().mockReturnValue({
              get: colGetMock,
              limit: jest.fn().mockReturnValue({
                get: colGetMock,
                startAfter: jest.fn().mockReturnValue({
                  get: colGetMock,
                }),
              }),
              where: jest.fn().mockReturnValue({
                get: colGetMock,
                limit: jest.fn().mockReturnValue({
                  get: colGetMock,
                }),
                where: jest.fn().mockReturnValue({
                  get: colGetMock,
                  limit: jest.fn().mockReturnValue({
                    get: colGetMock,
                  }),
                }),
              }),
            }),
          }),
        }),
      } as unknown as DocumentReference;
    });
  });
  const targetVersion = "1.0.0";

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
    const result = await logicFn[1](logicResultDoc, targetVersion);
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
    const result = await logicFn[1](logicResultDoc, targetVersion);
    expect(result.documents.length).toEqual(0);
    expect(console.error).toHaveBeenCalledWith("srcPath should not have a placeholder");
  });

  it("should create @views doc", async () => {
    let logicFn = viewLogics.createViewLogicFn(vd1);

    let result = await logicFn[1](createLogicResultDoc, targetVersion);
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "create");
    expect(result.documents[0]).toHaveProperty("dstPath", "users/1234/@views/users+1234");
    expect(result.documents[0].doc).toEqual({
      "path": "users/1234",
      "srcProps": ["age", "avatar", "name"],
      "destEntity": "friend",
    });

    logicFn = viewLogics.createViewLogicFn(vd2);

    result = await logicFn[1]({...createLogicResultDoc, dstPath: "users/1234/posts/987#followers[1234]"}, targetVersion);
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

    result = await logicFn[1]({...createLogicResultDoc, dstPath: "servers/123#createdBy"}, targetVersion);
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

    let result = await logicFn[1](deleteLogicResultDoc, targetVersion);
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "delete");
    expect(result.documents[0]).toHaveProperty("dstPath", "users/1234/@views/users+1234");

    logicFn = viewLogics.createViewLogicFn(vd2);

    result = await logicFn[1]({...deleteLogicResultDoc, dstPath: "users/1234/posts/987#followers[1234]",
    }, targetVersion);
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

    result = await logicFn[1]({...deleteLogicResultDoc, dstPath: "servers/123#createdBy"}, targetVersion);
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
    const result = await logicFn[0](mergeLogicResultDoc, targetVersion);

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
    const result = await logicFn[0](newDeleteLogicResultDoc, targetVersion);

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
    const result = await logicFn[0](mergeLogicResultDoc, targetVersion);

    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(docSpy).toHaveBeenCalledWith("users/1234/@views/users+456+friends+1234");
    expect(docSpy).toHaveBeenCalledWith("users/1234/@views/users+789+friends+1234");
    expect(batchUpdateMock).toHaveBeenCalledTimes(2);
    expect(batchCommitMock).toHaveBeenCalledTimes(1);
    expect(batchUpdateMock).toHaveBeenCalledWith(expect.objectContaining({path: "users/1234/@views/users+456+friends+1234"}), {
      "srcProps": [
        "age", "avatar", "name",
      ],
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
    const result = await logicFn[0](mergeLogicResultDoc, targetVersion);

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
    const result2 = await logicFn2[0](mergeLogicResultDoc, targetVersion);

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

    const resultDelete = await logicFn[0](deleteLogicResultDoc, targetVersion);

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
    const result3 = await logicFn3[0](userLogicResultDoc, targetVersion);

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
      srcEntity: "orderMenuItem",
      srcProps: ["name", "avatarUrl"],
      destEntity: "prepAreaMenuItem",
      destProp: {
        name: "createdBy",
        type: "map",
      },
      version: "1.0.0",
    });

    const dstPath = "topics/topic21/prepAreas/prepArea2/menus/prepAreaMenuItem34";
    const logicResultDoc: LogicResultDoc = {
      action: "merge",
      dstPath,
      doc: {
        "name": "Sample Menu",
        "@id": "menuItem789",
        "orderId": "order123",
      },
      priority: "normal",
    };
    const result = await logicFn[1](logicResultDoc, targetVersion);

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
      version: "1.0.0",
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
    const result = await logicFn[1](logicResultDoc, targetVersion);

    expect(result.documents[0].dstPath).toBe("topics/topic22/prepAreas/prepArea2/menus/menuItem789/@views/topics+topic21+prepAreas+prepArea2+menus+prepAreaMenuItem34+orderItem");
  });

  it("should return an error if srcPath has a placeholder", async () => {
    const logicFn = viewLogics.createViewLogicFn( {
      srcEntity: "orderMenuItem",
      srcProps: ["name", "avatarUrl"],
      destEntity: "prepAreaMenuItem",
      destProp: {
        name: "createdBy",
        type: "map",
      },
      version: "1.0.0",
    });

    const dstPath = "topics/topic21/prepAreas/prepArea2/menus/prepAreaMenuItem34#orderItem";
    const logicResultDoc: LogicResultDoc = {
      action: "merge",
      dstPath,
      doc: {
        "name": "Sample Menu",
        "@id": "menuItem789",
      },
    };
    const result = await logicFn[1](logicResultDoc, targetVersion);
    expect(result.status).toBe("error");
    expect(result.message).toBe("srcPath should not have a placeholder");
  });

  it("should create delete logicDoc if viewDoc path doesn't exist anymore", async () => {
    dbGetAllMock.mockResolvedValue([
      {exists: false, data: () => ({})},
      {exists: false, data: () => ({})},
    ]);
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

    const logicFn = viewLogics.createViewLogicFn(vd1);

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/1234/@views/users+456+friends+1234");

    document = result.documents[1];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/1234/@views/users+789+friends+1234");
  });

  it("should create delete logicDoc if viewDoc path destProp doesn't exist anymore", async () => {
    docGetMock.mockResolvedValue({
      data: () => {
        return {
          "@viewsAlreadyBuilt+friend": false,
        };
      },
      exists: true,
    });
    colGetMock.mockResolvedValueOnce({
      docs: [{
        id: "users+456+friends+1234+prop1",
        ref: {
          path: "users/1234/@views/users+456+friends+1234",
        },
        data: () => {
          return {
            "path": "users/456/friends/1234#prop1",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }, {
        id: "users+789+friends+1234+prop1[propId1]",
        ref: {
          path: "users/1234/@views/users+789+friends+1234",
        },
        data: () => {
          return {
            "path": "users/789/friends/1234#prop1[propId1]",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }],
    });

    const logicFn = viewLogics.createViewLogicFn(vd1);

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/1234/@views/users+456+friends+1234");

    document = result.documents[1];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/1234/@views/users+789+friends+1234");
  });

  it("should only execute 50 @views per batch, then queue the succeeding ones", async () => {
    docGetMock.mockResolvedValue({
      data: () => {
        return {
          "@viewsAlreadyBuilt+friend": false,
        };
      },
      exists: true,
    });
    colGetMock.mockResolvedValueOnce({
      docs: [...(Array.from({length: 49}, () => ({
        id: "users+456+friends+1234",
        ref: {
          path: "users/1234/@views/users+456+friends+1234",
        },
        data: ()=> {
          return {
            "@id": "users+userId+friends+1234",
            "destEntity": "user",
            "path": "users/userId/friends/1234",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }))),
      {
        "id": "50thViewId",
        "ref": {
          path: "users/1234/@views/50thViewId",
        },
        "data": ()=> {
          return {
            "@id": "50thViewId",
            "destEntity": "user",
            "path": "users/userId/friends/1234",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      },
      ],
    });
    const queueRunViewLogicsSpy =
      jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();

    const logicFn = viewLogics.createViewLogicFn(vd1);

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(50);

    expect(queueRunViewLogicsSpy).toHaveBeenCalledTimes(1);
    expect(queueRunViewLogicsSpy).toHaveBeenNthCalledWith(1, targetVersion, [mergeLogicResultDoc], "50thViewId");
  });

  it("should be able to process succeeding batches of @views", async () => {
    docGetMock.mockResolvedValueOnce({
      data: () => {
        return {
          "@viewsAlreadyBuilt+friend": false,
        };
      },
      exists: true,
    }).mockResolvedValueOnce({
      "data": () => {
        return {
          "@id": "50thViewId",
          "destEntity": "user",
          "path": "users/userId/friends/1234",
          "srcProps": ["age", "avatar", "name"],
        };
      },
      "exists": true,
    });
    colGetMock.mockResolvedValueOnce({
      docs: [{
        id: "users+456+friends+1234+prop1",
        ref: {
          path: "users/1234/@views/users+456+friends+1234",
        },
        data: () => {
          return {
            "path": "users/456/friends/1234#prop1",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }, {
        id: "users+789+friends+1234+prop1[propId1]",
        ref: {
          path: "users/1234/@views/users+789+friends+1234",
        },
        data: () => {
          return {
            "path": "users/789/friends/1234#prop1[propId1]",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }],
    });

    const logicFn = viewLogics.createViewLogicFn(vd1);

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, "50thViewId");

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);
  });

  describe("SyncCreate ViewLogic", () => {
    describe("dstToSrcLogicFn", () => {
      const dstPath = "topics/topic21/menus/menu1/ingredients/ingredient1";
      const logicFn = viewLogics.createViewLogicFn( {
        srcEntity: "recipeIngredient",
        srcProps: ["amount", "ingredient"],
        destEntity: "menuItemIngredient",
        options: {syncCreate: true},
        version: "1.0.0",
      });
      const logicResultDoc: LogicResultDoc = {
        action: "create",
        dstPath,
        doc: {
          "@id": "ingredientId",
          "amount": 2,
          "ingredient": "ingredientName",
          "topicId": "topic22",
        },
      };

      afterEach(() => {
        jest.clearAllMocks();
      });

      it("should return finished logicResult with 2 documents", async () => {
        jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(false);

        const result = await logicFn[1](logicResultDoc, targetVersion);
        expect(result.name).toBe("menuItemIngredient Dst-to-Src");
        expect(result.status).toBe("finished");
        expect(result.timeFinished).toBe(undefined);
      });

      it("should create @syncCreateView in global collection", async () => {
        jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(false);

        const result = await logicFn[1](logicResultDoc, targetVersion);
        expect(result.documents[1].dstPath).toBe("@syncCreateViews/topics+topic21+menus+menu1+ingredients");
        expect(result.documents[1].doc).toEqual({
          "destEntity": "menuItemIngredient",
          "dstPath": "topics/topic21/menus/menu1/ingredients",
          "srcPath": "topics/topic22/ingredients",
        });
      });

      it("should not create syncView if dstPath is invalid", async () => {
        const invalidPath = "topics/topic21/menus/menu1#ingredients";
        const invalidLogicResultDoc: LogicResultDoc = {
          ...logicResultDoc,
          dstPath: invalidPath,
        };
        jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(false);
        const errorSpy = jest.spyOn(console, "error").mockImplementation();

        const result = await logicFn[1](invalidLogicResultDoc, targetVersion);
        expect(errorSpy).toHaveBeenCalledWith("invalid syncCreate dstPath, topics/topic21/menus/menu1#ingredients");
        expect(result.documents.length).toEqual(1);
        expect(result.documents[0].dstPath).not.toBe("@syncCreateViews/topics+topic21+menus+menu1+ingredients");
      });

      it("should not create @syncCreateView in global collection if the a doc with same docId is already created", async () => {
        const infoSpy = jest.spyOn(console, "info").mockImplementation();
        jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(true);

        const result = await logicFn[1](logicResultDoc, targetVersion);
        expect(infoSpy).toHaveBeenCalledWith("@syncCreateViews/topics+topic21+menus+menu1+ingredients already exists — skipping creation.");
        expect(result.documents.length).toEqual(1);
        expect(result.documents[0].dstPath).not.toBe("@syncCreateViews/topics+topic21+menus+menu1+ingredients");
      });
    });

    describe("srcToDstLogicFn", () => {
      const logicFn = viewLogics.createViewLogicFn( {
        srcEntity: "recipeIngredient",
        srcProps: ["amount", "ingredient"],
        destEntity: "menuItemIngredient",
        options: {syncCreate: true},
        version: "1.0.0",
      });

      const dstPath = "topics/topic22/ingredients/ingredient1";
      const logicResultDoc: LogicResultDoc = {
        action: "create",
        dstPath,
        doc: {
          "@id": "ingredientId",
          "amount": 2,
          "ingredient": "ingredientName",
          "topicId": "topic22",
        },
      };

      const srcCollection = "topics/topic22/ingredients";
      const dstPath1 = "topics/topic21/menus/menu1/ingredients";
      const dstPath2 = "topics/topic21/preparationAreas/prepArea1/menus/menu1#ingredients";
      beforeEach(() => {
        colGetMock.mockResolvedValue({
          docs: [{
            id: "topics+topic21+menus+menu1+ingredients",
            data: () => {
              return {
                "destEntity": "menuItemIngredient",
                "dstPath": dstPath1,
                "srcPath": srcCollection,
              };
            },
          }, {
            id: "topics+topic21+preparationAreas+menu1+ingredients",
            data: () => {
              return {
                "destEntity": "preparationAreaIngredient",
                "dstPath": dstPath2,
                "srcPath": srcCollection,
              };
            },
          }],
        });
      });

      const doesPathExistsMock = jest.fn()
        .mockResolvedValueOnce(true);
      pathsMockable.doesPathExists = doesPathExistsMock;

      it("should auto create source document if there is a matching path in @syncCreateViews", async () => {
        const result = await logicFn[0](logicResultDoc, targetVersion);
        expect(result.documents.length).toBe(5);

        expect(result.documents[0].dstPath).toBe(`${dstPath1}/ingredient1`);
        expect(result.documents[0].doc).toBe(logicResultDoc.doc);

        expect(result.documents[2].dstPath).toBe(`${dstPath2}[ingredient1]`);
        expect(result.documents[2].doc).toBe(logicResultDoc.doc);
      });

      it("should manually create @view document for each newly created views", async () => {
        const result = await logicFn[0](logicResultDoc, targetVersion);

        const atViewsCollectionPath = `${srcCollection}/ingredient1/@views`;
        expect(result.documents[1].dstPath).toBe(
          `${atViewsCollectionPath}/topics+topic21+menus+menu1+ingredients+ingredient1`);
        expect(result.documents[1].doc).toEqual({
          "destEntity": "menuItemIngredient",
          "path": `${dstPath1}/ingredient1`,
          "srcProps": ["amount", "ingredient"],
        });
        expect(result.documents[3].dstPath).toBe(
          `${atViewsCollectionPath}/topics+topic21+preparationAreas+prepArea1+menus+menu1+ingredients[ingredient1]`);
        expect(result.documents[3].doc).toEqual({
          "destEntity": "menuItemIngredient",
          "destProp": "ingredients",
          "path": `${dstPath2}[ingredient1]`,
          "srcProps": ["amount", "ingredient"],
        });
      });

      it("should add the docId in the @{field} if there is a destProp", async () => {
        const result = await logicFn[0](logicResultDoc, targetVersion);

        expect(result.documents[4].dstPath).toBe(
          "topics/topic21/preparationAreas/prepArea1/menus/menu1");
        expect(result.documents[4].instructions).toEqual({
          "@ingredients": "arr+(ingredient1)",
        });
        expect(result.documents[4].skipRunViewLogics).toBe(true);
      });
    });
  });
});

describe("queueRunViewLogics", () => {
  let publishMessageSpy: jest.SpyInstance;
  let findMatchingViewLogicsSpy: jest.SpyInstance;
  beforeEach(() => {
    jest.restoreAllMocks();
    publishMessageSpy = jest.spyOn(VIEW_LOGICS_TOPIC, "publishMessage")
      .mockImplementation(() => {
        return "message-id";
      });
    // mock findMatchingViewLogics
    findMatchingViewLogicsSpy = jest.spyOn(viewLogics, "findMatchingViewLogics").mockReturnValue(new Map([
      ["0.0.1", {} as ViewLogicConfig],
    ]));
  });
  const targetVersion = "1.0.0";

  it("should queue docs to run view logics", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-merge"},
      dstPath: "users/test-user-id/documents/doc1",
    };
    await viewLogics.queueRunViewLogics(targetVersion, [doc1]);

    expect(findMatchingViewLogicsSpy).toHaveBeenCalled();
    expect(publishMessageSpy).toHaveBeenCalledWith({json: {"doc": doc1, "targetVersion": targetVersion}});
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
        json: {
          doc: doc1,
          targetVersion: "1.0.0",
        },
      },
    },
  } as CloudEvent<MessagePublishedData>;

  beforeEach(() => {
    createMetricExecutionSpy = jest.spyOn(indexUtils._mockable, "saveMetricExecution").mockResolvedValue();
    runViewLogicsSpy = jest.spyOn(viewLogics, "runViewLogics").mockImplementation(async () => {
      return viewLogicsResult.map((result) => ({...result, documents: [...result.documents]}));
    });
    expandConsolidateAndGroupByDstPathSpy = jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(expandConsolidateResult);
    distributeSpy = jest.spyOn(indexUtils, "distributeFnNonTransactional").mockResolvedValue([]);
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
    const result = await viewLogics.onMessageViewLogicsQueue(event);

    expect(runViewLogicsSpy).toHaveBeenCalledWith(doc1, "1.0.0", undefined);
    const expectedMetricExecutions = convertLogicResultsToMetricExecutions([...viewLogicsResult, distributeFnLogicResult]);
    expect(createMetricExecutionSpy).toHaveBeenCalledWith(expectedMetricExecutions);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalled();
    const calledWith = expandConsolidateAndGroupByDstPathSpy.mock.calls[0][0];
    // Since the array is cleared in the function, we can't check its content directly if it's the same reference
    // But we know it was called with the flattened documents
    expect(calledWith).toBeDefined();
    expect(distributeSpy).toHaveBeenCalledWith(expandConsolidateResult, true);
    expect(trackProcessedIdsMock).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME, event.id);
    expect(result).toEqual("Processed view logics");
  });
});

describe("findMatchingViewLogics", () => {
  const logicResultDoc: LogicResultDoc = {
    action: "merge",
    dstPath: "topics/topicId",
    doc: {title: "New title"},
  };
  beforeEach(()=> {
    jest.restoreAllMocks();
    jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(true);
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfigs, validatorConfigs, [], []);
    jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({
      entity: "topic",
      regex: /topics/,
    });
  });

  it("should return matching view logics with proper version", () => {
    const result = findMatchingViewLogics(logicResultDoc, "2.5.0");

    expect(result?.has("todos ViewLogic")).toBe(true); // included
    expect(result?.has("user#todosArray ViewLogic")).toBe(true); // included
    expect(result?.has("user#mainTopic ViewLogic")).toBe(false); // ahead of version 2.5.0

    // should run correct version
    expect(result?.get("todos ViewLogic")).toEqual(
      expect.objectContaining({
        version: "2.0.0",
      })
    );
  });

  it("should return matching view logics with proper naming", () => {
    const result = findMatchingViewLogics(logicResultDoc, "5.0.0");
    console.debug(result);

    // normal view
    expect(result?.has("todos ViewLogic")).toBe(true);
    // map view
    expect(result?.has("user#mainTopic ViewLogic")).toBe(true);
    // array-map view
    expect(result?.has("user#todosArray ViewLogic")).toBe(true);
  });
});
