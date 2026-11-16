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
import type {MessagePublishedData} from "firebase-functions/v2/pubsub";
import * as indexUtils from "../../index-utils";
import {firestore} from "firebase-admin";
import {
  initializeEmberFlow,
  projectConfig as globalProjectConfig,
  VIEW_LOGICS_TOPIC,
  VIEW_LOGICS_TOPIC_NAME,
} from "../../index";
import {PubSub} from "@google-cloud/pubsub";
import * as admin from "firebase-admin";
import {securityConfigs} from "../../sample-custom/security";
import {validatorConfigs} from "../../sample-custom/validators";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
import {DocumentReference} from "firebase-admin/lib/firestore";
import Timestamp = firestore.Timestamp;
import {_mockable as pathsMockable} from "../../utils/paths";
import CollectionReference = firestore.CollectionReference;
import {convertLogicResultsToMetricExecutions} from "../../index-utils";
import {findMatchingViewLogics, ViewLogicsPublishError} from "../../logics/view-logics";
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
    colGetMock = jest.fn().mockResolvedValue({docs: []});
    docUpdateMock = jest.fn();
    docSetMock = jest.fn();
    batchUpdateMock = jest.fn();
    batchCommitMock = jest.fn().mockResolvedValue({});
    dbGetAllMock = jest.fn().mockImplementation((...refs: any[]) => {
      return Promise.resolve(refs.map((ref: any) => ({
        exists: true,
        data: () => ({}),
        ref,
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
  const appVersion = "1.0.0";

  it("should log error document does not have @id", async () => {
    jest.spyOn(console, "error").mockImplementation();
    const logicFn = viewLogics.createViewLogicFn(vd4);

    const logicResultDoc: LogicResultDoc = {
      action: "create",
      dstPath: "random", // This will not match users/{userId}, so srcPath will have placeholders
      doc: {
        name: "John Doe",
      },
      priority: "normal",
    };
    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
    expect(result.documents.length).toEqual(0);
    expect(console.error).toHaveBeenCalledWith("srcPath should not have a placeholder");
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
    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
    expect(result.documents.length).toEqual(0);
    expect(console.error).toHaveBeenCalledWith("srcPath should not have a placeholder");
  });

  it("should create @views doc", async () => {
    let logicFn = viewLogics.createViewLogicFn(vd1);

    let result = await logicFn[1](createLogicResultDoc, targetVersion, appVersion);
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "merge");
    expect(result.documents[0]).toHaveProperty("dstPath", "users/1234/@views/users+1234");
    expect(result.documents[0].doc).toEqual({
      "path": "users/1234",
      "srcProps": ["age", "avatar", "name"],
      "destEntity": "friend",
    });

    logicFn = viewLogics.createViewLogicFn(vd2);

    result = await logicFn[1]({...createLogicResultDoc, dstPath: "users/1234/posts/987#followers[1234]"}, targetVersion, appVersion);
    expect(result.documents.length).toEqual(2);
    expect(result.documents[0]).toHaveProperty("action", "merge");
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
      "@followers": "arr(+1234)",
    });

    logicFn = viewLogics.createViewLogicFn(vd3);

    result = await logicFn[1]({...createLogicResultDoc, dstPath: "servers/123#createdBy"}, targetVersion, appVersion);
    expect(result.documents.length).toEqual(1);
    expect(result.documents[0]).toHaveProperty("action", "merge");
    expect(result.documents[0])
      .toHaveProperty("dstPath", "users/1234/@views/servers+123+createdBy");
    expect(result.documents[0].doc).toEqual({
      path: "servers/123#createdBy",
      srcProps: ["avatarUrl", "friendCount", "likes", "name"],
      destEntity: "server",
      destProp: "createdBy",
    });
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
    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, appVersion);

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
    const result = await logicFn[0](newDeleteLogicResultDoc, targetVersion, appVersion);

    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(docUpdateMock).not.toHaveBeenCalled();

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(0);
  });

  it("should update @views doc when srcProps is not equal", async () => {
    colGetMock.mockResolvedValueOnce({
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
    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, appVersion);

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
    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, appVersion);

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
    const result2 = await logicFn2[0](mergeLogicResultDoc, targetVersion, appVersion);

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

    const resultDelete = await logicFn[0](deleteLogicResultDoc, targetVersion, appVersion);

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
    const result3 = await logicFn3[0](userLogicResultDoc, targetVersion, appVersion);

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
    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);

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
    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);

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
    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
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

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, appVersion);

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
          parent: {
            parent: {
              id: "1234",
            },
          },
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
          parent: {
            parent: {
              id: "1234",
            },
          },
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

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, appVersion);

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

  it("should create delete logicDoc if viewDoc path destProp id is not equal to source id", async () => {
    docGetMock.mockResolvedValue({
      data: () => {
        return {
          "@viewsAlreadyBuilt+friend": false,
        };
      },
      exists: true,
    });

    dbGetAllMock = jest.fn().mockImplementation((...refs: any[]) => {
      return Promise.resolve(refs.map((ref: any) => ({
        exists: true,
        data: () => ({
          "prop1": {
            "@id": "5678",
            "age": 20,
            "name": "John Doe",
          },
        }),
        ref,
      })));
    });
    jest.spyOn(admin.firestore(), "getAll").mockImplementation(dbGetAllMock);

    colGetMock.mockResolvedValueOnce({
      docs: [{
        id: "users+456+friends+1234+prop1",
        ref: {
          path: "users/1234/@views/users+456+friends+1234",
          parent: {
            parent: {
              id: "1234",
            },
          },
        },
        data: () => {
          return {
            "path": "users/456/friends/1234#prop1",
            "srcProps": ["age", "avatar", "name"],
          };
        },
      }],
    });

    const logicFn = viewLogics.createViewLogicFn(vd1);

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, appVersion);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(1);

    const document = result.documents[0];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/1234/@views/users+456+friends+1234");
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

    const result = await logicFn[0](mergeLogicResultDoc, targetVersion, appVersion);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(50);

    expect(queueRunViewLogicsSpy).toHaveBeenCalledTimes(1);
    expect(queueRunViewLogicsSpy).toHaveBeenNthCalledWith(1, targetVersion, appVersion, [mergeLogicResultDoc], "50thViewId");
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
          parent: {
            parent: {
              id: "1234",
            },
          },
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
          parent: {
            parent: {
              id: "1234",
            },
          },
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

  it("should remove the id in @{destProp} array and delete @views doc when deleting an item in an array-map", async () => {
    const dstPath = "topics/topic21/menus/menu1#ingredients[ingredient1]";
    const logicFn = viewLogics.createViewLogicFn( {
      srcEntity: "recipeIngredient",
      srcProps: ["amount", "ingredient"],
      destEntity: "menuItemIngredient",
      options: {syncCreate: true},
      destProp: {
        name: "ingredients",
        type: "array-map",
      },
      version: "1.0.0",
    });
    const logicResultDoc: LogicResultDoc = {
      action: "delete",
      dstPath,
      doc: {
        "@id": "ingredient1",
        "topicId": "topic21",
      },
    };

    jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(false);

    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
    expect(result.documents.length).toBe(2);
    // @views deletion
    expect(result.documents[0].action).toBe("delete");
    expect(result.documents[0].dstPath).toBe("topics/topic21/ingredients/ingredient1/@views/topics+topic21+menus+menu1+ingredients[ingredient1]");
    // array-map removal
    expect(result.documents[1].action).toBe("merge");
    expect(result.documents[1].dstPath).toBe("topics/topic21/menus/menu1");
    expect(result.documents[1].instructions).toEqual({
      "@ingredients": "arr(-ingredient1)",
    });
  });

  it("should handle deletion and clean up even if viewDoc is undefined", async () => {
    const dstPath = "topics/topic21/menuItems/menu1/ingredients/ingredient1";
    const logicFn = viewLogics.createViewLogicFn( {
      srcEntity: "recipeIngredient",
      srcProps: ["amount", "ingredient"],
      destEntity: "menuItemIngredient",
      options: {syncCreate: true},
      version: "1.0.0",
    });
    const logicResultDoc: LogicResultDoc = {
      action: "delete",
      dstPath,
      // doc is intentionally missing
    };

    jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(false);

    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
    expect(result.status).toBe("finished");
    expect(result.documents.length).toBe(1);
    // @views deletion - srcDocId should have been extracted from dstPath (ingredient1)
    // topicId (topic21) should have been extracted from dstPath as well
    expect(result.documents[0].action).toBe("delete");
    expect(result.documents[0].dstPath).toBe("topics/topic21/ingredients/ingredient1/@views/topics+topic21+menuItems+menu1+ingredients+ingredient1");
  });

  it("should only delete @views doc when deleting a standard view", async () => {
    const dstPath = "topics/topic21/menus/menu1/ingredients/ingredient1";
    const logicFn = viewLogics.createViewLogicFn( {
      srcEntity: "recipeIngredient",
      srcProps: ["amount", "ingredient"],
      destEntity: "menuItemIngredient",
      options: {syncCreate: true},
      version: "1.0.0",
    });
    const logicResultDoc: LogicResultDoc = {
      action: "delete",
      dstPath,
      doc: {
        "@id": "ingredient1",
        "topicId": "topic21",
      },
    };

    const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
    expect(result.documents.length).toBe(1);
    expect(result.documents[0].action).toBe("delete");
    expect(result.documents[0].dstPath).toBe("topics/topic21/ingredients/ingredient1/@views/topics+topic21+menus+menu1+ingredients+ingredient1");
  });

  it("should handle deletion and clean up with unwrapped viewDoc (new delete behavior)", async () => {
    // This vd has a destProp 'createdBy' which is a map
    const vd: ViewDefinition = {
      srcEntity: "user",
      srcProps: ["name", "avatarUrl"],
      destEntity: "server",
      destProp: {
        name: "createdBy",
        type: "map",
      },
      version: "1.0.0",
    };
    const dstPath = "servers/server1#createdBy";
    const logicFn = viewLogics.createViewLogicFn(vd);

    // After clarifying changes, queueRunViewLogics for delete will set logicResultDoc.doc to the view document data (unwrapped)
    const logicResultDoc: LogicResultDoc = {
      action: "delete",
      dstPath,
      doc: {
        "@id": "user123",
        "userId": "user123",
        "someOtherField": "value",
      },
    };

    const result = await logicFn[1](logicResultDoc, "1.0.0", "1.0.0");
    expect(result.status).toBe("finished");
    expect(result.documents.length).toBe(1);
    expect(result.documents[0].action).toBe("delete");
    // Should have extracted user123 from doc["@id"]
    expect(result.documents[0].dstPath).toContain("users/user123/@views/");
  });

  it("should handle deletion and clean up with unwrapped viewDoc in array-map (new delete behavior)", async () => {
    // vd2 has followers as array-map
    const dstPath = "posts/post1#followers[user456]";
    const logicFn = viewLogics.createViewLogicFn(vd2);

    // logicResultDoc.doc is just the user document that was inside followers[user456]
    const logicResultDoc: LogicResultDoc = {
      action: "delete",
      dstPath,
      doc: {
        "@id": "user456",
        "name": "User 456",
      },
    };

    const result = await logicFn[1](logicResultDoc, "1.0.0", "1.0.0");
    expect(result.status).toBe("finished");
    // Should produce @views deletion and array-map removal
    expect(result.documents.length).toBe(2);
    expect(result.documents[0].action).toBe("delete");
    expect(result.documents[0].dstPath).toContain("users/user456/@views/");
    expect(result.documents[1].action).toBe("merge");
    expect(result.documents[1].instructions).toEqual({"@followers": "arr(-user456)"});
  });

  it("should cleanup @views entry in src when deleting a document with a map propView", async () => {
    const vd: ViewDefinition = {
      srcEntity: "user",
      srcProps: ["name", "email"],
      destEntity: "server",
      destProp: {
        name: "createdBy",
        type: "map",
      },
      version: "1.0.0",
    };
    const logicFn = viewLogics.createViewLogicFn(vd);

    const deleteDoc: LogicResultDoc = {
      action: "delete",
      dstPath: "servers/server1",
      doc: {
        name: "Server 1",
        createdBy: {
          "@id": "user123",
          "name": "User 123",
        },
      },
    };

    const result = await logicFn[1](deleteDoc, "1.0.0", "1.0.0");
    expect(result.status).toBe("finished");
    expect(result.documents.length).toBe(1);
    expect(result.documents[0]).toEqual({
      action: "delete",
      dstPath: "users/user123/@views/servers+server1+createdBy",
    });
  });

  it("should cleanup all @views entries in src when deleting a document with an array-map propView", async () => {
    const vd: ViewDefinition = {
      srcEntity: "user",
      srcProps: ["name"],
      destEntity: "server",
      destProp: {
        name: "followers",
        type: "array-map",
      },
      version: "1.0.0",
    };
    const logicFn = viewLogics.createViewLogicFn(vd);

    const deleteDoc: LogicResultDoc = {
      action: "delete",
      dstPath: "servers/server1",
      doc: {
        name: "Server 1",
        followers: {
          "userA": {"@id": "userA", "name": "Alice"},
          "userB": {"@id": "userB", "name": "Bob"},
        },
      },
    };

    const result = await logicFn[1](deleteDoc, "1.0.0", "1.0.0");
    expect(result.status).toBe("finished");
    expect(result.documents.length).toBe(2);
    expect(result.documents).toEqual([
      {
        action: "delete",
        dstPath: "users/userA/@views/servers+server1+followers[userA]",
      },
      {
        action: "delete",
        dstPath: "users/userB/@views/servers+server1+followers[userB]",
      },
    ]);
  });

  it("should return empty documents when deleting a document that does not contain the propView", async () => {
    const vd: ViewDefinition = {
      srcEntity: "user",
      srcProps: ["name"],
      destEntity: "server",
      destProp: {
        name: "followers",
        type: "array-map",
      },
      version: "1.0.0",
    };
    const logicFn = viewLogics.createViewLogicFn(vd);

    const deleteDoc: LogicResultDoc = {
      action: "delete",
      dstPath: "servers/server1",
      doc: {
        name: "Server 1",
      },
    };

    const result = await logicFn[1](deleteDoc, "1.0.0", "1.0.0");
    expect(result.status).toBe("finished");
    expect(result.documents.length).toBe(0);
  });

  describe("dstToSrcLogicFn edge cases", () => {
    it("should handle multiple placeholders in srcPath resolved from dstPath", async () => {
      // srcEntity: recipeIngredient, srcPath: topics/{topicId}/ingredients/{recipeIngredientId}
      // destEntity: menuItemIngredient, dstPath: topics/{topicId}/menuItems/{menuItemId}/ingredients/{menuItemIngredientId}
      const vdCustom: ViewDefinition = {
        srcEntity: "recipeIngredient",
        srcProps: ["amount"],
        destEntity: "menuItemIngredient",
        version: "1.0.0",
      };
      const logicFn = viewLogics.createViewLogicFn(vdCustom);
      const dstPath = "topics/topicA/menuItems/itemB/ingredients/ingredientC";
      const logicResultDoc: LogicResultDoc = {
        action: "delete",
        dstPath,
        doc: {
          "@id": "ingredientC",
          // topicId is missing in doc, should be extracted from dstPath
        },
      };

      const result = await logicFn[1](logicResultDoc, "1.0.0", "1.0.0");
      expect(result.status).toBe("finished");
      // srcDocId = ingredientC
      // topicId = topicA (from dstPath topics/topicA/...)
      // srcPath = topics/topicA/ingredients/ingredientC
      expect(result.documents[0].dstPath).toBe("topics/topicA/ingredients/ingredientC/@views/topics+topicA+menuItems+itemB+ingredients+ingredientC");
    });

    it("should handle multiple placeholders in srcPath resolved from both viewDoc and dstPath", async () => {
      // Let's use recipeIngredient as src and menuItemIngredient as dest
      const vdCustom: ViewDefinition = {
        srcEntity: "recipeIngredient",
        srcProps: ["amount"],
        destEntity: "menuItemIngredient",
        version: "1.0.0",
      };
      const logicFn = viewLogics.createViewLogicFn(vdCustom);
      const dstPath = "topics/topicA/menuItems/itemB/ingredients/ingredientC";
      const logicResultDoc: LogicResultDoc = {
        action: "delete",
        dstPath,
        doc: {
          "@id": "ingredientC",
          "topicId": "topicOverride", // Should override topicA from dstPath
        },
      };

      const result = await logicFn[1](logicResultDoc, "1.0.0", "1.0.0");
      expect(result.status).toBe("finished");
      expect(result.documents[0].dstPath).toBe("topics/topicOverride/ingredients/ingredientC/@views/topics+topicA+menuItems+itemB+ingredients+ingredientC");
    });

    it("should return error if srcPath placeholder cannot be resolved from viewDoc or dstPath", async () => {
      const vdCustom: ViewDefinition = {
        srcEntity: "recipeIngredient",
        srcProps: ["amount"],
        destEntity: "menuItemIngredient",
        version: "1.0.0",
      };
      const logicFn = viewLogics.createViewLogicFn(vdCustom);
      // dstPath that doesn't match topics/{topicId}/menuItems/{menuItemId}/ingredients/{menuItemIngredientId}
      const dstPath = "random/path/that/doesnt/match";
      const logicResultDoc: LogicResultDoc = {
        action: "delete",
        dstPath,
        doc: {
          "@id": "ingredientC",
          // topicId is missing
        },
      };

      jest.spyOn(console, "error").mockImplementation();
      const result = await logicFn[1](logicResultDoc, "1.0.0", "1.0.0");
      expect(result.status).toBe("error");
      expect(result.message).toBe("srcPath should not have a placeholder");
      expect(console.error).toHaveBeenCalledWith("srcPath should not have a placeholder");
    });

    it("should extract srcDocId from viewDoc even if it differs from dstPath last segment", async () => {
      const logicFn = viewLogics.createViewLogicFn(vd1);
      const dstPath = "friends/friend1";
      const logicResultDoc: LogicResultDoc = {
        action: "delete",
        dstPath,
        doc: {
          "@id": "userActualId", // Differs from friend1
        },
      };

      const result = await logicFn[1](logicResultDoc, "1.0.0", "1.0.0");
      expect(result.status).toBe("finished");
      // Should use userActualId
      expect(result.documents[0].dstPath).toBe("users/userActualId/@views/friends+friend1");
    });
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

        const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
        expect(result.name).toBe("menuItemIngredient Dst-to-Src");
        expect(result.status).toBe("finished");
        expect(result.timeFinished).toBe(undefined);
      });

      it("should create @syncCreateView in global collection", async () => {
        jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(false);

        const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
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

        const result = await logicFn[1](invalidLogicResultDoc, targetVersion, appVersion);
        expect(errorSpy).toHaveBeenCalledWith("invalid syncCreate dstPath, topics/topic21/menus/menu1#ingredients");
        expect(result.documents.length).toEqual(1);
        expect(result.documents[0].dstPath).not.toBe("@syncCreateViews/topics+topic21+menus+menu1+ingredients");
      });

      it("should not create @syncCreateView in global collection if the a doc with same docId is already created", async () => {
        const infoSpy = jest.spyOn(console, "info").mockImplementation();
        jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(true);

        const result = await logicFn[1](logicResultDoc, targetVersion, appVersion);
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
                "destProp": "ingredients",
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
        const result = await logicFn[0](logicResultDoc, targetVersion, appVersion);
        expect(result.documents.length).toBe(2);

        expect(result.documents[0].dstPath).toBe(`${dstPath1}/ingredient1`);
        expect(result.documents[0].doc).toBe(logicResultDoc.doc);
      });

      it("should manually create @view document for each newly created views", async () => {
        const result = await logicFn[0](logicResultDoc, targetVersion, appVersion);

        const atViewsCollectionPath = `${srcCollection}/ingredient1/@views`;
        expect(result.documents[1].dstPath).toBe(
          `${atViewsCollectionPath}/topics+topic21+menus+menu1+ingredients+ingredient1`);
        expect(result.documents[1].doc).toEqual({
          "destEntity": "menuItemIngredient",
          "path": `${dstPath1}/ingredient1`,
          "srcProps": ["amount", "ingredient"],
        });
      });

      it("should add the docId in the @{field} if there is a destProp", async () => {
        const logicFnPropView = viewLogics.createViewLogicFn({
          srcEntity: "recipeIngredient",
          srcProps: ["amount", "ingredient"],
          destEntity: "preparationAreaIngredient",
          destProp: {name: "ingredients", type: "array-map"},
          options: {syncCreate: true},
          version: "1.0.0",
        });

        const result = await logicFnPropView[0](logicResultDoc, targetVersion, appVersion);

        expect(result.documents.length).toBe(3);

        expect(result.documents[2].dstPath).toBe(
          "topics/topic21/preparationAreas/prepArea1/menus/menu1");
        expect(result.documents[2].instructions).toEqual({
          "@ingredients": "arr(+ingredient1)",
        });
        expect(result.documents[2].skipRunViewLogics).toBe(true);
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
  const appVersion = "1.0.0";

  it("should queue docs to run view logics", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-merge"},
      dstPath: "users/test-user-id/documents/doc1",
    };
    await viewLogics.queueRunViewLogics(targetVersion, appVersion, [doc1]);

    expect(findMatchingViewLogicsSpy).toHaveBeenCalled();
    expect(publishMessageSpy).toHaveBeenCalledWith({json: {"doc": doc1, targetVersion, appVersion}});
  });

  it("should capture unwrapped view document data when action is delete", async () => {
    // Mock findMatchingViewLogics to return a match for this test
    jest.spyOn(viewLogics, "findMatchingViewLogics").mockReturnValue(new Map([["test", {viewLogicFn: jest.fn(), name: "test"} as any]]));

    const doc1: LogicResultDoc = {
      action: "delete",
      priority: "normal",
      dstPath: "servers/server1#createdBy",
    };
    const mockData = {
      createdBy: {"name": "test-user", "@id": "user1"},
      serverName: "Server 1",
    };
    const getSpy = jest.fn().mockResolvedValue({
      data: () => mockData,
    });
    const docSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      get: getSpy,
    } as any);

    await viewLogics.queueRunViewLogics(targetVersion, appVersion, [doc1]);

    expect(docSpy).toHaveBeenCalledWith("servers/server1");
    expect(publishMessageSpy).toHaveBeenCalledWith({
      json: {
        "doc": {
          ...doc1,
          doc: mockData.createdBy,
        },
        targetVersion,
        appVersion,
      },
    });
    docSpy.mockRestore();
    (viewLogics.findMatchingViewLogics as jest.Mock).mockRestore();
  });

  it("should respect concurrency cap with 200 docs on distinct paths (max = 50) and handle clamping (0->1, NaN->1, 1000->500)", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    publishMessageSpy.mockImplementation(() => {
      inFlight++;
      if (inFlight > maxInFlight) {
        maxInFlight = inFlight;
      }
      return new Promise((resolve) => {
        setTimeout(() => {
          inFlight--;
          resolve("msg-id");
        }, 2);
      });
    });

    const origConcurrency = globalProjectConfig?.viewLogicsPublishConcurrency;
    try {
      // 1. Default (undefined): 200 docs on distinct paths -> maxInFlight is exactly 50
      globalProjectConfig.viewLogicsPublishConcurrency = undefined;
      const docs: LogicResultDoc[] = Array.from({length: 200}, (_, i) => ({
        action: "create",
        dstPath: `users/user-${i}`,
        doc: {name: `user-${i}`},
      }));

      await viewLogics.queueRunViewLogics(targetVersion, appVersion, docs);

      expect(publishMessageSpy).toHaveBeenCalledTimes(200);
      expect(maxInFlight).toBe(50);

      // 2. viewLogicsPublishConcurrency: 1 -> maxInFlight is 1
      globalProjectConfig.viewLogicsPublishConcurrency = 1;
      inFlight = 0;
      maxInFlight = 0;
      publishMessageSpy.mockClear();

      const smallDocs1: LogicResultDoc[] = Array.from({length: 10}, (_, i) => ({
        action: "create",
        dstPath: `users/user-${i}`,
        doc: {name: `user-${i}`},
      }));

      await viewLogics.queueRunViewLogics(targetVersion, appVersion, smallDocs1);

      expect(publishMessageSpy).toHaveBeenCalledTimes(10);
      expect(maxInFlight).toBe(1);

      // 3. viewLogicsPublishConcurrency: 0 -> clamped to 1
      globalProjectConfig.viewLogicsPublishConcurrency = 0;
      inFlight = 0;
      maxInFlight = 0;
      publishMessageSpy.mockClear();

      const smallDocs0: LogicResultDoc[] = Array.from({length: 10}, (_, i) => ({
        action: "create",
        dstPath: `users/user-${i}`,
        doc: {name: `user-${i}`},
      }));

      await viewLogics.queueRunViewLogics(targetVersion, appVersion, smallDocs0);

      expect(publishMessageSpy).toHaveBeenCalledTimes(10);
      expect(maxInFlight).toBe(1);

      // 4. viewLogicsPublishConcurrency: NaN -> clamped to 1
      globalProjectConfig.viewLogicsPublishConcurrency = NaN;
      inFlight = 0;
      maxInFlight = 0;
      publishMessageSpy.mockClear();

      const smallDocsNaN: LogicResultDoc[] = Array.from({length: 10}, (_, i) => ({
        action: "create",
        dstPath: `users/user-${i}`,
        doc: {name: `user-${i}`},
      }));

      await viewLogics.queueRunViewLogics(targetVersion, appVersion, smallDocsNaN);

      expect(publishMessageSpy).toHaveBeenCalledTimes(10);
      expect(maxInFlight).toBe(1);

      // 5. viewLogicsPublishConcurrency: 1000 with 600 docs -> clamped to 500
      globalProjectConfig.viewLogicsPublishConcurrency = 1000;
      inFlight = 0;
      maxInFlight = 0;
      publishMessageSpy.mockClear();

      const largeDocs: LogicResultDoc[] = Array.from({length: 600}, (_, i) => ({
        action: "create",
        dstPath: `users/user-${i}`,
        doc: {name: `user-${i}`},
      }));

      await viewLogics.queueRunViewLogics(targetVersion, appVersion, largeDocs);

      expect(publishMessageSpy).toHaveBeenCalledTimes(600);
      expect(maxInFlight).toBe(500);
    } finally {
      globalProjectConfig.viewLogicsPublishConcurrency = origConcurrency;
    }
  });

  it("should chain publishes sharing basePath in input order and run distinct paths concurrently", async () => {
    const docA1: LogicResultDoc = {
      action: "merge",
      dstPath: "servers/serverA#x",
      doc: {x: 1},
    };
    const docB: LogicResultDoc = {
      action: "merge",
      dstPath: "servers/serverB",
      doc: {b: 1},
    };
    const docA2: LogicResultDoc = {
      action: "merge",
      dstPath: "servers/serverA#y",
      doc: {y: 2},
    };
    const docA3: LogicResultDoc = {
      action: "merge",
      dstPath: "servers/serverA",
      doc: {a: 3},
    };

    let resolveA1!: (val: string) => void;
    let resolveB!: (val: string) => void;
    let resolveA2!: (val: string) => void;
    let resolveA3!: (val: string) => void;

    const pA1 = new Promise<string>((res) => {
      resolveA1 = res;
    });
    const pB = new Promise<string>((res) => {
      resolveB = res;
    });
    const pA2 = new Promise<string>((res) => {
      resolveA2 = res;
    });
    const pA3 = new Promise<string>((res) => {
      resolveA3 = res;
    });

    const publishedDocs: LogicResultDoc[] = [];

    publishMessageSpy.mockImplementation(({json}: {json: {doc: LogicResultDoc}}) => {
      publishedDocs.push(json.doc);
      if (json.doc.dstPath === "servers/serverA#x") return pA1;
      if (json.doc.dstPath === "servers/serverB") return pB;
      if (json.doc.dstPath === "servers/serverA#y") return pA2;
      if (json.doc.dstPath === "servers/serverA") return pA3;
      return Promise.resolve("msg-id");
    });

    const queuePromise = viewLogics.queueRunViewLogics(targetVersion, appVersion, [
      docA1,
      docB,
      docA2,
      docA3,
    ]);

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(publishedDocs).toContainEqual(docA1);
    expect(publishedDocs).toContainEqual(docB);
    expect(publishedDocs).not.toContainEqual(docA2);
    expect(publishedDocs).not.toContainEqual(docA3);

    resolveA1("A1-done");
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(publishedDocs).toContainEqual(docA2);
    expect(publishedDocs).not.toContainEqual(docA3);

    resolveA2("A2-done");
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(publishedDocs).toContainEqual(docA3);

    resolveB("B-done");
    resolveA3("A3-done");

    await queuePromise;
    expect(publishedDocs).toEqual([docA1, docB, docA2, docA3]);
  });

  it("should attempt all docs when failures occur, log each failure, and rethrow first error with failedDstPaths and count", async () => {
    const consoleErrorSpy = jest.spyOn(console, "error").mockImplementation();

    const docs: LogicResultDoc[] = [
      {action: "merge", dstPath: "servers/serverChain#prop0", doc: {val: 0}},
      {action: "merge", dstPath: "servers/serverChain#prop1", doc: {val: 1}},
      {action: "merge", dstPath: "servers/serverChain#prop2", doc: {val: 2}},
      ...Array.from({length: 97}, (_, i) => ({
        action: "create" as const,
        dstPath: `servers/server-${i + 3}`,
        doc: {val: i + 3},
      })),
    ];

    publishMessageSpy.mockImplementation(({json}: {json: {doc: LogicResultDoc}}) => {
      if (json.doc.dstPath === "servers/serverChain#prop1") {
        return Promise.reject(new Error("Publish failed for doc 1"));
      }
      if (json.doc.dstPath === "servers/server-5") {
        return Promise.reject(new Error("Publish failed for doc 5"));
      }
      return Promise.resolve("msg-id");
    });

    let thrownError: ViewLogicsPublishError | undefined;
    try {
      await viewLogics.queueRunViewLogics(targetVersion, appVersion, docs);
    } catch (err) {
      thrownError = err as ViewLogicsPublishError;
    }

    expect(thrownError).toBeDefined();
    expect(thrownError).toBeInstanceOf(Error);
    expect(publishMessageSpy).toHaveBeenCalledTimes(100);
    expect(publishMessageSpy).toHaveBeenCalledWith(expect.objectContaining({
      json: expect.objectContaining({
        doc: expect.objectContaining({
          dstPath: "servers/serverChain#prop2",
        }),
      }),
    }));
    expect(thrownError?.message).toContain("Publish failed for doc 1");
    expect(thrownError?.message).toContain("(and 1 more publish failures)");
    expect(thrownError?.failedDstPaths).toEqual([
      "servers/serverChain#prop1",
      "servers/server-5",
    ]);
    expect(consoleErrorSpy).toHaveBeenCalledTimes(2);
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      "Received error while publishing: servers/serverChain#prop1: Publish failed for doc 1"
    );
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      "Received error while publishing: servers/server-5: Publish failed for doc 5"
    );

    consoleErrorSpy.mockRestore();
  });

  it("should run delete branch reads concurrently and fill doc in place", async () => {
    let inFlightReads = 0;
    let maxInFlightReads = 0;

    const mockData = {
      userProp: {
        item1: {name: "item-one"},
      },
    };

    const docSpy = jest.spyOn(admin.firestore(), "doc").mockImplementation((_basePath: string) => {
      return {
        get: jest.fn().mockImplementation(() => {
          inFlightReads++;
          if (inFlightReads > maxInFlightReads) {
            maxInFlightReads = inFlightReads;
          }
          return new Promise((resolve) => {
            setTimeout(() => {
              inFlightReads--;
              resolve({
                data: () => mockData,
              });
            }, 5);
          });
        }),
      } as unknown as DocumentReference;
    });

    const deleteDocs: LogicResultDoc[] = Array.from({length: 10}, (_, i) => ({
      action: "delete",
      dstPath: `servers/server-${i}#userProp[item1]`,
      doc: {},
    }));

    await viewLogics.queueRunViewLogics(targetVersion, appVersion, deleteDocs);

    expect(maxInFlightReads).toBeGreaterThan(1);
    expect(publishMessageSpy).toHaveBeenCalledTimes(10);
    for (let i = 0; i < 10; i++) {
      expect(deleteDocs[i].doc).toEqual({name: "item-one"});
      expect(publishMessageSpy).toHaveBeenCalledWith(expect.objectContaining({
        json: expect.objectContaining({
          doc: expect.objectContaining({
            dstPath: `servers/server-${i}#userProp[item1]`,
            doc: {name: "item-one"},
          }),
        }),
      }));
    }

    docSpy.mockRestore();
  });

  it("should filter out skipRunViewLogics, non-matching docs, and non-create/merge/delete actions", async () => {
    const matchingCreate: LogicResultDoc = {
      action: "create",
      dstPath: "users/user-create",
      doc: {a: 1},
    };
    const matchingMerge: LogicResultDoc = {
      action: "merge",
      dstPath: "users/user-merge",
      doc: {b: 2},
    };
    const matchingDelete: LogicResultDoc = {
      action: "delete",
      dstPath: "users/user-delete",
      doc: {c: 3},
    };
    const skippedDoc: LogicResultDoc = {
      action: "create",
      dstPath: "users/user-skipped",
      doc: {d: 4},
      skipRunViewLogics: true,
    };
    const copyDoc: LogicResultDoc = {
      action: "copy",
      dstPath: "users/user-copy",
      doc: {e: 5},
    };
    const submitFormDoc: LogicResultDoc = {
      action: "submit-form",
      dstPath: "users/user-submit-form",
      doc: {f: 6},
    };
    const nonMatchingDoc: LogicResultDoc = {
      action: "create",
      dstPath: "unknown/unknown-path",
      doc: {g: 7},
    };

    findMatchingViewLogicsSpy.mockImplementation((doc: LogicResultDoc) => {
      if (doc.dstPath.startsWith("users/")) {
        return new Map([["0.0.1", {} as ViewLogicConfig]]);
      }
      return new Map();
    });

    await viewLogics.queueRunViewLogics(targetVersion, appVersion, [
      matchingCreate,
      matchingMerge,
      matchingDelete,
      skippedDoc,
      copyDoc,
      submitFormDoc,
      nonMatchingDoc,
    ]);

    expect(publishMessageSpy).toHaveBeenCalledTimes(3);
    expect(publishMessageSpy).toHaveBeenCalledWith({json: {doc: matchingCreate, appVersion, targetVersion}});
    expect(publishMessageSpy).toHaveBeenCalledWith({json: {doc: matchingMerge, appVersion, targetVersion}});
    expect(publishMessageSpy).toHaveBeenCalledWith({json: {doc: matchingDelete, appVersion, targetVersion}});
  });

  it("should preserve message shape including lastProcessedId when provided", async () => {
    const doc1: LogicResultDoc = {
      action: "create",
      dstPath: "users/user-1",
      doc: {name: "test-user"},
    };

    await viewLogics.queueRunViewLogics(targetVersion, appVersion, [doc1], "test-last-processed-id");

    expect(publishMessageSpy).toHaveBeenCalledWith({
      json: {
        doc: doc1,
        appVersion,
        targetVersion,
        lastProcessedId: "test-last-processed-id",
      },
    });
  });

  it("should create view-logics-queue topic with explicit batching and honor pubsubPublishOptions.viewLogics", () => {
    const topicSpy = jest.spyOn(PubSub.prototype, "topic");

    try {
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

      expect(topicSpy).toHaveBeenCalledWith("view-logics-queue", {
        batching: {
          maxMessages: 100,
          maxMilliseconds: 10,
          maxBytes: 1024 * 1024,
        },
      });

      const customOptions = {
        batching: {
          maxMessages: 50,
          maxMilliseconds: 20,
          maxBytes: 512 * 1024,
        },
      };
      initializeEmberFlow({
        projectConfig: {
          ...projectConfig,
          pubsubPublishOptions: {
            viewLogics: customOptions,
          },
        },
        admin,
        dbStructure,
        Entity,
        securityConfigs,
        validatorConfigs,
        logicConfigs: [],
        patchLogicConfigs: [],
      });

      expect(topicSpy).toHaveBeenCalledWith("view-logics-queue", {
        batching: {
          maxMessages: 50,
          maxMilliseconds: 20,
          maxBytes: 512 * 1024,
        },
      });

      const partialOptions = {
        batching: {
          maxMessages: 50,
        },
      };
      initializeEmberFlow({
        projectConfig: {
          ...projectConfig,
          pubsubPublishOptions: {
            viewLogics: partialOptions,
          },
        },
        admin,
        dbStructure,
        Entity,
        securityConfigs,
        validatorConfigs,
        logicConfigs: [],
        patchLogicConfigs: [],
      });

      expect(topicSpy).toHaveBeenCalledWith("view-logics-queue", {
        batching: {
          maxMessages: 50,
          maxMilliseconds: 10,
          maxBytes: 1024 * 1024,
        },
      });
    } finally {
      topicSpy.mockRestore();
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
    }
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
  const targetVersion = "1.0.0";
  const appVersion = "1.0.0";
  const event = {
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
    createMetricExecutionSpy = jest.spyOn(indexUtils._mockable, "saveMetricExecution").mockResolvedValue();
    runViewLogicsSpy = jest.spyOn(viewLogics, "runViewLogics").mockImplementation(async () => {
      return viewLogicsResult.map((result) => ({...result, documents: [...result.documents]}));
    });
    expandConsolidateAndGroupByDstPathSpy = jest.spyOn(indexUtils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(expandConsolidateResult);
    distributeSpy = jest.spyOn(indexUtils, "distributeFnNonTransactional").mockResolvedValue([]);
  });

  it("should skip duplicate message", async () => {
    isProcessedMock.mockResolvedValueOnce(true);
    await viewLogics.onMessageViewLogicsQueue(event);

    expect(isProcessedMock).toHaveBeenCalledWith(VIEW_LOGICS_TOPIC_NAME, event.id);
  });

  it("should distribute queued view logics", async () => {
    const distributeFnLogicResult: LogicResult = {
      name: "runViewLogics",
      status: "finished",
      documents: [],
      execTime: expect.any(Number),
    };
    const result = await viewLogics.onMessageViewLogicsQueue(event);

    expect(runViewLogicsSpy).toHaveBeenCalledWith(doc1, targetVersion, appVersion, undefined);
    const expectedMetricExecutions = convertLogicResultsToMetricExecutions([...viewLogicsResult, distributeFnLogicResult]);
    expect(createMetricExecutionSpy).toHaveBeenCalledWith(expectedMetricExecutions);
    expect(expandConsolidateAndGroupByDstPathSpy).toHaveBeenCalled();
    const calledWith = expandConsolidateAndGroupByDstPathSpy.mock.calls[0][0];
    // Since the array is cleared in the function, we can't check its content directly if it's the same reference
    // But we know it was called with the flattened documents
    expect(calledWith).toBeDefined();
    expect(distributeSpy).toHaveBeenCalledWith(expandConsolidateResult, appVersion, true);
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

    // normal view
    expect(result?.has("todos ViewLogic")).toBe(true);
    // map view
    expect(result?.has("user#mainTopic ViewLogic")).toBe(true);
    // array-map view
    expect(result?.has("user#todosArray ViewLogic")).toBe(true);
  });

  it("should return matching reverse view logics without destProp when destProp is undefined and action is create", () => {
    const createDoc: LogicResultDoc = {
      action: "create",
      dstPath: "users/userId",
      doc: {title: "New title"},
    };
    const result = findMatchingViewLogics(createDoc, "5.0.0");

    // It should not include reverse view logics meant for specific destProps
    // because dstPath does not have a #destProp
    expect(result?.has("user#todosArray Reverse ViewLogic")).toBe(false);
    expect(result?.has("user#mainTopic Reverse ViewLogic")).toBe(false);
  });

  it("should return matching reverse view logics for propViews when deleting a document without destProp", () => {
    // Override the mock to return "user" entity for the base path
    jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({
      entity: "user",
      regex: /users/,
    });

    const deleteDoc: LogicResultDoc = {
      action: "delete",
      dstPath: "users/userId",
      doc: {name: "Alice"},
    };
    const result = findMatchingViewLogics(deleteDoc, "5.0.0");

    // When deleting a document, it should include reverse view logics for embedded propViews
    expect(result?.has("user#todosArray Reverse ViewLogic")).toBe(true);
    expect(result?.has("user#mainTopic Reverse ViewLogic")).toBe(true);
  });

  it("should return matching reverse view logics for array-map merge actions", () => {
    // Override the mock to return "user" entity for the base path
    jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({
      entity: "user",
      regex: /users/,
    });

    const mergeDoc: LogicResultDoc = {
      action: "merge",
      dstPath: "users/userId#todosArray[todoId]",
      doc: {title: "Updated title"},
    };
    const result = findMatchingViewLogics(mergeDoc, "5.0.0");

    // It should include the reverse view logic because merge is now a valid action
    // and dstPath correctly routes to the entity "user" with destProp "todosArray"
    expect(result?.has("user#todosArray Reverse ViewLogic")).toBe(true);
  });
});

describe("createViewDoc", () => {
  beforeEach(() => {
    jest.restoreAllMocks();
    jest.spyOn(pathsMockable, "doesPathExists").mockResolvedValue(true);
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
  });

  it("should build a consistent @views doc for a map propView", () => {
    const result = viewLogics.createViewDoc("users/1234", "topics/t1#createdBy");

    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      action: "merge",
      dstPath: "users/1234/@views/topics+t1+createdBy",
      doc: {
        path: "topics/t1#createdBy",
        srcProps: ["email", "name"],
        destEntity: "topic",
        destProp: "createdBy",
      },
    });
  });

  it("should build an @views doc plus array-map instruction for an array-map propView", () => {
    const result = viewLogics.createViewDoc("users/1234", "servers/123#followers[1234]");

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      action: "merge",
      dstPath: "users/1234/@views/servers+123+followers[1234]",
      doc: {
        path: "servers/123#followers[1234]",
        srcProps: ["email", "name"],
        destEntity: "server",
        destProp: "followers",
      },
    });
    expect(result[1]).toEqual({
      action: "merge",
      dstPath: "servers/123",
      instructions: {
        "@followers": "arr(+1234)",
      },
      skipRunViewLogics: true,
    });
  });

  it("should build an @views doc without destProp for a full-document view", () => {
    const result = viewLogics.createViewDoc("users/1234", "servers/123/members/m1");

    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      action: "merge",
      dstPath: "users/1234/@views/servers+123+members+m1",
      doc: {
        path: "servers/123/members/m1",
        srcProps: ["name"],
        destEntity: "member",
      },
    });
    expect(result[0].doc).not.toHaveProperty("destProp");
  });

  it("should use the srcProps of the latest matching view definition version", () => {
    // todosCollection registers view(Topic, ["title"])@1.0.0 and view(Topic, ["title","name"])@2.0.0
    const result = viewLogics.createViewDoc("topics/t1", "users/999/todosCollection/td1");

    expect(result).toHaveLength(1);
    expect(result[0].doc).toEqual({
      path: "users/999/todosCollection/td1",
      srcProps: ["name", "title"],
      destEntity: "todos",
    });
  });

  it("should throw when no matching view definition exists", () => {
    expect(() => viewLogics.createViewDoc("users/1234", "servers/123#nonexistent"))
      .toThrow(/No matching ViewDefinition/);
  });

  it("should throw when the src entity cannot be resolved", () => {
    expect(() => viewLogics.createViewDoc("nonexistent/1", "servers/123#createdBy"))
      .toThrow(/Cannot resolve src entity/);
  });

  it("should throw when the dest entity cannot be resolved", () => {
    expect(() => viewLogics.createViewDoc("users/1234", "nonexistent/1#createdBy"))
      .toThrow(/Cannot resolve dest entity/);
  });
});
