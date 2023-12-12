import * as admin from "firebase-admin";
import {
  distribute,
  distributeLater,
  validateForm,
  getFormModifiedFields,
  delayFormSubmissionAndCheckIfCancelled,
  runBusinessLogics,
  groupDocsByUserAndDstPath,
  expandConsolidateAndGroupByDstPath,
  runViewLogics,
  _mockable,
  distributeDoc,
} from "../index-utils";
import {firestore} from "firebase-admin";
import {initializeEmberFlow} from "../index";
import {
  Action,
  LogicConfig,
  LogicResult,
  LogicResultAction,
  LogicResultDoc,
  ProjectConfig,
  ViewLogicConfig,
} from "../types";
import {Entity, dbStructure} from "../sample-custom/db-structure";
import {securityConfig} from "../sample-custom/security";
import {validatorConfig} from "../sample-custom/validators";
import Timestamp = firestore.Timestamp;
import {expandAndGroupDocPathsByEntity} from "../utils/paths";
import {BatchUtil} from "../utils/batch";
import * as distribution from "../utils/distribution";
import * as forms from "../utils/forms";
import {FormData} from "emberflow-admin-client/lib/types";

jest.spyOn(console, "log").mockImplementation();
jest.spyOn(console, "info").mockImplementation();

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

jest.mock("../utils/paths", () => {
  const originalModule = jest.requireActual("../utils/paths");

  return {
    ...originalModule,
    expandAndGroupDocPathsByEntity: jest.fn(),
  };
});

describe("distributeDoc", () => {
  let dbSpy: jest.SpyInstance;
  let queueInstructionsSpy: jest.SpyInstance;
  let docSetMock: jest.Mock;
  let docDeleteMock: jest.Mock;
  const batch = BatchUtil.getInstance();
  jest.spyOn(BatchUtil, "getInstance").mockImplementation(() => batch);

  beforeEach(() => {
    docSetMock = jest.fn().mockResolvedValue({});
    docDeleteMock = jest.fn().mockResolvedValue({});
    const dbDoc = ({
      set: docSetMock,
      delete: docDeleteMock,
      id: "test-doc-id",
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);
    queueInstructionsSpy = jest.spyOn(distribution, "queueInstructions").mockResolvedValue();
  });

  afterEach(() => {
    dbSpy.mockRestore();
    queueInstructionsSpy.mockRestore();
  });

  it("should delete a document from dstPath", async () => {
    const logicResultDoc: LogicResultDoc = {
      action: "delete",
      priority: "normal",
      dstPath: "/users/test-user-id/documents/test-doc-id",
    };

    await distributeDoc(logicResultDoc);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(docDeleteMock).toHaveBeenCalledTimes(1);
    expect(docDeleteMock).toHaveBeenCalled();
  });

  it("should delete documents in batch", async () => {
    const batchDeleteSpy = jest.spyOn(batch, "deleteDoc").mockResolvedValue(undefined);
    const logicResultDoc: LogicResultDoc = {
      action: "delete",
      priority: "normal",
      dstPath: "/users/test-user-id/documents/test-doc-id",
    };

    await distributeDoc(logicResultDoc, batch);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(batchDeleteSpy).toHaveBeenCalledTimes(1);

    batchDeleteSpy.mockRestore();
  });

  it("should merge a document to dstPath", async () => {
    const logicResultDoc: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      dstPath: "/users/test-user-id/documents/test-doc-id",
      doc: {name: "test-doc-name-updated"},
    };
    const expectedData = {
      ...logicResultDoc.doc,
      "@id": "test-doc-id",
    };

    await distributeDoc(logicResultDoc);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(docSetMock).toHaveBeenCalledTimes(1);
    expect(docSetMock).toHaveBeenCalledWith(expectedData, {merge: true});
  });

  it("should merge a document to dstPath and queue instructions", async () => {
    const logicResultDoc: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      instructions: {
        "count": "++",
        "score": "+5",
        "minusCount": "--",
        "minusScore": "-3",
      },
      dstPath: "/users/test-user-id/documents/test-doc-id",
    };
    const expectedData = {
      ...logicResultDoc.doc,
      "@id": "test-doc-id",
    };

    await distributeDoc(logicResultDoc);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(queueInstructionsSpy).toHaveBeenCalledTimes(1);
    expect(queueInstructionsSpy).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id", logicResultDoc.instructions);
    expect(docSetMock).toHaveBeenCalledTimes(1);
    expect(docSetMock).toHaveBeenCalledWith(expectedData, {merge: true});
  });

  it("should merge documents in batch", async () => {
    const batchSetSpy = jest.spyOn(batch, "set").mockResolvedValue(undefined);
    const logicResultDoc: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      dstPath: "/users/test-user-id/documents/test-doc-id",
    };

    await distributeDoc(logicResultDoc, batch);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(batchSetSpy).toHaveBeenCalledTimes(1);

    batchSetSpy.mockRestore();
  });

  it("should queue a document to submit form", async () => {
    const queueSubmitFormSpy =
      jest.spyOn(forms, "queueSubmitForm").mockResolvedValue("test-message-id");
    const logicResultDoc: LogicResultDoc = {
      action: "submit-form",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      dstPath: "/users/test-user-id/documents/test-doc-id",
    };
    const formData: FormData = {
      "@docPath": logicResultDoc.dstPath,
      "@actionType": "create",
      ...logicResultDoc.doc,
    };

    await distributeDoc(logicResultDoc, batch);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(queueSubmitFormSpy).toHaveBeenCalledTimes(1);
    expect(queueSubmitFormSpy).toHaveBeenCalledWith(formData);

    queueSubmitFormSpy.mockRestore();
  });
});

describe("distribute", () => {
  let dbSpy: jest.SpyInstance;
  let colSpy: jest.SpyInstance;
  let queueInstructionsSpy: jest.SpyInstance;
  const batch = BatchUtil.getInstance();
  jest.spyOn(BatchUtil, "getInstance").mockImplementation(() => batch);

  beforeEach(() => {
    const dbDoc = ({
      get: jest.fn().mockResolvedValue({exists: true, data: () => ({})}),
      set: jest.fn().mockResolvedValue({}),
      delete: jest.fn().mockResolvedValue({}),
      id: "test-doc-id",
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);
    colSpy = jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      doc: jest.fn(() => dbDoc),
    } as any);
    queueInstructionsSpy = jest.spyOn(distribution, "queueInstructions").mockResolvedValue();
  });

  afterEach(() => {
    dbSpy.mockRestore();
    colSpy.mockRestore();
    queueInstructionsSpy.mockRestore();
  });

  it("should merge a document to dstPath and queue instructions", async () => {
    const batchSetSpy = jest.spyOn(batch, "set").mockResolvedValue(undefined);

    const userDocsByDstPath = new Map([[
      "/users/test-user-id/documents/test-doc-id",
      [{
        action: "merge",
        priority: "normal",
        doc: {name: "test-doc-name-updated"},
        instructions: {
          "count": "++",
          "score": "+5",
          "minusCount": "--",
          "minusScore": "-3",
        },
        dstPath: "/users/test-user-id/documents/test-doc-id",
      } as LogicResultDoc],
    ]]);
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, []);
    await distribute(userDocsByDstPath);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(queueInstructionsSpy).toHaveBeenCalledTimes(1);
    expect(queueInstructionsSpy).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id", {
      "count": "++",
      "score": "+5",
      "minusCount": "--",
      "minusScore": "-3",
    });
    expect(batchSetSpy.mock.calls[0][1]).toEqual({
      "@id": "test-doc-id",
      "name": "test-doc-name-updated",
    });
    expect(batchSetSpy).toHaveBeenCalledTimes(1);
    batchSetSpy.mockRestore();
  });

  it("should delete a document at dstPath", async () => {
    const batchDeleteSpy = jest.spyOn(batch, "deleteDoc").mockResolvedValue(undefined);

    const userDocsByDstPath = new Map([[
      "/users/test-user-id/documents/test-doc-id",
      [{
        action: "delete",
        dstPath: "/users/test-user-id/documents/test-doc-id",
      } as LogicResultDoc],
    ]]);
    await distribute(userDocsByDstPath);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(batchDeleteSpy).toHaveBeenCalledTimes(1);

    batchDeleteSpy.mockRestore();
  });
});

describe("distributeLater", () => {
  let queueForDistributionLaterSpy: jest.SpyInstance;

  beforeEach(() => {
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, []);
    queueForDistributionLaterSpy = jest.spyOn(distribution, "queueForDistributionLater").mockResolvedValue();
  });

  afterEach(() => {
    queueForDistributionLaterSpy.mockRestore();
  });

  it("should queue docs for distribution later", async () => {
    const doc1: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      doc: {name: "test-doc-name-updated"},
      dstPath: "/users/test-user-id/documents/doc1",
    };
    const doc2: LogicResultDoc = {
      action: "merge",
      priority: "high",
      doc: {name: "test-doc-name-updated"},
      dstPath: "/users/test-user-id/documents/doc2",
    };
    const usersDocsByDstPath = new Map([
      ["/users/test-user-id/documents/doc1", [doc1]],
      ["/users/test-user-id/documents/doc2", [doc2]],
    ]);
    await distributeLater(usersDocsByDstPath);

    expect(queueForDistributionLaterSpy).toHaveBeenCalledTimes(1);
    expect(queueForDistributionLaterSpy).toHaveBeenCalledWith(doc1, doc2);
  });
});

describe("validateForm", () => {
  it("returns an object with empty validationResult when document is valid", async () => {
    const entity = "user";
    const document = {
      name: "John Doe",
      email: "johndoe@example.com",
      password: "abc123",
    };
    const [hasValidationError, validationResult] = await validateForm(entity, document);
    expect(hasValidationError).toBe(false);
    expect(validationResult).toEqual({});
  });

  it("returns an object with validation errors when document is invalid", async () => {
    const entity = "user";
    const document = {
      name: "",
      email: "johndoe@example.com",
      password: "abc",
    };
    const [hasValidationError, validationResult] = await validateForm(entity, document);
    expect(hasValidationError).toBe(true);
    expect(validationResult).toEqual({name: ["Name is required"]});
  });
});

describe("getFormModifiedFields", () => {
  it("should return an empty object when there are no form fields", () => {
    const document = {name: "John Doe", age: 30};
    const modifiedFields = getFormModifiedFields({}, document);
    expect(modifiedFields).toEqual({});
  });

  it("should return an array of modified form fields", () => {
    const document = {
      "name": "John Doe",
      "age": 30,
    };
    const modifiedFields = getFormModifiedFields({
      "name": "Jane Doe",
      "address": "123 Main St",
      "@status": "submit",
    }, document);
    expect(modifiedFields).toEqual({name: "Jane Doe", address: "123 Main St"});
  });
});

describe("delayFormSubmissionAndCheckIfCancelled", () => {
  test("should delay form submission for 500 ms and not cancel form submission", async () => {
    const delay = 500;
    const formResponseRef = {
      update: jest.fn(),
      get: jest.fn().mockResolvedValue({
        val: () => ({"@form": {"@status": "delay"}}),
      }),
    };
    const cancelFormSubmission = await delayFormSubmissionAndCheckIfCancelled(delay, formResponseRef as any);
    expect(cancelFormSubmission).toBe(false);
    expect(formResponseRef.update).toHaveBeenCalledTimes(1);
    expect(formResponseRef.update).toHaveBeenCalledWith({"@status": "delay"});
    expect(formResponseRef.get).toHaveBeenCalledTimes(1);
    expect(formResponseRef.get).toHaveBeenCalledWith();
  });

  test("should delay form submission for 2000 ms and cancel form submission", async () => {
    const delay = 2000;
    const formResponseRef = {
      update: jest.fn(),
      get: jest.fn().mockResolvedValue({
        val: () => ({"@status": "cancel"}),
      }),
    };
    const cancelFormSubmission = await delayFormSubmissionAndCheckIfCancelled(delay, formResponseRef as any);
    expect(cancelFormSubmission).toBe(true);
    expect(formResponseRef.update).toHaveBeenCalledTimes(1);
    expect(formResponseRef.update).toHaveBeenCalledWith({"@status": "delay"});
    expect(formResponseRef.get).toHaveBeenCalledTimes(1);
    expect(formResponseRef.get).toHaveBeenCalledWith();
  });
});

describe("runBusinessLogics", () => {
  const actionType = "create";
  const formModifiedFields = {field1: "value1", field2: "value2"};
  const entity = "user";
  const user = {
    id: "user123",
    name: "John Doe",
  };
  const action:Action = {
    user,
    eventContext: {
      id: "123",
      uid: "user123",
      docId: "document123",
      formId: "form123",
      docPath: "users/user123",
      entity: "user",
    },
    actionType,
    document: {
      "field1": "value1",
      "field2": "value2",
      "field3": "value3",
    },
    status: "processing",
    timeCreated: firestore.Timestamp.now(),
    modifiedFields: formModifiedFields,
  };

  let distributeFn: jest.Mock;
  let logicFn1: jest.Mock;
  let logicFn2: jest.Mock;
  let logicFn3: jest.Mock;

  let dbSpy: jest.SpyInstance;

  beforeEach(() => {
    distributeFn = jest.fn();
    logicFn1 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn2 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn3 = jest.fn().mockResolvedValue({status: "error", message: "Error message"});

    const dbDoc = ({
      get: jest.fn().mockResolvedValue({
        data: jest.fn().mockReturnValue({
          maxLogicResultPages: 10,
        }),
      }),
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);
  });

  afterEach(() => {
    // Cleanup
    logicFn1.mockRestore();
    logicFn2.mockRestore();
    logicFn3.mockRestore();
    distributeFn.mockRestore();
    dbSpy.mockRestore();
  });

  it("should call all matching logics and pass their results to distributeFn", async () => {
    const logics: LogicConfig[] = [
      {
        name: "Logic 1",
        actionTypes: ["create"],
        modifiedFields: ["field1"],
        entities: ["user"],
        logicFn: logicFn1,
      },
      {
        name: "Logic 2",
        actionTypes: "all",
        modifiedFields: ["field2"],
        entities: ["user"],
        logicFn: logicFn2,
      },
      {
        name: "Logic 3",
        actionTypes: ["delete"],
        modifiedFields: ["field3"],
        entities: ["user"],
        logicFn: logicFn3,
      },
    ];
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    await runBusinessLogics(actionType, formModifiedFields, entity, action, distributeFn);

    expect(logicFn1).toHaveBeenCalledWith(action, undefined);
    expect(logicFn2).toHaveBeenCalledWith(action, undefined);
    expect(logicFn3).not.toHaveBeenCalled();
    expect(distributeFn).toHaveBeenCalledTimes(1);
    expect(distributeFn).toHaveBeenCalledWith([
      expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
      expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
    ], 0);
  });

  it("should recall logic when it returns \"partial-result\" status", async () => {
    logicFn2.mockResolvedValueOnce({status: "partial-result", nextPage: {}});
    const logics: LogicConfig[] = [
      {
        name: "Logic 1",
        actionTypes: ["create"],
        modifiedFields: ["field1"],
        entities: ["user"],
        logicFn: logicFn1,
      },
      {
        name: "Logic 2",
        actionTypes: "all",
        modifiedFields: ["field2"],
        entities: ["user"],
        logicFn: logicFn2,
      },
    ];
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    await runBusinessLogics(actionType, formModifiedFields, entity, action, distributeFn);

    expect(logicFn1).toHaveBeenCalledWith(action, undefined);
    expect(logicFn2).toHaveBeenCalledWith(action, undefined);
    expect(distributeFn).toHaveBeenCalledTimes(2);
    expect(distributeFn.mock.calls[0]).toEqual([[
      expect.objectContaining({
        status: "partial-result",
        nextPage: {},
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
      expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
    ], 0]);

    expect(logicFn2).toHaveBeenCalledWith(action, {});
    expect(distributeFn.mock.calls[1]).toEqual([[
      expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
    ], 1]);
  });

  it("should not call any logic when no matching logics are found but distributeFn should still be " +
      "called", async () => {
    const logics: LogicConfig[] = [
      {
        name: "Logic 1",
        actionTypes: ["create"],
        modifiedFields: ["field1"],
        entities: ["customentity"],
        logicFn: logicFn1,
      },
      {
        name: "Logic 2",
        actionTypes: ["update"],
        modifiedFields: ["field2"],
        entities: ["customentity"],
        logicFn: logicFn2,
      },
      {
        name: "Logic 3",
        actionTypes: ["delete"],
        modifiedFields: ["field3"],
        entities: ["customentity"],
        logicFn: logicFn3,
      },
    ];
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    await runBusinessLogics(actionType, formModifiedFields, entity, action, distributeFn);

    expect(logicFn1).not.toHaveBeenCalled();
    expect(logicFn2).not.toHaveBeenCalled();
    expect(logicFn3).not.toHaveBeenCalled();
    expect(distributeFn).toHaveBeenCalledTimes(1);
    expect(distributeFn).toHaveBeenCalledWith([], 0);
  });

  it("should recall logic when it returns \"partial-result\" status indefinitely up to the config maxLogicResultPages",
    async () => {
      logicFn2.mockResolvedValue({status: "partial-result", nextPage: {}});
      const logics: LogicConfig[] = [
        {
          name: "Logic 1",
          actionTypes: ["create"],
          modifiedFields: ["field1"],
          entities: ["user"],
          logicFn: logicFn1,
        },
        {
          name: "Logic 2",
          actionTypes: "all",
          modifiedFields: ["field2"],
          entities: ["user"],
          logicFn: logicFn2,
        },
      ];
      initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
      await runBusinessLogics(actionType, formModifiedFields, entity, action, distributeFn);

      expect(logicFn1).toHaveBeenCalledWith(action, undefined);
      expect(logicFn2).toHaveBeenCalledWith(action, undefined);
      expect(logicFn1).toHaveBeenCalledTimes(1);
      expect(logicFn2).toHaveBeenCalledTimes(10);
      expect(distributeFn).toHaveBeenCalledTimes(10);
    });
});

describe("groupDocsByUserAndDstPath", () => {
  initializeEmberFlow(projectConfig, admin, dbStructure, Entity, {}, {}, []);
  const docsByDstPath = new Map<string, LogicResultDoc[]>([
    ["users/user123/document1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}}]],
    ["users/user123/document2", [{action: "merge", priority: "normal", dstPath: "users/user123/document2", doc: {field3: "value3", field6: "value6"}}]],
    ["users/user456/document3", [{action: "merge", priority: "normal", dstPath: "users/user456/document3", doc: {field4: "value4"}}]],
    ["users/user789/document4", [{action: "delete", priority: "normal", dstPath: "users/user789/document4"}]],
    ["othercollection/document5", [{action: "merge", priority: "normal", dstPath: "othercollection/document5", doc: {field5: "value5"}}]],
  ]);

  it("should group documents by destination path and user", () => {
    const userId = "user123";
    const expectedResults = {
      userDocsByDstPath: new Map<string, LogicResultDoc[]>([
        ["users/user123/document1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}}]],
        ["users/user123/document2", [{action: "merge", priority: "normal", dstPath: "users/user123/document2", doc: {field3: "value3", field6: "value6"}}]],
      ]),
      otherUsersDocsByDstPath: new Map<string, LogicResultDoc[]>([
        ["users/user456/document3", [{action: "merge", priority: "normal", dstPath: "users/user456/document3", doc: {field4: "value4"}}]],
        ["users/user789/document4", [{action: "delete", priority: "normal", dstPath: "users/user789/document4"}]],
        ["othercollection/document5", [{action: "merge", priority: "normal", dstPath: "othercollection/document5", doc: {field5: "value5"}}]],
      ]),
    };

    const results = groupDocsByUserAndDstPath(docsByDstPath, userId);

    expect(results).toEqual(expectedResults);
  });
});

describe("expandConsolidateAndGroupByDstPath", () => {
  let dbSpy: jest.SpyInstance;
  let consoleWarnSpy: jest.SpyInstance;

  const games = [
    {
      "@id": "game1Id",
      "name": "Game 1",
    },
    {
      "@id": "game2Id",
      "name": "Game 2",
    },
  ];
  const friend = {
    "@id": "friendId",
    "name": "Friend Name",
    "games": games.length,
  };

  beforeEach(() => {
    const dbDoc = (path: string) => {
      const pathArray = path.split("/");
      const docId = pathArray[pathArray.length - 1];
      return {
        get: jest.fn().mockResolvedValue({
          exists: true,
          data: () => (
            docId === "123" ? friend :
              docId === "1" ? games[0] :
                docId === "2" ? games[1] :
                  {}
          ),
        }),
      } as unknown as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    };
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockImplementation((path) => {
      return dbDoc(path);
    });

    // Mock console.warn
    consoleWarnSpy = jest.spyOn(console, "warn").mockImplementation(jest.fn());

    // Clear the mock before each test
    (expandAndGroupDocPathsByEntity as jest.Mock).mockClear();

    // Mock expandAndGroupDocPathsByEntity to return sample grouped paths
    (expandAndGroupDocPathsByEntity as jest.Mock).mockResolvedValueOnce({
      [Entity.User]: [
        "users/123/friends/123",
        "users/123/friends/123/games/1",
        "users/123/friends/123/games/2",
      ],
    });
  });

  afterEach(() => {
    // Cleanup
    dbSpy.mockRestore();
    consoleWarnSpy.mockRestore();
  });

  it("should consolidate logic results documents correctly", async () => {
    // Arrange
    const logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "create", priority: "normal", dstPath: "path8/doc8", doc: {field1: "value1"}, instructions: {}},
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1"}, instructions: {field2: "++"}},
          {action: "delete", priority: "normal", dstPath: "path2/doc2"},
        ],
      },
      {
        name: "logic 2",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path8/doc8", doc: {field3: "value3"}, instructions: {field4: "--"}},
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a"}, instructions: {field2: "--"}},
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field3: "value3"}, instructions: {field4: "--"}},
          {action: "copy", priority: "normal", srcPath: "path3/doc3", dstPath: "path4/doc4"},
          {action: "merge", priority: "normal", dstPath: "path2/doc2", doc: {field4: "value4"}},
          {action: "merge", priority: "normal", dstPath: "path7/doc7", doc: {field6: "value7"}},
        ],
      },
      {
        name: "logic 3",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "copy", priority: "normal", srcPath: "path5/doc5", dstPath: "path6/doc6"},
          {action: "delete", priority: "normal", dstPath: "path7/doc7"},
          {action: "delete", priority: "normal", dstPath: "path7/doc7"},
          {action: "copy", priority: "normal", srcPath: "path3/doc3", dstPath: "path4/doc4"},
        ],
      },
      {
        name: "logic 4",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "delete", priority: "normal", dstPath: "path2/doc2"},
          {action: "copy", priority: "normal", srcPath: "path3/doc3", dstPath: "path7/doc7"},
        ],
      },
    ];

    // Act
    const logicResultDocs = logicResults.map((logicResult) => logicResult.documents).flat();
    const result = await expandConsolidateAndGroupByDstPath(logicResultDocs);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc[]>([
      ["path1/doc1", [{action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a", field3: "value3"}, instructions: {field2: "--", field4: "--"}}]],
      ["path2/doc2", [{action: "delete", priority: "normal", dstPath: "path2/doc2"}]],
      ["path4/doc4", [{action: "merge", priority: "normal", dstPath: "path4/doc4", doc: {}, instructions: {}}]],
      ["path7/doc7", [{action: "delete", priority: "normal", dstPath: "path7/doc7"}]],
      ["path6/doc6", [{action: "merge", priority: "normal", dstPath: "path6/doc6", doc: {}}]],
      ["path8/doc8", [{action: "create", priority: "normal", dstPath: "path8/doc8", doc: {field1: "value1", field3: "value3"}, instructions: {field4: "--"}}]],
    ]);

    expect(result).toEqual(expectedResult);

    // Verify that console.warn was called
    expect(consoleWarnSpy).toHaveBeenCalledTimes(7);

    // Verify that console.warn was called with the correct message
    expect(consoleWarnSpy.mock.calls[0][0]).toBe("Overwriting key \"field1\" in doc for dstPath \"path1/doc1\"");
    expect(consoleWarnSpy.mock.calls[1][0]).toBe("Overwriting key \"field2\" in instructions for dstPath \"path1/doc1\"");
    expect(consoleWarnSpy.mock.calls[2][0]).toBe("Action merge ignored because a \"delete\" for dstPath \"path2/doc2\" already exists");
    expect(consoleWarnSpy.mock.calls[3][0]).toBe("Action merge for dstPath \"path7/doc7\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[4][0]).toBe("Action delete for dstPath \"path7/doc7\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[5][0]).toBe("Action delete for dstPath \"path2/doc2\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[6][0]).toBe("Action merge ignored because a \"delete\" for dstPath \"path7/doc7\" already exists");
  });

  it("should expand recursive-copy logic results documents to merge logic results", async () => {
    const logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "recursive-copy", priority: "normal", srcPath: "users/123/friends/123", dstPath: "users/456/friends/123"},
        ],
      },
      {
        name: "logic 2",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a"}, instructions: {field2: "--"}},
        ],
      },
    ];

    // Act
    const logicResultDocs = logicResults.map((logicResult) => logicResult.documents).flat();
    const result = await expandConsolidateAndGroupByDstPath(logicResultDocs);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc[]>([
      ["path1/doc1", [{
        action: "merge",
        priority: "normal",
        dstPath: "path1/doc1",
        doc: {field1: "value1a"},
        instructions: {field2: "--"},
      }]],
      ["users/456/friends/123", [{
        action: "merge",
        priority: "normal",
        doc: friend,
        dstPath: "users/456/friends/123",
      }]],
      ["users/456/friends/123/games/1", [{
        action: "merge",
        priority: "normal",
        doc: games[0],
        dstPath: "users/456/friends/123/games/1",
      }]],
      ["users/456/friends/123/games/2", [{
        action: "merge",
        priority: "normal",
        doc: games[1],
        dstPath: "users/456/friends/123/games/2",
      }]],
    ]);

    // Checks if "recursive-copy" is removed from the logic results
    expect([...result.values()].every((logicResultDocs) =>
      logicResultDocs.every((doc) => doc.action !== "recursive-copy")))
      .toBe(true);
    expect(result).toEqual(expectedResult);
  });

  it("should expand recursive-delete logic results documents to delete logic results", async () => {
    const logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "recursive-delete", priority: "normal", dstPath: "users/123/friends/123"},
        ],
      },
      {
        name: "logic 2",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a"}, instructions: {field2: "--"}},
        ],
      },
      {
        name: "logic 3",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "users/123/friends/123", instructions: {games: "--"}},
        ],
      },
    ];

    // Act
    const logicResultDocs = logicResults.map((logicResult) => logicResult.documents).flat();
    const result = await expandConsolidateAndGroupByDstPath(logicResultDocs);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc[]>([
      ["path1/doc1", [{
        action: "merge",
        priority: "normal",
        dstPath: "path1/doc1",
        doc: {field1: "value1a"},
        instructions: {field2: "--"},
      }]],
      ["users/123/friends/123", [{
        action: "delete",
        priority: "normal",
        dstPath: "users/123/friends/123",
      }]],
      ["users/123/friends/123/games/1", [{
        action: "delete",
        priority: "normal",
        dstPath: "users/123/friends/123/games/1",
      }]],
      ["users/123/friends/123/games/2", [{
        action: "delete",
        priority: "normal",
        dstPath: "users/123/friends/123/games/2",
      }]],
    ]);

    // Checks if "recursive-delete" is removed from the logic results
    expect([...result.values()].every((logicResultDocs) =>
      logicResultDocs.every((doc) => doc.action !== "recursive-delete")))
      .toBe(true);
    expect(result).toEqual(expectedResult);

    // Verify that console.warn was called
    expect(consoleWarnSpy).toHaveBeenCalledTimes(1);
    expect(consoleWarnSpy.mock.calls[0][0]).toBe("Action merge ignored because a \"delete\" for dstPath \"users/123/friends/123\" already exists");
  });

  it("should convert copy logic results documents to merge logic results", async () => {
    const logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "copy", priority: "normal", srcPath: "users/123/friends/123/games/1", dstPath: "users/456/friends/123/games/1"},
          {action: "copy", priority: "normal", srcPath: "users/123/friends/123/games/2", dstPath: "users/456/friends/123/games/2"},
        ],
      },
      {
        name: "logic 2",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a"}, instructions: {field2: "--"}},
        ],
      },
    ];

    // Act
    const logicResultDocs = logicResults.map((logicResult) => logicResult.documents).flat();
    const result = await expandConsolidateAndGroupByDstPath(logicResultDocs);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc[]>([
      ["path1/doc1", [{
        action: "merge",
        priority: "normal",
        dstPath: "path1/doc1",
        doc: {field1: "value1a"},
        instructions: {field2: "--"},
      }]],
      ["users/456/friends/123/games/1", [{
        action: "merge",
        priority: "normal",
        doc: games[0],
        dstPath: "users/456/friends/123/games/1",
      }]],
      ["users/456/friends/123/games/2", [{
        action: "merge",
        priority: "normal",
        doc: games[1],
        dstPath: "users/456/friends/123/games/2",
      }]],
    ]);

    // Checks if "copy" is removed from the logic results
    expect([...result.values()].every((logicResultDocs) =>
      logicResultDocs.every((doc) => doc.action !== "copy")))
      .toBe(true);
    expect(result).toEqual(expectedResult);
  });
});

describe("runViewLogics", () => {
  // Define mock functions
  const viewLogicFn1 = jest.fn();
  const viewLogicFn2 = jest.fn();
  const customViewLogicsConfig: ViewLogicConfig[] = [
    {
      name: "logic 1",
      entity: "user",
      modifiedFields: ["sampleField1", "sampleField2"],
      viewLogicFn: viewLogicFn1,
    },
    {
      name: "logic 2",
      entity: "user",
      modifiedFields: ["sampleField3"],
      viewLogicFn: viewLogicFn2,
    },
  ];

  beforeEach(() => {
    jest.spyOn(_mockable, "getViewLogicsConfig").mockReturnValue(customViewLogicsConfig);
  });

  it("should run view logics properly", async () => {
    const logicResult1: LogicResultDoc = {
      action: "merge" as LogicResultAction,
      priority: "normal",
      doc: {name: "value1", sampleField2: "value2"},
      dstPath: "users/user123",
    };
    const logicResult2: LogicResultDoc = {
      action: "delete" as LogicResultAction,
      priority: "normal",
      dstPath: "users/user124",
    };
    viewLogicFn1.mockResolvedValue({});
    viewLogicFn2.mockResolvedValue({});

    const results1 = await runViewLogics(logicResult1);
    const results2 = await runViewLogics(logicResult2);
    const results = [...results1, ...results2];

    expect(viewLogicFn1).toHaveBeenCalledTimes(2);
    expect(viewLogicFn1.mock.calls[0][0]).toBe(logicResult1);
    expect(viewLogicFn1.mock.calls[1][0]).toBe(logicResult2);
    expect(viewLogicFn2).toHaveBeenCalledTimes(1);
    expect(viewLogicFn2).toHaveBeenCalledWith(logicResult2);
    expect(results).toHaveLength(3);
  });
});
