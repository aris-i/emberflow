import * as admin from "firebase-admin";
import {
  distribute,
  validateForm,
  getFormModifiedFields,
  delayFormSubmissionAndCheckIfCancelled,
  runBusinessLogics,
  groupDocsByUserAndDstPath,
  expandConsolidateAndGroupByDstPath,
  runViewLogics,
  _mockable,
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
import * as batch from "../utils/batch";
import Timestamp = firestore.Timestamp;

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

describe("distribute", () => {
  let dbSpy: jest.SpyInstance;
  let colSpy: jest.SpyInstance;

  beforeEach(() => {
    const dbDoc = ({
      get: jest.fn().mockResolvedValue({exists: true, data: () => ({})}),
      set: jest.fn().mockResolvedValue({}),
      delete: jest.fn().mockResolvedValue({}),
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);
    colSpy = jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      doc: jest.fn(() => dbDoc),
    } as any);
  });

  afterEach(() => {
    dbSpy.mockRestore();
    colSpy.mockRestore();
  });

  it("should merge a document to dstPath with instructions", async () => {
    const batchSetSpy = jest.spyOn(batch, "set").mockResolvedValue(undefined);

    const userDocsByDstPath = new Map([[
      "/users/test-user-id/documents/test-doc-id",
      {
        action: "merge",
        doc: {name: "test-doc-name-updated"},
        instructions: {
          "count": "++",
          "score": "+5",
          "minusCount": "--",
          "minusScore": "-3",
        },
        dstPath: "/users/test-user-id/documents/test-doc-id",
      } as LogicResultDoc,
    ]]);
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, []);
    await distribute(userDocsByDstPath);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(batchSetSpy.mock.calls[0][1]).toEqual({
      "name": "test-doc-name-updated",
      "count": admin.firestore.FieldValue.increment(1),
      "score": admin.firestore.FieldValue.increment(5),
      "minusCount": admin.firestore.FieldValue.increment(-1),
      "minusScore": admin.firestore.FieldValue.increment(-3),
    });
    expect(batchSetSpy).toHaveBeenCalledTimes(1);
    batchSetSpy.mockRestore();
  });

  it("should delete a document at dstPath", async () => {
    const batchDeleteSpy = jest.spyOn(batch, "deleteDoc").mockResolvedValue(undefined);

    const userDocsByDstPath = new Map([[
      "/users/test-user-id/documents/test-doc-id",
        {
          action: "delete",
          dstPath: "/users/test-user-id/documents/test-doc-id",
        } as LogicResultDoc,
    ]]);
    await distribute(userDocsByDstPath);

    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(batchDeleteSpy).toHaveBeenCalledTimes(1);

    batchDeleteSpy.mockRestore();
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

  it("should call all matching logics and return their results", async () => {
    const logicFn1 = jest.fn().mockResolvedValue({status: "finished"});
    const logicFn2 = jest.fn().mockResolvedValue({status: "finished"});
    const logicFn3 = jest.fn().mockResolvedValue({status: "error", message: "Error message"});
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
    const results = await runBusinessLogics(actionType, formModifiedFields, entity, action);

    expect(logicFn1).toHaveBeenCalledWith(action);
    expect(logicFn2).toHaveBeenCalledWith(action);
    expect(logicFn3).not.toHaveBeenCalled();
    expect(results).toEqual([
      expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
      expect.objectContaining({status: "finished"}),
    ]);
  });

  it("should return an empty array if no matching logics are found", async () => {
    const logics: LogicConfig[] = [
      {
        name: "Logic 1",
        actionTypes: ["create"],
        modifiedFields: ["field1"],
        entities: ["customentity"],
        logicFn: jest.fn(),
      },
      {
        name: "Logic 2",
        actionTypes: ["update"],
        modifiedFields: ["field2"],
        entities: ["customentity"],
        logicFn: jest.fn(),
      },
      {
        name: "Logic 3",
        actionTypes: ["delete"],
        modifiedFields: ["field3"],
        entities: ["customentity"],
        logicFn: jest.fn(),
      },
    ];
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    const results = await runBusinessLogics(actionType, formModifiedFields, entity, action);

    expect(results).toEqual([]);
  });
});

describe("groupDocsByUserAndDstPath", () => {
  initializeEmberFlow(projectConfig, admin, dbStructure, Entity, {}, {}, []);
  const docsByDstPath = new Map<string, LogicResultDoc>([
    ["users/user123/document1", {action: "merge", dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}}],
    ["users/user123/document2", {action: "merge", dstPath: "users/user123/document2", doc: {field3: "value3", field6: "value6"}}],
    ["users/user456/document3", {action: "merge", dstPath: "users/user456/document3", doc: {field4: "value4"}}],
    ["users/user789/document4", {action: "delete", dstPath: "users/user789/document4"}],
    ["othercollection/document5", {action: "merge", dstPath: "othercollection/document5", doc: {field5: "value5"}}],
  ]);

  it("should group documents by destination path and user", () => {
    const userId = "user123";
    const expectedResults = {
      userDocsByDstPath: new Map<string, LogicResultDoc>([
        ["users/user123/document1", {action: "merge", dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}}],
        ["users/user123/document2", {action: "merge", dstPath: "users/user123/document2", doc: {field3: "value3", field6: "value6"}}],
      ]),
      otherUsersDocsByDstPath: new Map<string, LogicResultDoc>([
        ["users/user456/document3", {action: "merge", dstPath: "users/user456/document3", doc: {field4: "value4"}}],
        ["users/user789/document4", {action: "delete", dstPath: "users/user789/document4"}],
        ["othercollection/document5", {action: "merge", dstPath: "othercollection/document5", doc: {field5: "value5"}}],
      ]),
    };

    const results = groupDocsByUserAndDstPath(docsByDstPath, userId);

    expect(results).toEqual(expectedResults);
  });
});


describe("expandConsolidateAndGroupByDstPath", () => {
  it("should consolidate logic results documents correctly", async () => {
    // Arrange
    const logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", dstPath: "path1/doc1", doc: {field1: "value1"}, instructions: {field2: "++"}},
          {action: "delete", dstPath: "path2/doc2"},
        ],
      },
      {
        name: "logic 2",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", dstPath: "path1/doc1", doc: {field1: "value1a"}, instructions: {field2: "--"}},
          {action: "merge", dstPath: "path1/doc1", doc: {field3: "value3"}, instructions: {field4: "--"}},
          {action: "copy", srcPath: "path3/doc3", dstPath: "path4/doc4"},
          {action: "merge", dstPath: "path2/doc2", doc: {field4: "value4"}},
          {action: "merge", dstPath: "path7/doc7", doc: {field6: "value7"}},
        ],
      },
      {
        name: "logic 3",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "copy", srcPath: "path5/doc5", dstPath: "path6/doc6"},
          {action: "delete", dstPath: "path7/doc7"},
          {action: "delete", dstPath: "path7/doc7"},
          {action: "copy", srcPath: "path3/doc3", dstPath: "path4/doc4"},
        ],
      },
      {
        name: "logic 4",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "delete", dstPath: "path2/doc2"},
          {action: "copy", srcPath: "path3/doc3", dstPath: "path7/doc7"},
        ],
      },
    ];

    // Mock console.warn
    const consoleWarnSpy = jest.spyOn(console, "warn").mockImplementation(jest.fn());

    // Act
    const result = await expandConsolidateAndGroupByDstPath(logicResults);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc>([
      ["path1/doc1", {action: "merge", dstPath: "path1/doc1", doc: {field1: "value1a", field3: "value3"}, instructions: {field2: "--", field4: "--"}}],
      ["path2/doc2", {action: "delete", dstPath: "path2/doc2"}],
      ["path4/doc4", {action: "copy", srcPath: "path3/doc3", dstPath: "path4/doc4"}],
      ["path6/doc6", {action: "copy", srcPath: "path5/doc5", dstPath: "path6/doc6"}],
      ["path7/doc7", {action: "copy", srcPath: "path3/doc3", dstPath: "path7/doc7"}],
    ]);

    expect(result).toEqual(expectedResult);
    // Verify that console.warn was called
    expect(consoleWarnSpy).toHaveBeenCalledTimes(8);

    // Verify that console.warn was called with the correct message
    expect(consoleWarnSpy.mock.calls[0][0]).toBe("Overwriting key \"field1\" in doc for dstPath \"path1/doc1\"");
    expect(consoleWarnSpy.mock.calls[1][0]).toBe("Overwriting key \"field2\" in instructions for dstPath \"path1/doc1\"");
    expect(consoleWarnSpy.mock.calls[2][0]).toBe("Action \"merge\" ignored because a \"delete\" for dstPath \"path2/doc2\" already exists");
    expect(consoleWarnSpy.mock.calls[3][0]).toBe("Action \"merge\" for dstPath \"path7/doc7\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[4][0]).toBe("Action \"delete\" ignored because a \"delete\" for dstPath \"path7/doc7\" already exists");
    expect(consoleWarnSpy.mock.calls[5][0]).toBe("Action \"copy\" ignored because \"copy\" for dstPath \"path4/doc4\" already exists");
    expect(consoleWarnSpy.mock.calls[6][0]).toBe("Action \"delete\" ignored because a \"delete\" for dstPath \"path2/doc2\" already exists");
    expect(consoleWarnSpy.mock.calls[7][0]).toBe("Action \"delete\" for dstPath \"path7/doc7\" is being replaced by action \"copy\"");

    // Cleanup
    consoleWarnSpy.mockRestore();
  });
  it("should expand recursive-copy logic results documents to merge logic results", () => {
    // TODO:  Create a test case for this.  Make sure to test that recursive-copy is removed from the logic results.
  });
  it("should expand recursive-delete logic results documents to delete logic results", () => {
    // TODO:  Create a test case for this
  });
  it("should convert copy logic results documents to merge logic results", () => {
    // TODO:  Create a test case for this
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
    const dstPathLogicDocsMap: Map<string, LogicResultDoc> = new Map();
    const logicResult1 = {
      action: "merge" as LogicResultAction,
      doc: {name: "value1", sampleField2: "value2"},
      dstPath: "users/user123",
    };
    const logicResult2 = {
      action: "delete" as LogicResultAction,
      dstPath: "users/user124",
    };
    dstPathLogicDocsMap.set(logicResult1.dstPath, logicResult1);
    dstPathLogicDocsMap.set(logicResult2.dstPath, logicResult2);
    viewLogicFn1.mockResolvedValue({});
    viewLogicFn2.mockResolvedValue({});

    const results = await runViewLogics(dstPathLogicDocsMap);

    expect(viewLogicFn1).toHaveBeenCalledTimes(2);
    expect(viewLogicFn1.mock.calls[0][0]).toBe(logicResult1);
    expect(viewLogicFn1.mock.calls[1][0]).toBe(logicResult2);
    expect(viewLogicFn2).toHaveBeenCalledTimes(1);
    expect(viewLogicFn2).toHaveBeenCalledWith(logicResult2);
    expect(results).toHaveLength(3);
  });
});
