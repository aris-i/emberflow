import * as admin from "firebase-admin";
import {
  distribute,
  revertModificationsOutsideForm,
  validateForm,
  getFormModifiedFields,
  delayFormSubmissionAndCheckIfCancelled,
  runBusinessLogics,
  groupDocsByUserAndDstPath,
  consolidateAndGroupByDstPath,
  runViewLogics,
  _mockable,
} from "../index-utils";
import {firestore} from "firebase-admin";
import DocumentSnapshot = firestore.DocumentSnapshot;
import {initializeEmberFlow} from "../index";
import {
  Action,
  LogicConfig,
  LogicResult, LogicResultAction,
  LogicResultDoc,
  ViewLogicConfig,
} from "../types";
import * as paths from "../utils/paths";
import {Entity, dbStructure} from "../sample-custom/db-structure";
import {securityConfig} from "../sample-custom/security";
import {validatorConfig} from "../sample-custom/validators";

admin.initializeApp();
jest.spyOn(console, "log").mockImplementation();
jest.spyOn(console, "info").mockImplementation();

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
    const setSpy = jest.fn().mockResolvedValue({});
    const batchSpy = jest.spyOn(admin.firestore(), "batch").mockReturnValue({
      set: setSpy,
      delete: jest.fn(),
      commit: jest.fn().mockResolvedValue(undefined),
    } as any);

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
    initializeEmberFlow(admin, dbStructure, Entity, securityConfig, validatorConfig, []);
    await distribute(userDocsByDstPath);

    expect(batchSpy).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(setSpy.mock.calls[0][1]).toEqual({
      name: "test-doc-name-updated",
      count: admin.firestore.FieldValue.increment(1),
      score: admin.firestore.FieldValue.increment(5),
      minusCount: admin.firestore.FieldValue.increment(-1),
      minusScore: admin.firestore.FieldValue.increment(-3),
    });
    expect(setSpy.mock.calls[0][2]).toEqual({merge: true});
    expect(setSpy).toHaveBeenCalledTimes(1);

    batchSpy.mockRestore();
  });

  it("should merge a document to dstPath with # in path", async () => {
    const setSpy = jest.fn().mockResolvedValue({});
    const batchSpy = jest.spyOn(admin.firestore(), "batch").mockReturnValue({
      set: setSpy,
      delete: jest.fn(),
      commit: jest.fn().mockResolvedValue(undefined),
    } as any);

    const randomId = "random-id";
    const docSpy = jest.spyOn(admin.firestore.CollectionReference.prototype, "doc").mockReturnValue({
      set: jest.fn(),
      delete: jest.fn(),
      collection: jest.fn(),
      get: jest.fn(),
      id: randomId,
      parent: jest.fn(),
      path: "/users/test-user-id/documents/" + randomId,
      withConverter: jest.fn(),
    } as any);
    jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      doc: docSpy,
    } as any);

    const userDocsByDstPath = new Map([[
      "/users/test-user-id/documents/#",
        {
          action: "merge",
          doc: {name: "test-doc-name-updated"},
          instructions: undefined,
          dstPath: "/users/test-user-id/documents/#",
        } as LogicResultDoc,
    ]]);
    await distribute(userDocsByDstPath);

    expect(batchSpy).toHaveBeenCalledTimes(1);
    expect(admin.firestore().collection).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).not.toHaveBeenCalled();
    expect(admin.firestore().collection).toHaveBeenCalledWith("/users/test-user-id/documents");
    expect(docSpy).toHaveBeenCalledTimes(1);
    expect(setSpy.mock.calls[0][1]).toEqual({name: "test-doc-name-updated"});
    expect(setSpy.mock.calls[0][2]).toEqual({merge: true});
    expect(setSpy).toHaveBeenCalledTimes(1);

    batchSpy.mockRestore();
    docSpy.mockRestore();
  });

  it("should delete a document at dstPath", async () => {
    const deleteSpy = jest.fn().mockResolvedValue({});
    const batchSpy = jest.spyOn(admin.firestore(), "batch").mockReturnValue({
      set: jest.fn(),
      delete: deleteSpy,
      commit: jest.fn().mockResolvedValue(undefined),
    } as any);

    const userDocsByDstPath = new Map([[
      "/users/test-user-id/documents/test-doc-id",
        {
          action: "delete",
          dstPath: "/users/test-user-id/documents/test-doc-id",
        } as LogicResultDoc,
    ]]);
    await distribute(userDocsByDstPath);

    expect(batchSpy).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(deleteSpy).toHaveBeenCalledTimes(1);

    batchSpy.mockRestore();
  });

  it("should copy a document to dstPath with shallow copy mode", async () => {
    const setSpy = jest.fn().mockResolvedValue({});
    const batchSpy = jest.spyOn(admin.firestore(), "batch").mockReturnValue({
      set: setSpy,
      delete: jest.fn(),
      commit: jest.fn().mockResolvedValue(undefined),
    } as any);

    const userDocsByDstPath = new Map([[
      "/users/test-user-id/documents/test-doc-id",
        {
          action: "copy",
          copyMode: "shallow",
          srcPath: "/users/test-user-id-1/documents/test-doc-id",
          dstPath: "/users/test-user-id-2/documents/test-doc-id",
        } as LogicResultDoc,
    ]]);
    await distribute(userDocsByDstPath);

    expect(batchSpy).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(2);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id-1/documents/test-doc-id");
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id-2/documents/test-doc-id");
    expect(setSpy.mock.calls[0][1]).toEqual({});
    expect(setSpy).toHaveBeenCalledTimes(1);

    batchSpy.mockRestore();
  });

  it("should copy a document to dstPath with recursive copy mode", async () => {
    const setSpy = jest.fn().mockResolvedValue({});
    const batchSpy = jest.spyOn(admin.firestore(), "batch").mockReturnValue({
      set: setSpy,
      delete: jest.fn(),
      commit: jest.fn().mockResolvedValue(undefined),
    } as any);

    const srcDocMock = ({
      get: jest.fn().mockResolvedValue({
        exists: true,
        data: () => ({field: "value"}),
      }),
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;

    const mockDoc = jest.spyOn(admin.firestore(), "doc").mockReturnValue(srcDocMock);
    const mockExpandAndGroupDocPaths = jest.spyOn(paths, "expandAndGroupDocPaths").mockResolvedValue({});


    const userDocsByDstPath = new Map([[
      "/users/test-user-id-2/documents/test-doc-id",
        {
          action: "copy",
          copyMode: "recursive",
          srcPath: "/users/test-user-id-1/documents/test-doc-id",
          dstPath: "/users/test-user-id-2/documents/test-doc-id",
        } as LogicResultDoc,
    ]]);
    await distribute(userDocsByDstPath);

    expect(mockExpandAndGroupDocPaths).toHaveBeenCalledTimes(1);
    expect(batchSpy).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(2);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id-1/documents/test-doc-id");
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id-2/documents/test-doc-id");
    expect(setSpy.mock.calls[0][1]).toEqual({field: "value"});
    expect(setSpy).toHaveBeenCalledTimes(1);

    batchSpy.mockRestore();
    mockDoc.mockRestore();
    mockExpandAndGroupDocPaths.mockRestore();
  });
});

describe("revertModificationsOutsideForm", () => {
  let dbSpy: jest.SpyInstance;
  let mockSnapshot: DocumentSnapshot;

  beforeEach(() => {
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      get: jest.fn().mockResolvedValue(mockSnapshot),
      update: jest.fn().mockResolvedValue({}),
    } as any);

    mockSnapshot = {
      ref: {
        update: jest.fn().mockResolvedValue({}),
      },
      data: jest.fn().mockReturnValue({}),
    } as any;
  });

  afterEach(() => {
    dbSpy.mockRestore();
  });

  const mockDocument = {
    "name": "Alice",
    "age": 25,
    "@form": {
      "name": "Alicia",
      "age": 26,
      "@status": "submit",
    },
  };
  const mockBeforeDocument = {
    "name": "Alice",
    "age": 26,
    "@form": {
      "name": "Alice",
      "age": 26,
      "@status": "finished",
    },
  };

  it("should revert modifications to fields outside the @form object", async () => {
    const updateSpy = jest.spyOn(mockSnapshot.ref, "update");
    await revertModificationsOutsideForm(mockDocument, mockBeforeDocument, mockSnapshot);
    expect(updateSpy).toHaveBeenCalledWith({age: 26});
  });

  it("should not revert modifications to fields inside the @form object", async () => {
    const updateSpy = jest.spyOn(mockSnapshot.ref, "update");
    await revertModificationsOutsideForm(mockDocument, mockBeforeDocument, mockSnapshot);
    expect(updateSpy).not.toHaveBeenCalledWith({"@form": {"name": "Alice"}});
  });

  it("should not update the snapshot if there are no modifications to revert", async () => {
    const mockDocument = {
      "name": "Alice",
      "age": 26,
      "@form": {
        "name": "Alicia",
        "age": 25,
        "@status": "processing",
      },
    };
    const updateSpy = jest.spyOn(mockSnapshot.ref, "update");
    await revertModificationsOutsideForm(mockDocument, mockBeforeDocument, mockSnapshot);
    expect(updateSpy).not.toHaveBeenCalled();
  });
});

describe("validateForm", () => {
  const docPath = "users/1";
  it("returns an object with empty validationResult when document is valid", async () => {
    const entity = "user";
    const document = {
      name: "John Doe",
      email: "johndoe@example.com",
      password: "abc123",
    };
    const [hasValidationError, validationResult] = await validateForm(entity, document, docPath);
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
    const [hasValidationError, validationResult] = await validateForm(entity, document, docPath);
    expect(hasValidationError).toBe(true);
    expect(validationResult).toEqual({name: ["Name is required"]});
  });
});

describe("getFormModifiedFields", () => {
  it("should return an empty array when there are no form fields", () => {
    const document = {name: "John Doe", age: 30};
    const modifiedFields = getFormModifiedFields(document);
    expect(modifiedFields).toEqual([]);
  });

  it("should return an array of modified form fields", () => {
    const document = {
      "name": "John Doe",
      "age": 30,
      "@form": {
        "name": "Jane Doe",
        "address": "123 Main St",
        "@status": "submit",
      },
    };
    const modifiedFields = getFormModifiedFields(document);
    expect(modifiedFields).toEqual(["name", "address"]);
  });

  it("should return an empty array when form is empty", () => {
    const document = {
      "name": "John Doe",
      "age": 30,
      "@form": {
        "@status": "submit",
      },
    };
    const modifiedFields = getFormModifiedFields(document);
    expect(modifiedFields).toEqual([]);
  });
});

describe("delayFormSubmissionAndCheckIfCancelled", () => {
  test("should delay form submission for 500 ms and not cancel form submission", async () => {
    const delay = 500;
    const snapshot = {
      ref: {
        update: jest.fn(),
        get: jest.fn().mockResolvedValue({
          data: () => ({"@form": {"@status": "delay"}}),
        }),
      },
    };
    const cancelFormSubmission = await delayFormSubmissionAndCheckIfCancelled(delay, snapshot as any);
    expect(cancelFormSubmission).toBe(false);
    expect(snapshot.ref.update).toHaveBeenCalledTimes(1);
    expect(snapshot.ref.update).toHaveBeenCalledWith({"@form.@status": "delay"});
    expect(snapshot.ref.get).toHaveBeenCalledTimes(1);
    expect(snapshot.ref.get).toHaveBeenCalledWith();
  });

  test("should delay form submission for 2000 ms and cancel form submission", async () => {
    const delay = 2000;
    const snapshot = {
      ref: {
        update: jest.fn(),
        get: jest.fn().mockResolvedValue({
          data: () => ({"@form": {"@status": "cancel"}}),
        }),
      },
    };
    const cancelFormSubmission = await delayFormSubmissionAndCheckIfCancelled(delay, snapshot as any);
    expect(cancelFormSubmission).toBe(true);
    expect(snapshot.ref.update).toHaveBeenCalledTimes(1);
    expect(snapshot.ref.update).toHaveBeenCalledWith({"@form.@status": "delay"});
    expect(snapshot.ref.get).toHaveBeenCalledTimes(1);
    expect(snapshot.ref.get).toHaveBeenCalledWith();
  });
});

describe("runBusinessLogics", () => {
  const actionType = "create";
  const formModifiedFields = ["field1", "field2"];
  const entity = "user";
  const action:Action = {
    actionType,
    path: "users/user123",
    document: {
      "@form": {
        "@actionType": actionType,
      },
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
    initializeEmberFlow(admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    const results = await runBusinessLogics(actionType, formModifiedFields, entity, action);

    expect(logicFn1).toHaveBeenCalledWith(action);
    expect(logicFn2).toHaveBeenCalledWith(action);
    expect(logicFn3).not.toHaveBeenCalled();
    expect(results).toEqual([
      {status: "finished"},
      {status: "finished"},
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
    initializeEmberFlow(admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    const results = await runBusinessLogics(actionType, formModifiedFields, entity, action);

    expect(results).toEqual([]);
  });
});

describe("groupDocsByUserAndDstPath", () => {
  initializeEmberFlow(admin, dbStructure, Entity, {}, {}, []);
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


describe("consolidateLogicResultsDocs", () => {
  it("should consolidate logic results documents correctly", () => {
    // Arrange
    const logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", dstPath: "path1", doc: {field1: "value1"}, instructions: {field2: "++"}},
          {action: "delete", dstPath: "path2"},
        ],
      },
      {
        name: "logic 2",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", dstPath: "path1", doc: {field1: "value1a"}, instructions: {field2: "--"}},
          {action: "merge", dstPath: "path1", doc: {field3: "value3"}, instructions: {field4: "--"}},
          {action: "copy", srcPath: "path3", dstPath: "path4"},
          {action: "merge", dstPath: "path2", doc: {field4: "value4"}},
          {action: "merge", dstPath: "path7", doc: {field6: "value7"}},
        ],
      },
      {
        name: "logic 3",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "copy", srcPath: "path5", dstPath: "path6"},
          {action: "delete", dstPath: "path7"},
          {action: "delete", dstPath: "path7"},
          {action: "copy", srcPath: "path3", dstPath: "path4"},
        ],
      },
      {
        name: "logic 4",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "delete", dstPath: "path2"},
          {action: "copy", srcPath: "path3", dstPath: "path7"},
        ],
      },
    ];

    // Mock console.warn
    const consoleWarnSpy = jest.spyOn(console, "warn").mockImplementation(jest.fn());

    // Act
    const result = consolidateAndGroupByDstPath(logicResults);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc>([
      ["path1", {action: "merge", dstPath: "path1", doc: {field1: "value1a", field3: "value3"}, instructions: {field2: "--", field4: "--"}}],
      ["path2", {action: "delete", dstPath: "path2"}],
      ["path4", {action: "copy", srcPath: "path3", dstPath: "path4"}],
      ["path6", {action: "copy", srcPath: "path5", dstPath: "path6"}],
      ["path7", {action: "copy", srcPath: "path3", dstPath: "path7"}],
    ]);

    expect(result).toEqual(expectedResult);
    // Verify that console.warn was called
    expect(consoleWarnSpy).toHaveBeenCalledTimes(8);

    // Verify that console.warn was called with the correct message
    expect(consoleWarnSpy.mock.calls[0][0]).toBe("Overwriting key \"field1\" in doc for dstPath \"path1\"");
    expect(consoleWarnSpy.mock.calls[1][0]).toBe("Overwriting key \"field2\" in instructions for dstPath \"path1\"");
    expect(consoleWarnSpy.mock.calls[2][0]).toBe("Action \"merge\" ignored because a \"delete\" for dstPath \"path2\" already exists");
    expect(consoleWarnSpy.mock.calls[3][0]).toBe("Action \"merge\" for dstPath \"path7\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[4][0]).toBe("Action \"delete\" ignored because a \"delete\" for dstPath \"path7\" already exists");
    expect(consoleWarnSpy.mock.calls[5][0]).toBe("Action \"copy\" ignored because \"copy\" for dstPath \"path4\" already exists");
    expect(consoleWarnSpy.mock.calls[6][0]).toBe("Action \"delete\" ignored because a \"delete\" for dstPath \"path2\" already exists");
    expect(consoleWarnSpy.mock.calls[7][0]).toBe("Action \"delete\" for dstPath \"path7\" is being replaced by action \"copy\"");

    // Cleanup
    consoleWarnSpy.mockRestore();
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
