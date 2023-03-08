import * as admin from "firebase-admin";
import {
  distribute,
  revertModificationsOutsideForm,
  validateForm,
  getFormModifiedFields,
  delayFormSubmissionAndCheckIfCancelled,
  runBusinessLogics,
  groupDocsByUserAndDstPath,
} from "../index-utils";
import {firestore} from "firebase-admin";
import DocumentSnapshot = firestore.DocumentSnapshot;
import {Entity} from "../custom/db-structure";
import {
  Action,
  LogicConfig,
  LogicResult,
} from "../types";


admin.initializeApp();

describe("distribute", () => {
  let dbSpy: jest.SpyInstance;

  beforeEach(() => {
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue({
      get: jest.fn().mockResolvedValue({exists: true, data: () => ({})}),
      set: jest.fn().mockResolvedValue({}),
      delete: jest.fn().mockResolvedValue({}),
    } as any);
  });

  afterEach(() => {
    dbSpy.mockRestore();
  });

  it("should copy a document to dstPath", async () => {
    const userDocsByDstPath = {
      "/users/test-user-id/documents/test-doc-id": [
        {
          doc: "test-doc-id",
          instructions: undefined,
          dstPath: "/users/test-user-id/documents/test-doc-id",
        },
      ],
    };
    await distribute(userDocsByDstPath);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(2);
    expect(admin.firestore().doc).toHaveBeenCalledWith("test-doc-id");
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(admin.firestore().doc("/users/test-user-id/documents/test-doc-id").set).toHaveBeenCalledWith({});
  });

  it("should delete a document at dstPath", async () => {
    const userDocsByDstPath = {
      "/users/test-user-id/documents/test-doc-id": [
        {
          doc: null,
          instructions: undefined,
          dstPath: "/users/test-user-id/documents/test-doc-id",
        },
      ],
    };
    await distribute(userDocsByDstPath);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(admin.firestore().doc("/users/test-user-id/documents/test-doc-id").delete).toHaveBeenCalled();
  });

  it("should merge a document to dstPath", async () => {
    const userDocsByDstPath = {
      "/users/test-user-id/documents/test-doc-id": [
        {
          doc: {name: "test-doc-name-updated"},
          instructions: undefined,
          dstPath: "/users/test-user-id/documents/test-doc-id",
        },
      ],
    };
    await distribute(userDocsByDstPath);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(admin.firestore().doc("/users/test-user-id/documents/test-doc-id").set).toHaveBeenCalledWith(
      {name: "test-doc-name-updated"},
      {merge: true}
    );
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
  it("returns an object with empty validationResult when document is valid", () => {
    const entity = Entity.User;
    const document = {
      name: "John Doe",
      email: "johndoe@example.com",
      password: "abc123",
    };
    const {hasValidationError, validationResult} = validateForm(entity, document);
    expect(hasValidationError).toBe(false);
    expect(validationResult).toEqual({});
  });

  it("returns an object with validation errors when document is invalid", () => {
    const entity = Entity.User;
    const document = {
      name: "",
      email: "johndoe@example.com",
      password: "abc",
    };
    const {hasValidationError, validationResult} = validateForm(entity, document);
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
  const entity = Entity.User;
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
        entities: [Entity.User],
        logicFn: logicFn1,
      },
      {
        name: "Logic 2",
        actionTypes: "all",
        modifiedFields: ["field2"],
        entities: [Entity.User],
        logicFn: logicFn2,
      },
      {
        name: "Logic 3",
        actionTypes: ["delete"],
        modifiedFields: ["field3"],
        entities: [Entity.User],
        logicFn: logicFn3,
      },
    ];
    const results = await runBusinessLogics(actionType, formModifiedFields, entity, action, logics);

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
        entities: [Entity.YourCustomEntity],
        logicFn: jest.fn(),
      },
      {
        name: "Logic 2",
        actionTypes: ["update"],
        modifiedFields: ["field2"],
        entities: [Entity.YourCustomEntity],
        logicFn: jest.fn(),
      },
      {
        name: "Logic 3",
        actionTypes: ["delete"],
        modifiedFields: ["field3"],
        entities: [Entity.YourCustomEntity],
        logicFn: jest.fn(),
      },
    ];
    const results = await runBusinessLogics(actionType, formModifiedFields, entity, action, logics);

    expect(results).toEqual([]);
  });
});

describe("groupDocsByUserAndDstPath", () => {
  const logicResults: LogicResult[] = [
    {
      name: "logic 1",
      status: "finished",
      execTime: 25,
      timeFinished: firestore.Timestamp.now(),
      documents: [
        {dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}},
        {dstPath: "users/user123/document2", doc: {field3: "value3"}},
        {dstPath: "users/user456/document3", doc: {field4: "value4"}},
        {dstPath: "users/user789/document4", doc: null},
        {dstPath: "othercollection/document5", doc: {field5: "value5"}},
      ],
    },
    {
      name: "logic 2",
      status: "finished",
      execTime: 25,
      timeFinished: firestore.Timestamp.now(),
      documents: [
        {dstPath: "users/user123/document2", doc: {field6: "value6"}},
        {dstPath: "users/user123/document6", doc: null},
        {dstPath: "users/user123/document7", doc: {field7: "value7"}},
      ],
    },
    {
      name: "logic 2",
      execTime: 25,
      timeFinished: firestore.Timestamp.now(),
      status: "error",
      documents: [{dstPath: "users/user123/document8", doc: {field8: "value8"}}],
    },
  ];

  it("should group documents by destination path and user", () => {
    const userId = "user123";
    const expectedResults = {
      userDocsByDstPath: {
        "users/user123/document1": [{dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}}],
        "users/user123/document2": [
          {dstPath: "users/user123/document2", doc: {field3: "value3"}},
          {dstPath: "users/user123/document2", doc: {field6: "value6"}},
        ],
        "users/user123/document6": [{dstPath: "users/user123/document6", doc: null}],
        "users/user123/document7": [{dstPath: "users/user123/document7", doc: {field7: "value7"}}],
      },
      otherUsersDocsByDstPath: {
        "users/user456/document3": [{dstPath: "users/user456/document3", doc: {field4: "value4"}}],
        "users/user789/document4": [{dstPath: "users/user789/document4", doc: null}],
        "othercollection/document5": [{dstPath: "othercollection/document5", doc: {field5: "value5"}}],
      },
    };

    const results = groupDocsByUserAndDstPath(logicResults, userId);

    expect(results).toEqual(expectedResults);
  });
});
