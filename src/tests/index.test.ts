import {_mockable, initializeEmberFlow, onFormSubmit} from "../index";
import * as indexutils from "../index-utils";
import * as admin from "firebase-admin";
import * as viewLogics from "../logics/view-logics";
import {database, firestore} from "firebase-admin";

import {
  EventContext,
  LogicResult,
  LogicResultAction,
  LogicResultDoc,
  ProjectConfig,
  SecurityResult,
  ValidateFormResult,
} from "../types";
import * as functions from "firebase-functions";
import {dbStructure, Entity} from "../sample-custom/db-structure";
import {Firestore} from "firebase-admin/firestore";
import {DatabaseEvent, DataSnapshot} from "firebase-functions/lib/v2/providers/database";
import * as paths from "../utils/paths";
import * as adminClient from "emberflow-admin-client/lib";
import DocumentReference = firestore.DocumentReference;
import CollectionReference = firestore.CollectionReference;
import Timestamp = firestore.Timestamp;
import Database = database.Database;

const projectConfig: ProjectConfig = {
  projectId: "your-project-id",
  budgetAlertTopicName: "budget-alerts",
  region: "us-central1",
  rtdbName: "rtdb",
  maxCostLimitPerFunction: 100,
  specialCostLimitPerFunction: {
    function1: 50,
    function2: 75,
    function3: 120,
  },
};

jest.spyOn(admin, "initializeApp").mockImplementation();
jest.spyOn(admin, "app").mockImplementation();
jest.spyOn(adminClient, "initClient").mockImplementation();
jest.spyOn(adminClient, "submitForm").mockImplementation();

const dataMock = jest.fn().mockReturnValue({});

const getMock: CollectionReference = {
  data: dataMock,
} as unknown as CollectionReference;

const docMock: DocumentReference = {
  set: jest.fn(),
  get: jest.fn().mockResolvedValue(getMock),
  update: jest.fn(),
  collection: jest.fn(() => collectionMock),
} as unknown as DocumentReference;

const collectionMock: CollectionReference = {
  doc: jest.fn(() => docMock),
} as unknown as CollectionReference;

jest.spyOn(admin, "firestore")
  .mockImplementation(() => {
    return {
      collection: jest.fn(() => collectionMock),
      doc: jest.fn(() => docMock),
    } as unknown as Firestore;
  });

jest.spyOn(admin, "database")
  .mockImplementation(() => {
    return {
      ref: jest.fn(),
    } as unknown as Database;
  });

const parseEntityMock = jest.fn();
jest.spyOn(paths, "parseEntity")
  .mockImplementation(parseEntityMock);

admin.initializeApp();

initializeEmberFlow(projectConfig, admin, dbStructure, Entity, {}, {}, []);

const refMock = {
  update: jest.fn(),
};

function createDocumentSnapshot(form: FirebaseFirestore.DocumentData)
    : DataSnapshot {
  return {
    key: "test-id",
    ref: refMock,
    val: () => form,
  } as unknown as functions.database.DataSnapshot;
}

function createEvent(form: FirebaseFirestore.DocumentData, userId?: string): DatabaseEvent<DataSnapshot> {
  if (!userId) {
    const formData = JSON.parse(form.formData);
    userId = formData["@docPath"].split("/")[1];
  }
  return {
    id: "test-id",
    data: createDocumentSnapshot(form),
    params: {
      formId: "test-fid",
      userId,
    },
  } as unknown as DatabaseEvent<DataSnapshot>;
}

describe("onFormSubmit", () => {
  const entity = "user";

  const eventContext: EventContext = {
    id: "test-id",
    uid: "test-uid",
    formId: "test-fid",
    docId: "test-uid",
    docPath: "users/test-uid",
    entity,
  };

  beforeEach(() => {
    jest.spyOn(_mockable, "createNowTimestamp").mockReturnValue(Timestamp.now());
    jest.spyOn(console, "log").mockImplementation();
    jest.spyOn(console, "warn").mockImplementation();
    parseEntityMock.mockReturnValue({
      entity: "user",
      entityId: "test-uid",
    });
    refMock.update.mockReset();
  });


  it("should return when there's no matched entity", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@actionType": "update",
        "@docPath": "users/test-uid",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);

    const document = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    dataMock.mockReturnValue(document);

    parseEntityMock.mockReturnValue({
      entityId: "test-id",
    });
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@message": "docPath does not match any known Entity",
    });
    expect(console.warn).toHaveBeenCalledWith("docPath does not match any known Entity");
  });

  it("should return when target docPath is not allowed for given userId", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@actionType": "update",
        "@docPath": "users/another-user-id",
      }),
      "@status": "submit",
    };

    const event = createEvent(form, "user-id");

    const document = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    dataMock.mockReturnValue(document);

    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@message": "User id from path does not match user id from event params",
    });
    expect(console.warn).toHaveBeenCalledWith("User id from path does not match user id from event params");
  });

  it("should return when there's no provided @actionType", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@docPath": "users/test-uid",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);

    const document = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    dataMock.mockReturnValue(document);

    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@message": "No @actionType found",
    });
    expect(console.warn).toHaveBeenCalledWith("No @actionType found");
  });

  it("should return when no user data found", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@actionType": "update",
        "@docPath": "users/test-uid",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);

    const document = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    dataMock.mockReturnValueOnce(document);

    const user = undefined;
    dataMock.mockReturnValueOnce(user);

    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockResolvedValue([false, {}] as ValidateFormResult);
    await onFormSubmit(event);
    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@message": "No user data found",
    });
  });

  it("should return on security check failure and user should be forDistribution", async () => {
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn");
    const rejectedSecurityResult: SecurityResult = {
      status: "rejected",
      message: "Unauthorized access",
    };
    const securityFnMock = jest.fn().mockResolvedValue(rejectedSecurityResult);
    getSecurityFnMock.mockReturnValue(securityFnMock);

    const docPath = "@internal/forDistribution/distribution/0";
    const formData = {
      "field1": "newValue",
      "field2": "oldValue",
      "@actionType": "update",
      "@docPath": docPath,
    };
    const form = {
      "formData": JSON.stringify(formData),
      "@status": "submit",
    };

    const event = createEvent(form);

    const document = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    dataMock.mockReturnValueOnce(document);

    const user = {
      "@id": "forDistribution",
    };

    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockResolvedValue([false, {}] as ValidateFormResult);
    await onFormSubmit(event);

    expect(getSecurityFnMock).toHaveBeenCalledWith(entity);
    expect(validateFormMock).toHaveBeenCalledWith(entity, formData);
    expect(securityFnMock).toHaveBeenCalledWith(entity, docPath, document, "update", {field1: "newValue"}, user);
    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "security-error",
      "@message": "Unauthorized access",
    });
    // expect(console.log).toHaveBeenCalledWith(`Security check failed: ${rejectedSecurityResult.message}`);
    getSecurityFnMock.mockReset();
  });

  it("should call delayFormSubmissionAndCheckIfCancelled with correct parameters", async () => {
    const validateFormMock = jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    const getFormModifiedFieldsMock =
            jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue({
              "field1": "value1",
              "field2": "value2",
            });
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
      Promise.resolve({status: "allowed"})
    );
    const delayFormSubmissionAndCheckIfCancelledSpy = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(true);

    const form = {
      "formData": JSON.stringify({
        "@delay": 1000,
        "@actionType": "create",
        "someField": "exampleValue",
        "@docPath": "users/test-uid",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    await onFormSubmit(event);

    // Test that the delayFormSubmissionAndCheckIfCancelled function is called with the correct parameters
    expect(delayFormSubmissionAndCheckIfCancelledSpy).toHaveBeenCalledWith(
      1000,
      refMock,
    );

    expect(refMock.update).toHaveBeenCalledWith({"@status": "cancelled"});
    validateFormMock.mockReset();
    getFormModifiedFieldsMock.mockReset();
    getSecurityFnMock.mockReset();
    delayFormSubmissionAndCheckIfCancelledSpy.mockReset();
  });

  it("should set form @status to 'submitted' after passing all checks", async () => {
    const validateFormMock = jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    const getFormModifiedFieldsMock =
            jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue({
              "field1": "value1",
              "field2": "value2",
            });
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
      Promise.resolve({status: "allowed"})
    );
    const delayFormSubmissionAndCheckIfCancelledMock = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const setActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const initActionRefMock = jest.spyOn(_mockable, "initActionRef").mockResolvedValue({
      set: setActionMock,
      update: updateActionMock,
    } as any);

    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": "users/test-uid",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({"@status": "processing"});
    expect(refMock.update).toHaveBeenCalledWith({"@status": "submitted"});

    // Test that addActionSpy is called with the correct parameters
    expect(setActionMock).toHaveBeenCalled();
    expect(updateActionMock).toHaveBeenCalled();

    validateFormMock.mockReset();
    getFormModifiedFieldsMock.mockReset();
    getSecurityFnMock.mockReset();
    delayFormSubmissionAndCheckIfCancelledMock.mockReset();
    setActionMock.mockReset();
    updateActionMock.mockReset();
    initActionRefMock.mockReset();
  });

  it("should set form @status to 'submitted' after passing all checks even with non user path", async () => {
    const getFormModifiedFieldsMock =
        jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue({
          "field1": "value1",
          "field2": "value2",
        });
    const delayFormSubmissionAndCheckIfCancelledMock = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const setActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const initActionRefMock = jest.spyOn(_mockable, "initActionRef").mockResolvedValue({
      set: setActionMock,
      update: updateActionMock,
    } as any);

    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": "@internal/forDistribution/distributions/0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({"@status": "processing"});
    expect(refMock.update).toHaveBeenCalledWith({"@status": "submitted"});

    // Test that addActionSpy is called with the correct parameters
    expect(setActionMock).toHaveBeenCalled();
    expect(updateActionMock).toHaveBeenCalled();

    getFormModifiedFieldsMock.mockReset();
    delayFormSubmissionAndCheckIfCancelledMock.mockReset();
    setActionMock.mockReset();
    updateActionMock.mockReset();
    initActionRefMock.mockReset();
  });

  it("should set form @status to 'cancelled' if there is a cancel-then-retry logic result status", async () => {
    const delayFormSubmissionAndCheckIfCancelledMock =
      jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const setActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const initActionRefMock = jest.spyOn(_mockable, "initActionRef").mockResolvedValue({
      set: setActionMock,
      update: updateActionMock,
    } as any);

    const runBusinessLogicsSpy =
      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue("cancel-then-retry");

    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": "@internal/forDistribution/distributions/0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({"@status": "processing"});
    expect(refMock.update).toHaveBeenCalledWith({"@status": "submitted"});
    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "cancelled",
      "@message": "cancel-then-retry received from business logic",
    });

    expect(setActionMock).toHaveBeenCalled();
    expect(updateActionMock).not.toHaveBeenCalled();

    delayFormSubmissionAndCheckIfCancelledMock.mockReset();
    setActionMock.mockReset();
    updateActionMock.mockReset();
    initActionRefMock.mockReset();
    runBusinessLogicsSpy.mockReset();
  });

  it("should set action-status to 'finished-with-error' if there are logic error results", async () => {
    const validateFormMock = jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    const getFormModifiedFieldsMock =
            jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue({
              "field1": "value1",
              "field2": "value2",
            });
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() => Promise.resolve({status: "allowed"}));
    const delayFormSubmissionAndCheckIfCancelledMock = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const errorMessage = "logic error message";
    const runBusinessLogicsMock = jest.spyOn(indexutils, "runBusinessLogics").mockImplementation(
      async (actionType, formModifiedFields, entity, action, distributeFn) => {
        const logicResults: LogicResult[] = [
          {
            name: "testLogic",
            status: "error",
            message: errorMessage,
            timeFinished: _mockable.createNowTimestamp(),
            documents: [],
          },
        ];
        await distributeFn(logicResults, 0);
        return "done";
      }
    );

    const form = {
      "formData": JSON.stringify({
        "@docPath": "users/test-uid",
        "@actionType": "create",
        "name": "test",
      }),
      "@status": "submit",
    };
    const doc = {
      name: "test",
      description: "test description",
    };
    dataMock.mockReturnValue(doc);

    const event = createEvent(form);

    await onFormSubmit(event);

    const expectedAction = {
      actionType: "create",
      document: {
        description: "test description",
        name: "test",
      },
      eventContext: {
        docId: "test-uid",
        docPath: "users/test-uid",
        entity: "user",
        formId: "test-fid",
        id: "test-id",
        uid: "test-uid",
      },
      modifiedFields: {
        field1: "value1",
        field2: "value2",
      },
      timeCreated: _mockable.createNowTimestamp(),
      user: {
        description: "test description",
        name: "test",
      },
      status: "processing",
    };

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsMock).toHaveBeenCalledWith(
      "create",
      {"field1": "value1", "field2": "value2"},
      "user",
      expectedAction,
      expect.any(Function)
    );

    // form should still finish successfully
    expect(refMock.update).toHaveBeenCalledTimes(3);
    expect(refMock.update.mock.calls[0][0]).toEqual({"@status": "processing"});
    expect(refMock.update.mock.calls[1][0]).toEqual({"@status": "submitted"});
    expect(refMock.update.mock.calls[2][0]).toEqual({"@status": "finished"});

    expect(docMock.set).toHaveBeenCalledWith(expect.objectContaining({
      eventContext,
      actionType: "create",
      document: doc,
      modifiedFields: {"field1": "value1", "field2": "value2"},
      status: "processing",
    }));
    expect(docMock.update).toHaveBeenCalledTimes(1);
    expect((docMock.update as jest.Mock).mock.calls[0][0]).toEqual({
      status: "finished-with-error",
      message: errorMessage,
    });

    validateFormMock.mockReset();
    getFormModifiedFieldsMock.mockReset();
    getSecurityFnMock.mockReset();
    delayFormSubmissionAndCheckIfCancelledMock.mockReset();
    runBusinessLogicsMock.mockReset();
    (docMock.set as jest.Mock).mockReset();
    (docMock.update as jest.Mock).mockReset();
  });

  it("should execute the sequence of operations correctly", async () => {
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue({"field1": "value1", "field2": "value2"});
    jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() => Promise.resolve({status: "allowed"}));
    jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);
    jest.spyOn(indexutils, "distribute").mockResolvedValue();
    jest.spyOn(indexutils, "distributeLater").mockResolvedValue();
    jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();

    const highPriorityDocs: LogicResultDoc[] = [{
      action: "create" as LogicResultAction,
      dstPath: "users/test-uid",
      priority: "high",
    }];
    const normalPriorityDocs: LogicResultDoc[] = [{
      action: "create" as LogicResultAction,
      dstPath: "users/test-uid",
      priority: "normal",
    },
    {
      action: "create" as LogicResultAction,
      dstPath: "users/test-uid-2",
      priority: "normal",
    },
    ];
    const lowPriorityDocs: LogicResultDoc[] = [{
      action: "create" as LogicResultAction,
      dstPath: "users/test-uid",
      priority: "low",
    }];

    const logicResults: LogicResult[] = [
      {
        name: "testLogic",
        status: "finished",
        timeFinished: _mockable.createNowTimestamp(),
        documents: [...highPriorityDocs, ...normalPriorityDocs, ...lowPriorityDocs],
      },
    ];

    const runBusinessLogicsSpy =
            jest.spyOn(indexutils, "runBusinessLogics").mockImplementation(
              async (actionType, formModifiedFields, entity, action, distributeFn) => {
                await distributeFn(logicResults, 0);
                return "done";
              }
            );

    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": "users/test-uid",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);

    const consolidateHighPriorityDocs = new Map<string, LogicResultDoc[]>([["users/test-uid", [highPriorityDocs[0]]], ["users/test-uid-2", [highPriorityDocs[0]]]]);
    const consolidateNormalPriorityDocs = new Map<string, LogicResultDoc[]>([["users/test-uid", [normalPriorityDocs[0]]], ["users/test-uid", [normalPriorityDocs[1]]], ["users/test-uid-2", [normalPriorityDocs[0]]], ["users/test-uid-2", [normalPriorityDocs[1]]]]);
    const consolidateLowPriorityDocs = new Map<string, LogicResultDoc[]>([["users/test-uid", [lowPriorityDocs[0]]], ["users/test-uid-2", [lowPriorityDocs[0]]], ["users/test-uid-3", [lowPriorityDocs[0]]]]);

    const userHighPriorityByDstPath = new Map<string, LogicResultDoc[]>([["users/test-uid", [highPriorityDocs[0]]]]);
    const otherUsersHighPriorityByDstPath = new Map<string, LogicResultDoc[]>([["users/test-uid-2", [highPriorityDocs[0]]]]);
    const userNormalPriorityByDstPath = new Map<string, LogicResultDoc[]>([["users/test-uid", [normalPriorityDocs[0]]], ["users/test-uid", [normalPriorityDocs[1]]]]);
    const otherUsersNormalPriorityByDstPath = new Map<string, LogicResultDoc[]>([["users/test-uid-2", [normalPriorityDocs[0]]], ["users/test-uid-2", [normalPriorityDocs[1]]]]);
    const userLowPriorityByDstPath = new Map<string, LogicResultDoc[]>([["users/test-uid", [lowPriorityDocs[0]]]]);
    const otherUsersLowPriorityByDstPath = new Map<string, LogicResultDoc[]>([["users/test-uid-2", [lowPriorityDocs[0]]], ["users/test-uid-3", [lowPriorityDocs[0]]]]);

    const consolidatedViewLogicResults = new Map<string, LogicResultDoc[]>([]);
    // const consolidatedViewLogicResults = new Map<string, LogicResultDoc>([["users/test-uid", logicResults[0]]]);
    const consolidatedPeerSyncViewLogicResults = new Map<string, LogicResultDoc[]>([["users/test-uid-2", [highPriorityDocs[0]]]]);
    const userDocsMap = [
      userHighPriorityByDstPath,
      userNormalPriorityByDstPath,
      userLowPriorityByDstPath,
    ];
    const userDocs = userDocsMap
      .flatMap((map) => Array.from(map.values()))
      .flat();
    const viewLogicResults: LogicResult[] = [{
      name: "User ViewLogic",
      status: "finished",
      timeFinished: _mockable.createNowTimestamp(),
      documents: logicResults.map((result) => result.documents).flat(),
    }];
    const viewLogicResultDocs = viewLogicResults.map((result) => result.documents).flat();
    const userViewDocs = viewLogicResultDocs.filter((doc) => [doc.dstPath === "users/test-uid"]);
    const userViewDocsByDstPath = new Map<string, LogicResultDoc[]>(userViewDocs.map((doc) => [doc.dstPath, [doc]]));
    const otherUserViewDocs = viewLogicResultDocs.filter((doc) => [doc.dstPath !== "users/test-uid"]);
    const otherUsersViewDocsByDstPath = new Map<string, LogicResultDoc[]>(otherUserViewDocs.map((doc) => [doc.dstPath, [doc]]));
    const peerSyncViewLogicResults: LogicResult[] = [{
      name: "SyncPeerViews",
      status: "finished",
      timeFinished: _mockable.createNowTimestamp(),
      documents: logicResults.map((result) => result.documents).flat(),
    }];

    const peerSyncViewLogicResultDocs = peerSyncViewLogicResults.map((result) => result.documents).flat();
    const peerSyncViewDocs = peerSyncViewLogicResultDocs.filter((doc) => [doc.dstPath !== "users/test-uid"]);
    const otherUsersPeerSyncViewDocsByDstPath = new Map<string, LogicResultDoc[]>(peerSyncViewDocs.map((doc) => [doc.dstPath, [doc]]));

    jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath")
      .mockResolvedValueOnce(consolidateHighPriorityDocs)
      .mockResolvedValueOnce(consolidateNormalPriorityDocs)
      .mockResolvedValueOnce(consolidateLowPriorityDocs)
      .mockResolvedValueOnce(consolidatedViewLogicResults)
      .mockResolvedValue(consolidatedPeerSyncViewLogicResults);
    jest.spyOn(indexutils, "groupDocsByUserAndDstPath")
      .mockReturnValueOnce({
        userDocsByDstPath: userHighPriorityByDstPath,
        otherUsersDocsByDstPath: otherUsersHighPriorityByDstPath,
      })
      .mockReturnValueOnce({
        userDocsByDstPath: userNormalPriorityByDstPath,
        otherUsersDocsByDstPath: otherUsersNormalPriorityByDstPath,
      })
      .mockReturnValueOnce({
        userDocsByDstPath: userLowPriorityByDstPath,
        otherUsersDocsByDstPath: otherUsersLowPriorityByDstPath,
      })
      .mockReturnValueOnce({
        userDocsByDstPath: userViewDocsByDstPath,
        otherUsersDocsByDstPath: otherUsersViewDocsByDstPath,
      })
      .mockReturnValueOnce({
        userDocsByDstPath: new Map<string, LogicResultDoc[]>(),
        otherUsersDocsByDstPath: otherUsersPeerSyncViewDocsByDstPath,
      });
    jest.spyOn(indexutils, "runViewLogics").mockResolvedValue(viewLogicResults);
    jest.spyOn(indexutils, "distribute");
    jest.spyOn(indexutils, "distributeLater");

    await onFormSubmit(event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsSpy).toHaveBeenCalled();
    expect(docMock.set).toHaveBeenCalledTimes(6);
    expect(refMock.update).toHaveBeenCalledTimes(3);
    expect(refMock.update).toHaveBeenNthCalledWith(1, {"@status": "processing"});
    expect(refMock.update).toHaveBeenNthCalledWith(2, {"@status": "submitted"});
    expect(refMock.update).toHaveBeenNthCalledWith(3, {"@status": "finished"});

    // Test that the functions are called in the correct sequence
    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(1, highPriorityDocs);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(1, consolidateHighPriorityDocs, "test-uid");
    expect(indexutils.distribute).toHaveBeenNthCalledWith(1, userHighPriorityByDstPath);
    expect(indexutils.distribute).toHaveBeenNthCalledWith(2, otherUsersHighPriorityByDstPath);

    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(2, normalPriorityDocs);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(2, consolidateNormalPriorityDocs, "test-uid");
    expect(indexutils.distribute).toHaveBeenNthCalledWith(3, userNormalPriorityByDstPath);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(1, otherUsersNormalPriorityByDstPath);

    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(3, lowPriorityDocs);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(3, consolidateLowPriorityDocs, "test-uid");
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(2, userLowPriorityByDstPath);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(3, otherUsersLowPriorityByDstPath);

    expect(viewLogics.queueRunViewLogics).toHaveBeenCalledWith(userDocs);

    expect(docMock.update).toHaveBeenCalledTimes(1);
    expect((docMock.update as jest.Mock).mock.calls[0][0]).toEqual({status: "finished"});
  });
});
