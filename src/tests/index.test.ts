import {onFormSubmit, _mockable, initializeEmberFlow} from "../index";
import * as indexutils from "../index-utils";
import * as admin from "firebase-admin";

import {EventContext, LogicResult, LogicResultDoc, ProjectConfig, SecurityResult, ValidateFormResult} from "../types";
import {database, firestore} from "firebase-admin";
import * as functions from "firebase-functions";
import DocumentReference = firestore.DocumentReference;
import CollectionReference = firestore.CollectionReference;
import Timestamp = firestore.Timestamp;
import {dbStructure, Entity} from "../sample-custom/db-structure";
import Database = database.Database;
import {Firestore} from "firebase-admin/firestore";
import {DatabaseEvent, DataSnapshot} from "firebase-functions/lib/v2/providers/database";
import * as paths from "../utils/paths";

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

function createEvent(form: FirebaseFirestore.DocumentData): DatabaseEvent<DataSnapshot> {
  return {
    id: "test-id",
    data: createDocumentSnapshot(form),
    params: {
      formId: "test-fid",
      userId: "test-uid",
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
    // Add the spy for _mockable
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


  it("should return on security check failure", async () => {
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn");
    const rejectedSecurityResult: SecurityResult = {
      status: "rejected",
      message: "Unauthorized access",
    };
    const securityFnMock = jest.fn().mockResolvedValue(rejectedSecurityResult);
    getSecurityFnMock.mockReturnValue(securityFnMock);

    const docPath = "users/test-uid";
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
      "username": "test-user",
    };
    dataMock.mockReturnValueOnce(user);

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
    expect(console.log).toHaveBeenCalledWith(`Security check failed: ${rejectedSecurityResult.message}`);
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
    const runBusinessLogicsMock = jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue([
      {
        name: "testLogic",
        status: "error",
        message: errorMessage,
        timeFinished: _mockable.createNowTimestamp(),
        documents: [],
      },
    ]);

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

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsMock).toHaveBeenCalledWith(
      "create",
      {"field1": "value1", "field2": "value2"},
      "user",
      expect.objectContaining({
        eventContext,
        actionType: "create",
        document: doc,
        modifiedFields: {"field1": "value1", "field2": "value2"},
        status: "processing",
        // TimeCreated is not specified because it's dynamic
      })
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
      // TimeCreated is not specified because it's dynamic
    }));
    expect(docMock.update).toHaveBeenCalledTimes(2);
    expect((docMock.update as jest.Mock).mock.calls[0][0]).toEqual({status: "finished-with-error", message: errorMessage});

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

    const businessLogicResults: LogicResult[] = [
      {
        name: "testLogic",
        status: "finished",
        timeFinished: _mockable.createNowTimestamp(),
        documents: [],
      },
    ];
    const runBusinessLogicsSpy =
        jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue(businessLogicResults);

    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": "users/test-uid",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);

    const consolidatedLogicResults = new Map<string, LogicResultDoc>();
    const consolidatedViewLogicResults = new Map<string, LogicResultDoc>();
    const consolidatedPeerSyncViewLogicResults = new Map<string, LogicResultDoc>();
    const userDocsByDstPath = new Map<string, LogicResultDoc>();
    const otherUsersDocsByDstPath = new Map<string, LogicResultDoc>();
    const viewLogicResults: LogicResult[] = [];
    const userViewDocsByDstPath = new Map<string, LogicResultDoc>();
    const otherUsersViewDocsByDstPath = new Map<string, LogicResultDoc>();
    const peerSyncViewLogicResults: LogicResult[] = [];
    const otherUsersPeerSyncViewDocsByDstPath = new Map<string, LogicResultDoc>();

    jest.spyOn(indexutils, "consolidateAndGroupByDstPath")
      .mockReturnValueOnce(consolidatedLogicResults)
      .mockReturnValueOnce(consolidatedViewLogicResults)
      .mockReturnValue(consolidatedPeerSyncViewLogicResults);
    jest.spyOn(indexutils, "groupDocsByUserAndDstPath")
      .mockReturnValueOnce({
        userDocsByDstPath,
        otherUsersDocsByDstPath,
      })
      .mockReturnValueOnce({
        userDocsByDstPath: userViewDocsByDstPath,
        otherUsersDocsByDstPath: otherUsersViewDocsByDstPath,
      })
      .mockReturnValueOnce({
        userDocsByDstPath: new Map<string, LogicResultDoc>(),
        otherUsersDocsByDstPath: otherUsersPeerSyncViewDocsByDstPath,
      });
    jest.spyOn(indexutils, "runViewLogics").mockResolvedValue(viewLogicResults);
    jest.spyOn(indexutils, "runPeerSyncViews").mockResolvedValue(peerSyncViewLogicResults);
    jest.spyOn(indexutils, "distribute");

    await onFormSubmit(event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsSpy).toHaveBeenCalled();
    expect(refMock.update).toHaveBeenCalledTimes(3);

    expect(docMock.set).toHaveBeenCalledTimes(2);

    // Test that the functions are called in the correct sequence
    expect(indexutils.consolidateAndGroupByDstPath).toHaveBeenNthCalledWith(1, businessLogicResults);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(1, consolidatedLogicResults, "test-uid");
    expect(indexutils.runViewLogics).toHaveBeenCalledWith(userDocsByDstPath);
    expect(indexutils.consolidateAndGroupByDstPath).toHaveBeenNthCalledWith(2, viewLogicResults);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(2, consolidatedViewLogicResults, "test-uid");
    expect(indexutils.distribute).toHaveBeenNthCalledWith(1, userDocsByDstPath);
    expect(indexutils.distribute).toHaveBeenNthCalledWith(2, userViewDocsByDstPath);
    expect(refMock.update).toHaveBeenCalledWith({"@status": "finished"});
    expect(indexutils.runPeerSyncViews).toHaveBeenCalledWith(userDocsByDstPath);
    expect(indexutils.consolidateAndGroupByDstPath).toHaveBeenNthCalledWith(3, peerSyncViewLogicResults);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(3, consolidatedPeerSyncViewLogicResults, "test-uid");
    expect(indexutils.distribute).toHaveBeenCalledWith(otherUsersDocsByDstPath);
    expect(indexutils.distribute).toHaveBeenCalledWith(otherUsersViewDocsByDstPath);
    expect(indexutils.distribute).toHaveBeenCalledWith(otherUsersPeerSyncViewDocsByDstPath);
    expect(docMock.update).toHaveBeenCalledTimes(1);
    expect((docMock.update as jest.Mock).mock.calls[0][0]).toEqual({status: "finished"});
  });
});
