import {onDocChange, _mockable, initializeEmberFlow} from "../index";
import * as indexutils from "../index-utils";
import * as admin from "firebase-admin";

import {EventContext, LogicResult, LogicResultDoc, ProjectConfig, SecurityResult, ValidateFormResult} from "../types";
import DocumentData = firestore.DocumentData;
import {database, firestore} from "firebase-admin";
import * as functions from "firebase-functions";
import DocumentReference = firestore.DocumentReference;
import CollectionReference = firestore.CollectionReference;
import Timestamp = firestore.Timestamp;
import {dbStructure, Entity} from "../sample-custom/db-structure";
import Database = database.Database;
import {Firestore} from "firebase-admin/firestore";

// TODO: Create unit test for all new functions and modified functions

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
const funcName = "testFunction";

jest.spyOn(admin, "initializeApp").mockImplementation();

const dataMock = jest.fn().mockReturnValue({});

const getMock: CollectionReference = {
  data: dataMock,
} as unknown as CollectionReference;

const docMock: DocumentReference = {
  set: jest.fn(),
  get: jest.fn().mockResolvedValue(getMock),
  update: jest.fn(),
  collection: jest.fn(),
} as unknown as DocumentReference;

const collectionMock: CollectionReference = {
  doc: jest.fn(() => docMock),
} as unknown as CollectionReference;

const firestoreMock = jest.spyOn(admin, "firestore")
  .mockImplementation(() => {
    return {
      collection: jest.fn(() => collectionMock),
      doc: jest.fn(() => docMock),
    } as unknown as Firestore;
  });

const refMock = {
  update: jest.fn(),
};

const rtdbMock = jest.spyOn(admin, "database")
  .mockImplementation(() => {
    return {
      ref: jest.fn(() => refMock),
    } as unknown as Database;
  });
console.log(firestoreMock, rtdbMock);

admin.initializeApp();

initializeEmberFlow(projectConfig, admin, dbStructure, Entity, {}, {}, []);
function createDocumentSnapshot(data: DocumentData|null, refPath: string, exists = true)
    : functions.database.DataSnapshot {
  return {
    exists,
    id: "test-id",
    ref: {
      set: jest.fn(),
      update: jest.fn(),
      remove: jest.fn(),
    },
    val: () => data,
  } as unknown as functions.database.DataSnapshot;
}

function createChange(beforeData: DocumentData|null, afterData: DocumentData|null, refPath: string): functions.Change<functions.database.DataSnapshot> {
  return {
    before: createDocumentSnapshot(beforeData, refPath),
    after: createDocumentSnapshot(afterData, refPath),
  };
}


describe("onDocChange", () => {
  const entity = "user";
  const refPath = "/forms/test-id";

  const eventContext: EventContext = {
    id: "test-id",
    uid: "test-uid",
    formId: "test-form-id",
    docId: "test-doc-id",
    docPath: "/users/test-uid/test/doc-path",
  };

  beforeEach(() => {
    // Add the spy for _mockable
    jest.spyOn(_mockable, "createNowTimestamp").mockReturnValue(Timestamp.now());
    jest.spyOn(console, "log").mockImplementation();
    refMock.update.mockReset();
  });


  it("should return on security check failure", async () => {
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn");
    const rejectedSecurityResult: SecurityResult = {
      status: "rejected",
      message: "Unauthorized access",
    };
    const securityFnMock = jest.fn().mockResolvedValue(rejectedSecurityResult);
    getSecurityFnMock.mockReturnValue(securityFnMock);

    const beforeFormData = {
      "field1": "oldValue",
      "field2": "oldValue",
      "@status": "submit",
    };

    const afterFormData = {
      "field1": "newValue",
      "field2": "oldValue",
      "@actionType": "update",
      "@status": "submit",
    };

    const change = createChange(beforeFormData, afterFormData, refPath);

    const document = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    dataMock.mockReturnValue(document);

    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockResolvedValue([false, {}] as ValidateFormResult);
    await onDocChange(funcName, entity, change, eventContext, "update");

    expect(getSecurityFnMock).toHaveBeenCalledWith(entity);
    expect(validateFormMock).toHaveBeenCalledWith(entity, change.after.val());
    expect(securityFnMock).toHaveBeenCalledWith(entity, change.after.val(), document, "update", ["field1"]);
    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "security-error",
      "@message": "Unauthorized access",
    });
    expect(console.log).toHaveBeenCalledWith(`Security check failed: ${rejectedSecurityResult.message}`);
    getSecurityFnMock.mockReset();
  });

  it("should call delayFormSubmissionAndCheckIfCancelled with correct parameters", async () => {
    const validateFormMock = jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    const getFormModifiedFieldsMock = jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
      Promise.resolve({status: "allowed"})
    );
    const delayFormSubmissionAndCheckIfCancelledSpy = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(true);

    const doc = {
      "@delay": 1000,
      "@status": "submit",
      "@actionType": "create",
      "someField": "exampleValue",
    };

    const change = createChange(null, doc, "/example/test-id");
    const event = "create";
    await onDocChange(funcName, "user", change, eventContext, event);

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

  it("should not process the form if @status is not 'submit'", async () => {
    const validateFormMock = jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    const getFormModifiedFieldsMock = jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
      Promise.resolve({status: "allowed"})
    );

    const delayFormSubmissionAndCheckIfCancelledSpy = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(true);

    const doc = {
      "@form": {
        "@actionType": "create",
        "@status": "not-submit",
        "@delay": 1000,
      },
      "someField": "exampleValue",
    };

    const change = createChange(null, doc, "/example/test-id");
    const event = "create";
    await onDocChange(funcName, "user", change, eventContext, event);

    // Test that the delayFormSubmissionAndCheckIfCancelled function is NOT called
    expect(indexutils.getFormModifiedFields).not.toHaveBeenCalled();
    expect(indexutils.getSecurityFn).not.toHaveBeenCalled();
    expect(delayFormSubmissionAndCheckIfCancelledSpy).not.toHaveBeenCalled();
    expect(refMock.update).not.toHaveBeenCalled();

    validateFormMock.mockReset();
    getFormModifiedFieldsMock.mockReset();
    getSecurityFnMock.mockReset();
  });

  it("should set form @status to 'submitted' after passing all checks", async () => {
    const validateFormMock = jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    const getFormModifiedFieldsMock = jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
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

    const doc = {
      "@actionType": "create",
      "name": "test",
      "@status": "submit",
    };

    const change = createChange(null, doc, "/example/test-id");
    const event = "create";
    await onDocChange(funcName, "user", change, eventContext, event);

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

  it("should set @form.@status to 'logic-error' if there are logic error results", async () => {
    const validateFormMock = jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    const getFormModifiedFieldsMock = jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
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

    const setActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const initActionRefMock = jest.spyOn(_mockable, "initActionRef").mockResolvedValue({
      set: setActionMock,
      update: updateActionMock,
      collection: jest.fn().mockReturnValue({
        doc: jest.fn().mockReturnValue({
          set: setActionMock,
          update: updateActionMock,
        }),
      }),
    } as any);

    const form = {
      "@actionType": "create",
      "name": "test",
      "@status": "submit",
    };
    const doc = {
      name: "test",
      description: "test description",
    };
    dataMock.mockReturnValue(doc);

    const change = createChange(null, form, "/example/test-id");
    const event = "create";

    await onDocChange(funcName, "user", change, eventContext, event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsMock).toHaveBeenCalledWith(
      "create",
      ["field1", "field2"],
      "user",
      expect.objectContaining({
        eventContext,
        actionType: "create",
        document: doc,
        form: {
          "@actionType": "create",
          "@status": "submit",
          "name": "test",
        },
        modifiedFields: ["field1", "field2"],
        status: "processing",
        // TimeCreated is not specified because it's dynamic
      })
    );
    expect(refMock.update).toHaveBeenCalledTimes(3);
    expect(refMock.update.mock.calls[0][0]).toEqual({"@status": "processing"});
    expect(refMock.update.mock.calls[1][0]).toEqual({"@status": "submitted"});
    expect(refMock.update.mock.calls[2][0]).toEqual({"@status": "finished"});

    // Test that collectionAddSpy is not called if there are logic errors
    expect(setActionMock).toHaveBeenCalledWith(expect.objectContaining({
      eventContext,
      actionType: "create",
      document: doc,
      form: {
        "@actionType": "create",
        "@status": "submit",
        "name": "test",
      },
      modifiedFields: ["field1", "field2"],
      status: "processing",
      // TimeCreated is not specified because it's dynamic
    }));
    expect(updateActionMock).toHaveBeenCalledTimes(2);
    expect(updateActionMock.mock.calls[0][0]).toEqual({status: "finished-with-error", message: errorMessage});

    validateFormMock.mockReset();
    getFormModifiedFieldsMock.mockReset();
    getSecurityFnMock.mockReset();
    delayFormSubmissionAndCheckIfCancelledMock.mockReset();
    runBusinessLogicsMock.mockReset();
    setActionMock.mockReset();
    updateActionMock.mockReset();
    initActionRefMock.mockReset();
  });


  it("should execute the sequence of operations correctly", async () => {
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
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

    const setActionSpy = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionSpy = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    jest.spyOn(_mockable, "initActionRef").mockResolvedValue({
      set: setActionSpy,
      update: updateActionSpy,
      collection: jest.fn().mockReturnValue({
        doc: jest.fn().mockReturnValue({
          set: setActionSpy,
          update: updateActionSpy,
        }),
      }),
    } as any);

    const form = {
      "@actionType": "create",
      "name": "test",
      "@status": "submit",
    };

    const change = createChange(null, form, "/example/test-id");
    const event = "create";

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

    await onDocChange(funcName, "user", change, eventContext, event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsSpy).toHaveBeenCalled();
    expect(refMock.update).toHaveBeenCalledTimes(3);

    expect(setActionSpy).toHaveBeenCalledTimes(2);

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
    expect(updateActionSpy).toHaveBeenCalledTimes(1);
    expect(updateActionSpy.mock.calls[0][0]).toEqual({status: "finished"});
  });
});
