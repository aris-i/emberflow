import {onDocChange, _mockable, initializeEmberFlow} from "../index";
import * as indexutils from "../index-utils";
import * as admin from "firebase-admin";

import {LogicResult, LogicResultDoc, SecurityResult, ValidateFormResult} from "../types";
import DocumentData = firestore.DocumentData;
import {firestore} from "firebase-admin";
import DocumentSnapshot = firestore.DocumentSnapshot;
import * as functions from "firebase-functions";
import DocumentReference = firestore.DocumentReference;
import CollectionReference = firestore.CollectionReference;
import Timestamp = firestore.Timestamp;
import {dbStructure, Entity} from "../sample-custom/db-structure";

admin.initializeApp();
initializeEmberFlow(admin, dbStructure, Entity, {}, {}, []);
function createDocumentSnapshot(data: DocumentData|null, refPath: string, exists = true): DocumentSnapshot<DocumentData> {
  return {
    exists,
    id: "test-id",
    ref: {
      id: "test-id",
      path: refPath,
      set: jest.fn(),
      update: jest.fn(),
      delete: jest.fn(),
      get: jest.fn(),
    },
    data: () => data,
    get: jest.fn(),
    isEqual: jest.fn(),
    create_time: _mockable.createNowTimestamp(),
    update_time: _mockable.createNowTimestamp(),
    readTime: _mockable.createNowTimestamp(),
  } as unknown as DocumentSnapshot<DocumentData>;
}

function createChange(beforeData: DocumentData|null, afterData: DocumentData|null, refPath: string): functions.Change<functions.firestore.DocumentSnapshot> {
  return {
    before: createDocumentSnapshot(beforeData, refPath),
    after: createDocumentSnapshot(afterData, refPath),
  };
}


describe("onDocChange", () => {
  const entity = "user";
  const refPath = "/example/test-id";

  const contextWithoutAuth: functions.EventContext = {
    auth: undefined,
    authType: "ADMIN",
    eventId: "test-event-id",
    eventType: "test-event-type",
    params: {},
    resource: {
      name: "projects/test-project/databases/(default)/documents/test",
      service: "firestore.googleapis.com",
      type: "type",
    },
    timestamp: "2022-03-15T18:52:04.369Z",
  };
  const contextWithAuth: functions.EventContext = {
    auth: {
      uid: "test-uid",
      token: {
        email: "test-email",
        email_verified: true,
        name: "test-name",
        picture: "test-picture",
        sub: "test-sub",
      },
    },
    authType: "ADMIN",
    eventId: "test-event-id",
    eventType: "test-event-type",
    params: {},
    resource: {
      name: "projects/test-project/databases/(default)/documents/test",
      service: "firestore.googleapis.com",
      type: "type",
    },
    timestamp: "2022-03-15T18:52:04.369Z",
  };

  beforeEach(() => {
    // ...
    jest.spyOn(admin, "initializeApp").mockImplementation();

    const docMock: DocumentReference = {
      set: jest.fn(),
      get: jest.fn(),
      update: jest.fn(),
      delete: jest.fn(),
      parent: {} as CollectionReference,
      path: "",
      firestore: {} as FirebaseFirestore.Firestore,
      id: "",
      isEqual: jest.fn(),

      // Add the missing properties and methods
      collection: jest.fn(),
      listCollections: jest.fn(),
      create: jest.fn(),
      onSnapshot: jest.fn(),
      withConverter: jest.fn(),
      // Add other required properties/methods as needed
    };


    const collectionMock: CollectionReference = {
      doc: jest.fn(() => docMock),
      // Add other required properties/methods as needed
    } as unknown as CollectionReference;

    // Mock admin.firestore
    const firestoreMock = jest.spyOn(admin, "firestore");

    const firestoreInstance: FirebaseFirestore.Firestore = {
      collection: jest.fn(() => collectionMock),
      settings: jest.fn(),
      doc: jest.fn(),
      collectionGroup: jest.fn(),
      getAll: jest.fn(),
      runTransaction: jest.fn(),
      batch: jest.fn(),
      terminate: jest.fn(),
      // Add the missing methods
      recursiveDelete: jest.fn(),
      listCollections: jest.fn(),
      bulkWriter: jest.fn(),
      bundle: jest.fn(),
    };

    firestoreMock.mockImplementation(() => firestoreInstance);

    // Add the spy for _mockable
    jest.spyOn(_mockable, "createNowTimestamp").mockReturnValue(Timestamp.now());
    jest.spyOn(console, "log").mockImplementation();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });


  it("should not execute when auth is null", async () => {
    const change = createChange({}, {}, refPath);
    await onDocChange(entity, change, contextWithoutAuth, "delete");
    expect(console.log).toHaveBeenCalledWith("Auth is null, then this change is initiated by the service account and should be ignored");
    expect(console.log).toHaveBeenCalledTimes(1);
  });

  it("should re-add deleted document", async () => {
    const deletedDocumentData = {
      field1: "value1",
      field2: "value2",
    };
    const change = createChange(deletedDocumentData, null, refPath);
    await onDocChange(entity, change, contextWithAuth, "delete");
    expect(change.after.ref.set).toHaveBeenCalledWith(deletedDocumentData);
    expect(change.after.ref.set).toHaveBeenCalledTimes(1);
    expect(console.log).toHaveBeenCalledWith("Document re-added with ID test-id");
    expect(console.log).toHaveBeenCalledTimes(4);
  });

  it("should revert modifications outside form", async () => {
    const beforeData = {
      field1: "oldValue",
      field2: "oldValue",
      field3: {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    const afterData = {
      "field1": "newValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "newValue",
        nestedField2: "oldValue",
      },
      "@form": {
        "@actionType": "update",
        "@status": "submit",
      },
    };

    const change = createChange(beforeData, afterData, refPath);

    // Mock validateForm to return a validation error
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([
      true,
      {
        field1: ["error message 1"],
        field2: ["error message 2"],
      },
    ]);

    const revertModificationsOutsideFormMock = jest.spyOn(
      indexutils,
      "revertModificationsOutsideForm"
    ).mockResolvedValue();

    await onDocChange("user", change, contextWithAuth, "update");

    expect(revertModificationsOutsideFormMock).toHaveBeenCalledWith(afterData, beforeData, change.after);
    expect(indexutils.validateForm).toHaveBeenCalledWith("user", afterData, refPath);
    expect(change.after.ref.update).toHaveBeenCalledWith({
      "@form.@status": "form-validation-failed",
      "@form.@message": {"field1": ["error message 1"], "field2": ["error message 2"]},
    });
    expect(change.after.ref.update).toHaveBeenCalledTimes(1);
  });

  it("should return on security check failure", async () => {
    const getSecurityFnMock = jest.spyOn(indexutils, "getSecurityFn");
    const rejectedSecurityResult: SecurityResult = {
      status: "rejected",
      message: "Unauthorized access",
    };
    const securityFnMock = jest.fn().mockResolvedValue(rejectedSecurityResult);
    getSecurityFnMock.mockReturnValue(securityFnMock);

    const beforeData = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
      "@form": {
        "field1": "oldValue",
        "field2": "oldValue",
        "@status": "submit",
      },
    };

    const afterData = {
      "field1": "oldValue",
      "field2": "oldValue",
      "field3": {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
      "@form": {
        "field1": "newValue",
        "field2": "oldValue",
        "@actionType": "update",
        "@status": "submit",
      },
    };

    const change = createChange(beforeData, afterData, refPath);

    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockResolvedValue([false, {}] as ValidateFormResult);
    await onDocChange(entity, change, contextWithAuth, "update");

    expect(getSecurityFnMock).toHaveBeenCalledWith(entity);
    expect(validateFormMock).toHaveBeenCalledWith(entity, change.after.data(), refPath);
    expect(securityFnMock).toHaveBeenCalledWith(entity, change.after.data(), "update", ["field1"]);
    expect(change.after.ref.update).toHaveBeenCalledWith({
      "@form.@status": "security-error",
      "@form.@message": "Unauthorized access",
    });
    expect(console.log).toHaveBeenCalledWith(`Security check failed: ${rejectedSecurityResult.message}`);
  });

  it("should call delayFormSubmissionAndCheckIfCancelled with correct parameters", async () => {
    jest.spyOn(indexutils, "revertModificationsOutsideForm").mockResolvedValue();
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
    jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
      Promise.resolve({status: "allowed"})
    );

    const delayFormSubmissionAndCheckIfCancelledSpy = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(true);

    const doc = {
      "@form": {
        "@delay": 1000,
        "@status": "submit",
        "@actionType": "create",
      },
      "someField": "exampleValue",
    };

    const change = createChange(null, doc, "/example/test-id");
    const event = "create";
    await onDocChange("user", change, contextWithAuth, event);

    // Test that the delayFormSubmissionAndCheckIfCancelled function is called with the correct parameters
    expect(delayFormSubmissionAndCheckIfCancelledSpy).toHaveBeenCalledWith(
      1000,
      change.after,
    );

    // Test that snapshot.ref.update is called with the correct parameters
    expect(change.after.ref.update).toHaveBeenCalledWith({"@form.@status": "cancelled"});
  });

  it("should not process the form if @form.@status is not 'submit'", async () => {
    jest.spyOn(indexutils, "revertModificationsOutsideForm").mockResolvedValue();
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
    jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
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
    await onDocChange("user", change, contextWithAuth, event);

    // Test that the delayFormSubmissionAndCheckIfCancelled function is NOT called
    expect(indexutils.revertModificationsOutsideForm).toHaveBeenCalled();
    expect(indexutils.getFormModifiedFields).not.toHaveBeenCalled();
    expect(indexutils.getSecurityFn).not.toHaveBeenCalled();
    expect(delayFormSubmissionAndCheckIfCancelledSpy).not.toHaveBeenCalled();

    // Test that snapshot.ref.update is NOT called
    expect(change.after.ref.update).not.toHaveBeenCalled();
  });

  it("should set @form.@status to 'submitted' after passing all checks", async () => {
    jest.spyOn(indexutils, "revertModificationsOutsideForm").mockResolvedValue();
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
    jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
      Promise.resolve({status: "allowed"})
    );
    jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const addActionSpy = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      add: addActionSpy,
    } as any);

    const doc = {
      "@form": {
        "@actionType": "create",
        "name": "test",
        "@status": "submit",
      },
      "someField": "exampleValue",
    };

    const change = createChange(null, doc, "/example/test-id");
    const event = "create";
    await onDocChange("user", change, contextWithAuth, event);

    // Test that the snapshot.ref.update is called with the correct parameters
    expect(change.after.ref.update).toHaveBeenCalledWith({"@form.@status": "processing"});
    expect(change.after.ref.update).toHaveBeenCalledWith({"@form.@status": "submitted"});

    // Test that addActionSpy is called with the correct parameters
    expect(addActionSpy).toHaveBeenCalled();
  });

  it("should set @form.@status to 'logic-error' if there are logic error results", async () => {
    jest.spyOn(indexutils, "revertModificationsOutsideForm").mockResolvedValue();
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue(["field1", "field2"]);
    jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() => Promise.resolve({status: "allowed"}));
    jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const errorMessage = "logic error message";
    const runBusinessLogicsSpy = jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue([
      {
        name: "testLogic",
        status: "error",
        message: errorMessage,
        timeFinished: _mockable.createNowTimestamp(),
        documents: [],
      },
    ]);

    const actionRefUpdateSpy = jest.fn();
    const collectionAddSpy = jest.fn().mockResolvedValue({
      update: actionRefUpdateSpy,
      collection: jest.fn().mockReturnValue({
        add: jest.fn(),
      }),
    });
    jest.spyOn(admin.firestore(), "collection").mockReturnValue({add: collectionAddSpy} as any);

    const doc = {
      "@form": {
        "@actionType": "create",
        "name": "test",
        "@status": "submit",
      },
      "someField": "exampleValue",
    };

    const change = createChange(null, doc, "/example/test-id");
    const event = "create";

    await onDocChange("user", change, contextWithAuth, event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsSpy).toHaveBeenCalledWith(
      "create",
      ["field1", "field2"],
      "user",
      expect.objectContaining({
        actionType: "create",
        document: {
          "@form": {
            "@actionType": "create",
            "@status": "submit",
            "name": "test",
          },
          "someField": "exampleValue",
        },
        modifiedFields: ["field1", "field2"],
        path: "/example/test-id",
        status: "processing",
        // TimeCreated is not specified because it's dynamic
      })
    );
    // Test that the snapshot.ref.update is called with the correct parameters
    expect(change.after.ref.update).toHaveBeenCalledTimes(3);
    const updateMock = change.after.ref.update as jest.Mock;
    expect(updateMock.mock.calls[0][0]).toEqual({"@form.@status": "processing"});
    expect(updateMock.mock.calls[1][0]).toEqual({"@form.@status": "submitted"});
    expect(updateMock.mock.calls[2][0]).toEqual({"@form.@status": "finished"});

    // Test that collectionAddSpy is not called if there are logic errors
    expect(collectionAddSpy).toHaveBeenCalledTimes(1);
    expect(collectionAddSpy).toHaveBeenCalledWith(expect.objectContaining({
      actionType: "create",
      document: {
        "@form": {
          "@actionType": "create",
          "@status": "submit",
          "name": "test",
        },
        "someField": "exampleValue",
      },
      modifiedFields: ["field1", "field2"],
      path: "/example/test-id",
      status: "processing",
      // TimeCreated is not specified because it's dynamic
    }));
    expect(actionRefUpdateSpy).toHaveBeenCalledTimes(2);
    expect(actionRefUpdateSpy.mock.calls[0][0]).toEqual({status: "finished-with-error", message: errorMessage});
  });

  // ... existing test code ...

  it("should execute the sequence of operations correctly", async () => {
    jest.spyOn(indexutils, "revertModificationsOutsideForm").mockResolvedValue();
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

    const actionRefUpdateSpy = jest.fn();
    const collectionAddSpy = jest.fn().mockResolvedValue({
      update: actionRefUpdateSpy,
      collection: jest.fn().mockReturnValue({
        add: jest.fn(),
      }),
    });
    jest.spyOn(admin.firestore(), "collection").mockReturnValue({add: collectionAddSpy} as any);

    const doc = {
      "@form": {
        "@actionType": "create",
        "name": "test",
        "@status": "submit",
      },
      "someField": "exampleValue",
    };

    const change = createChange(null, doc, "/example/test-id");
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

    await onDocChange("user", change, contextWithAuth, event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsSpy).toHaveBeenCalled();
    // Test that the snapshot.ref.update is called with the correct parameters
    expect(change.after.ref.update).toHaveBeenCalledTimes(3);

    expect(collectionAddSpy).toHaveBeenCalledTimes(1);

    // Test that the functions are called in the correct sequence
    expect(indexutils.consolidateAndGroupByDstPath).toHaveBeenNthCalledWith(1, businessLogicResults);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(1, consolidatedLogicResults, "test-uid");
    expect(indexutils.runViewLogics).toHaveBeenCalledWith(userDocsByDstPath);
    expect(indexutils.consolidateAndGroupByDstPath).toHaveBeenNthCalledWith(2, viewLogicResults);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(2, consolidatedViewLogicResults, "test-uid");
    expect(indexutils.distribute).toHaveBeenNthCalledWith(1, userDocsByDstPath);
    expect(indexutils.distribute).toHaveBeenNthCalledWith(2, userViewDocsByDstPath);
    expect(change.after.ref.update).toHaveBeenCalledWith({"@form.@status": "finished"});
    expect(indexutils.runPeerSyncViews).toHaveBeenCalledWith(userDocsByDstPath);
    expect(indexutils.consolidateAndGroupByDstPath).toHaveBeenNthCalledWith(3, peerSyncViewLogicResults);
    expect(indexutils.groupDocsByUserAndDstPath).toHaveBeenNthCalledWith(3, consolidatedPeerSyncViewLogicResults, "test-uid");
    expect(indexutils.distribute).toHaveBeenCalledWith(otherUsersDocsByDstPath);
    expect(indexutils.distribute).toHaveBeenCalledWith(otherUsersViewDocsByDstPath);
    expect(indexutils.distribute).toHaveBeenCalledWith(otherUsersPeerSyncViewDocsByDstPath);
    expect(actionRefUpdateSpy).toHaveBeenCalledTimes(1);
    expect(actionRefUpdateSpy.mock.calls[0][0]).toEqual({status: "finished"});
  });
});
