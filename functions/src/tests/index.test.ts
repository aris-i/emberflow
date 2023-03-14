import {Entity} from "../custom/db-structure";
import * as functions from "firebase-functions";
import {onDocChange} from "../index";
import * as indexutils from "../index-utils";
import * as admin from "firebase-admin";
import {firestore} from "firebase-admin";
import DocumentData = firestore.DocumentData;
import CollectionReference = firestore.CollectionReference;
import Firestore = firestore.Firestore;
import DocumentReference = firestore.DocumentReference;
import Mocked = jest.Mocked;


jest.spyOn(console, "log").mockImplementation();
jest.spyOn(console, "info").mockImplementation();

describe("onDocChange", () => {
  const entity: Entity = Entity.User;
  const userId = "test-user-id";
  const refPath = "/example/test-id";
  const deletedDocumentSnapshot = jest.fn().mockImplementation(() => {
    return {
      exists: true,
      id: "test-id",
      ref: {
        id: "test-id",
        path: refPath,
      },
      data: () => ({
        field1: "value1",
        field2: "value2",
      }),
    };
  });

  let context: functions.EventContext;

  beforeEach(() => {
    context = {
      auth: {
        uid: userId,
        token: {
          email: "test@test.com",
          email_verified: true,
          name: "test",
          picture: "test",
          sub: "test",
        },
      },
      params: {
        docId: "test-id",
      },
      eventId: "event-id",
      eventType: "google.firestore.document.write",
      resource: {
        name: "projects/test-project/databases/(default)/documents/example/test-id",
        service: "firestore.googleapis.com",
        type: "type",
      },
      timestamp: "2022-03-15T18:52:04.369Z",
    };
    jest.clearAllMocks();
  });

  it("should not execute when auth is null", async () => {
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
    await onDocChange(entity, {after: null, before: deletedDocumentSnapshot()}, contextWithoutAuth, "delete");
    expect(console.log).toHaveBeenCalledWith("Auth is null, then this change is initiated by the service account and should be ignored");
    expect(console.log).toHaveBeenCalledTimes(1);
  });

  it("should re-add deleted document", async () => {
    const ref = {
      id: "test-id",
      path: refPath,
      set: jest.fn(),
    };
    const before = {
      ...deletedDocumentSnapshot(),
      ref,
    };
    const deleteChange: functions.Change<functions.firestore.DocumentSnapshot | null> = {
      after: null,
      before,
    };
    await onDocChange(entity, deleteChange, context, "delete");
    expect(ref.set).toHaveBeenCalledWith({
      field1: "value1",
      field2: "value2",
    });
    expect(ref.set).toHaveBeenCalledTimes(1);
    expect(console.log).toHaveBeenCalledWith("Document re-added with ID test-id");
    expect(console.log).toHaveBeenCalledTimes(4);
  });

  it("should revert modifications outside form", async () => {
    const before = {
      field1: "oldValue",
      field2: "oldValue",
      field3: {
        nestedField1: "oldValue",
        nestedField2: "oldValue",
      },
    };
    const after = {
      field1: "newValue",
      field2: "oldValue",
      field3: {
        nestedField1: "newValue",
        nestedField2: "oldValue",
      },
    };

    const updateMock = jest.fn();
    const firestore = {} as Firestore;
    const collectionRef = {} as CollectionReference<DocumentData>;

    const ref = {
      id: "test-id",
      firestore,
      parent: collectionRef,
      path: "example/test-id",
      collection: jest.fn(),
      update: updateMock,
      get: jest.fn(),
      isEqual: jest.fn(),
      delete: jest.fn(),
      withConverter: jest.fn(),
      listCollections: jest.fn(),
    } as unknown as Mocked<DocumentReference<DocumentData>>;

    const change: functions.Change<functions.firestore.DocumentSnapshot> = {
      after: {
        exists: true,
        id: "test-id",
        ref: ref as DocumentReference<DocumentData>,
        data: () => after,
        get: jest.fn(),
        isEqual: jest.fn(),
        create_time: admin.firestore.Timestamp.now(),
        update_time: admin.firestore.Timestamp.now(),
        readTime: admin.firestore.Timestamp.now(),
      } as functions.firestore.DocumentSnapshot,
      before: {
        exists: true,
        id: "test-id",
        ref: ref as DocumentReference<DocumentData>,
        data: () => before,
        get: jest.fn(),
        isEqual: jest.fn(),
        create_time: admin.firestore.Timestamp.now(),
        update_time: admin.firestore.Timestamp.now(),
        readTime: admin.firestore.Timestamp.now(),
      } as functions.firestore.DocumentSnapshot,
    };

    // Mock validateForm to return a validation error
    jest.spyOn(indexutils, "validateForm").mockReturnValue({
      hasValidationError: true,
      validationResult: {
        field1: ["error message 1"],
        field2: ["error message 2"],
      },
    });

    const revertModificationsOutsideFormMock = jest.spyOn(
      indexutils,
      "revertModificationsOutsideForm"
    ).mockResolvedValue();

    await onDocChange(Entity.User, change, context, "update");

    expect(revertModificationsOutsideFormMock).toHaveBeenCalledWith(after, before, change.after);
    expect(indexutils.validateForm).toHaveBeenCalledWith(Entity.User, after);
    expect(ref.update).toHaveBeenCalledWith({"@form.@status": "form-validation-failed", "@form.@message": {"field1": ["error message 1"], "field2": ["error message 2"]}});
    expect(ref.update).toHaveBeenCalledTimes(1);
  });
});
