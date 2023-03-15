import {Entity} from "../custom/db-structure";
import * as functions from "firebase-functions";
import {onDocChange} from "../index";
import * as indexutils from "../index-utils";
import * as admin from "firebase-admin";
import {SecurityResult, ValidateFormResult} from "../types";
import DocumentData = firestore.DocumentData;
import {firestore} from "firebase-admin";
import DocumentSnapshot = firestore.DocumentSnapshot;

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
    create_time: admin.firestore.Timestamp.now(),
    update_time: admin.firestore.Timestamp.now(),
    readTime: admin.firestore.Timestamp.now(),
  } as unknown as DocumentSnapshot<DocumentData>;
}

function createChange(beforeData: DocumentData, afterData: DocumentData|null, refPath: string): functions.Change<functions.firestore.DocumentSnapshot> {
  return {
    before: createDocumentSnapshot(beforeData, refPath),
    after: createDocumentSnapshot(afterData, refPath),
  };
}

jest.spyOn(console, "log").mockImplementation();
jest.spyOn(console, "info").mockImplementation();

describe("onDocChange", () => {
  const entity: Entity = Entity.User;
  const userId = "test-user-id";
  const refPath = "/example/test-id";

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
    await onDocChange(entity, change, context, "delete");
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
      field1: "newValue",
      field2: "oldValue",
      field3: {
        nestedField1: "newValue",
        nestedField2: "oldValue",
      },
    };

    const change = createChange(beforeData, afterData, refPath);

    // Mock validateForm to return a validation error
    jest.spyOn(indexutils, "validateForm").mockReturnValue([
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

    await onDocChange(Entity.User, change, context, "update");

    expect(revertModificationsOutsideFormMock).toHaveBeenCalledWith(afterData, beforeData, change.after);
    expect(indexutils.validateForm).toHaveBeenCalledWith(Entity.User, afterData);
    expect(change.after.ref.update).toHaveBeenCalledWith({"@form.@status": "form-validation-failed", "@form.@message": {"field1": ["error message 1"], "field2": ["error message 2"]}});
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
        "@status": "submit",
      },
    };

    const change = createChange(beforeData, afterData, refPath);

    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockReturnValue([false, {}] as ValidateFormResult);

    await onDocChange(entity, change, context, "update");

    expect(getSecurityFnMock).toHaveBeenCalledWith(entity);
    expect(validateFormMock).toHaveBeenCalledWith(entity, change.after.data());
    expect(securityFnMock).toHaveBeenCalledWith(entity, change.after.data(), "update", ["field1"]);
    expect(change.after.ref.update).toHaveBeenCalledWith({"@form.@status": "security-error", "@form.@message": "Unauthorized access"});
    expect(console.log).toHaveBeenCalledWith(`Security check failed: ${rejectedSecurityResult.message}`);
  });
});
