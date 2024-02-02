import * as admin from "firebase-admin";
import * as indexUtils from "../index-utils";
import {firestore} from "firebase-admin";
import {initializeEmberFlow, _mockable} from "../index";
import {
  Action,
  LogicConfig,
  LogicResult,
  LogicResultDocAction,
  LogicResultDoc,
  ProjectConfig,
  ViewLogicConfig,
  EventContext,
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
import {DocumentData, DocumentReference} from "firebase-admin/lib/firestore";
import * as indexutils from "../index-utils";
import SpyInstance = jest.SpyInstance;
import CollectionReference = firestore.CollectionReference;
import * as misc from "../utils/misc";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import {cleanLogicMetricsExecutions} from "../index-utils";

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

    await indexUtils.distributeDoc(logicResultDoc);
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

    await indexUtils.distributeDoc(logicResultDoc, batch);
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

    await indexUtils.distributeDoc(logicResultDoc);
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

    await indexUtils.distributeDoc(logicResultDoc);
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

    await indexUtils.distributeDoc(logicResultDoc, batch);
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

    await indexUtils.distributeDoc(logicResultDoc, batch);
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
    await indexUtils.distribute(userDocsByDstPath);

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
    await indexUtils.distribute(userDocsByDstPath);

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
    await indexUtils.distributeLater(usersDocsByDstPath);

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
    const [hasValidationError, validationResult] = await indexUtils.validateForm(entity, document);
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
    const [hasValidationError, validationResult] = await indexUtils.validateForm(entity, document);
    expect(hasValidationError).toBe(true);
    expect(validationResult).toEqual({name: ["Name is required"]});
  });
});

describe("getFormModifiedFields", () => {
  it("should return an empty object when there are no form fields", () => {
    const document = {name: "John Doe", age: 30};
    const modifiedFields = indexUtils.getFormModifiedFields({}, document);
    expect(modifiedFields).toEqual({});
  });

  it("should return an array of modified form fields", () => {
    const document = {
      "name": "John Doe",
      "age": 30,
    };
    const modifiedFields = indexUtils.getFormModifiedFields({
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
    const cancelFormSubmission = await indexUtils.delayFormSubmissionAndCheckIfCancelled(delay, formResponseRef as any);
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
    const cancelFormSubmission = await indexUtils.delayFormSubmissionAndCheckIfCancelled(delay, formResponseRef as any);
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
      entity: entity,
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
  let simulateSubmitFormSpy: jest.SpyInstance;
  let updateLogicMetricsSpy: jest.SpyInstance;
  let actionRef: DocumentReference;

  beforeEach(() => {
    distributeFn = jest.fn();
    logicFn1 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn2 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn3 = jest.fn().mockResolvedValue({status: "error", message: "Error message"});

    simulateSubmitFormSpy = jest.spyOn(indexUtils._mockable, "simulateSubmitForm").mockResolvedValue();
    updateLogicMetricsSpy = jest.spyOn(indexUtils._mockable, "updateLogicMetrics").mockResolvedValue();

    const dbDoc = ({
      get: jest.fn().mockResolvedValue({
        data: jest.fn().mockReturnValue({
          maxLogicResultPages: 10,
        }),
      }),
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);

    const setActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    actionRef = {
      set: setActionMock,
      update: updateActionMock,
    } as any as DocumentReference;

    jest.spyOn(_mockable, "initActionRef").mockReturnValue(actionRef);
  });

  afterEach(() => {
    // Cleanup
    logicFn1.mockRestore();
    logicFn2.mockRestore();
    logicFn3.mockRestore();
    distributeFn.mockRestore();
    dbSpy.mockRestore();
    simulateSubmitFormSpy.mockRestore();
    updateLogicMetricsSpy.mockRestore();
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
    const runStatus = await indexUtils.runBusinessLogics(actionRef, action, distributeFn);

    expect(logicFn1).toHaveBeenCalledWith(action, new Map(), undefined);
    expect(logicFn2).toHaveBeenCalledWith(action, new Map(), undefined);
    expect(logicFn3).not.toHaveBeenCalled();
    expect(distributeFn).toHaveBeenCalledTimes(1);
    expect(distributeFn).toHaveBeenCalledWith(actionRef,
      [expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
      expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      })], 0);
    expect(runStatus).toEqual("done");
    expect(updateLogicMetricsSpy).toHaveBeenCalledTimes(1);
    expect(simulateSubmitFormSpy).toHaveBeenCalledTimes(1);
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
    const runStatus = await indexUtils.runBusinessLogics(actionRef, action, distributeFn);

    expect(logicFn1).toHaveBeenCalledWith(action, new Map(), undefined);
    expect(logicFn2).toHaveBeenCalledWith(action, new Map(), undefined);
    expect(distributeFn).toHaveBeenCalledTimes(2);
    expect(distributeFn.mock.calls[0]).toEqual([actionRef,
      [expect.objectContaining({
        status: "partial-result",
        nextPage: {},
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }),
      expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      })], 0]);

    expect(logicFn2).toHaveBeenCalledWith(action, new Map(), {});
    expect(distributeFn.mock.calls[1]).toEqual([actionRef,
      [expect.objectContaining({
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      })], 1]);
    expect(runStatus).toEqual("done");
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
    const runStatus = await indexUtils.runBusinessLogics(actionRef, action, distributeFn);

    expect(logicFn1).not.toHaveBeenCalled();
    expect(logicFn2).not.toHaveBeenCalled();
    expect(logicFn3).not.toHaveBeenCalled();
    expect(distributeFn).toHaveBeenCalledTimes(1);
    expect(distributeFn).toHaveBeenCalledWith(actionRef, [], 0);
    expect(runStatus).toEqual("no-matching-logics");
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
      const runStatus = await indexUtils.runBusinessLogics(actionRef, action, distributeFn);

      expect(logicFn1).toHaveBeenCalledWith(action, new Map(), undefined);
      expect(logicFn2).toHaveBeenCalledWith(action, new Map(), undefined);
      expect(logicFn1).toHaveBeenCalledTimes(1);
      expect(logicFn2).toHaveBeenCalledTimes(10);
      expect(distributeFn).toHaveBeenCalledTimes(10);
      expect(runStatus).toEqual("done");
    });

  it("should return when a logic returns a \"cancel-then-retry\" status", async () => {
    logicFn2.mockResolvedValue({status: "cancel-then-retry"});
    logicFn3.mockResolvedValue({status: "finished"});
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
        actionTypes: "all",
        modifiedFields: ["field1"],
        entities: ["user"],
        logicFn: logicFn3,
      },
    ];
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    const runStatus = await indexUtils.runBusinessLogics(actionRef, action, distributeFn);

    expect(logicFn1).not.toHaveBeenCalled();
    expect(logicFn2).toHaveBeenCalledWith(action, new Map(), undefined);
    expect(logicFn3).toHaveBeenCalledWith(action, new Map(), undefined);
    expect(logicFn2).toHaveBeenCalledTimes(1);
    expect(logicFn3).toHaveBeenCalledTimes(1);
    expect(distributeFn).not.toHaveBeenCalled();
    expect(runStatus).toEqual("cancel-then-retry");
  });

  it("should pass data from previous logic to the next logic via shared map",
    async () => {
      const expectedSharedMap = new Map<string, any>();
      expectedSharedMap.set("another-document-id", {
        "@id": "another-document-id",
        "title": "Another Document Title",
      });
      expectedSharedMap.set("test-document-id", {
        "@id": "test-document-id",
        "title": "Test Document Title",
      });
      logicFn2.mockImplementation((action, sharedMap) => {
        sharedMap.set("another-document-id", expectedSharedMap.get("another-document-id"));
        return {status: "finished"};
      });
      logicFn3.mockImplementation((action, sharedMap) => {
        sharedMap.set("test-document-id", expectedSharedMap.get("test-document-id"));
        return {status: "finished"};
      });
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
          actionTypes: ["create"],
          modifiedFields: ["field2"],
          entities: ["user"],
          logicFn: logicFn3,
        },
      ];
      initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
      const runStatus = await indexUtils.runBusinessLogics(actionRef, action, distributeFn);

      expect(logicFn1).toHaveBeenCalledWith(action, expectedSharedMap, undefined);
      expect(logicFn2).toHaveBeenCalledWith(action, expectedSharedMap, undefined);
      expect(logicFn3).toHaveBeenCalledWith(action, expectedSharedMap, undefined);
      expect(runStatus).toEqual("done");
    });
});

describe("simulateSubmitForm", () => {
  const eventContext: EventContext = {
    id: "test-event-id",
    uid: "test-user-id",
    docPath: "servers/test-doc-id",
    docId: "test-doc-id",
    formId: "test-form-id",
    entity: "servers",
  };
  const user: DocumentData = {
    "@id": "test-user-id",
    "username": "Topic Creator",
    "avatarUrl": "Avatar URL",
    "firstName": "Topic",
    "lastName": "Creator",
  };
  const action: Action = {
    actionType: "create",
    eventContext: eventContext,
    user: user,
    document: {},
    status: "new",
    timeCreated: admin.firestore.Timestamp.now(),
    modifiedFields: {},
  };
  const distributeFn = jest.fn();

  let runBusinessLogicsSpy: jest.SpyInstance;
  let dataMock: jest.Mock;
  let docMock: DocumentReference;
  let now: Timestamp;

  beforeEach(() => {
    jest.spyOn(console, "debug").mockImplementation();
    jest.spyOn(console, "warn").mockImplementation();

    dataMock = jest.fn().mockReturnValue({});

    const getMock: CollectionReference = {
      data: dataMock,
    } as unknown as CollectionReference;

    docMock = {
      set: jest.fn(),
      get: jest.fn().mockResolvedValue(getMock),
      collection: jest.fn(() => collectionMock),
    } as unknown as DocumentReference;

    const collectionMock: CollectionReference = {
      doc: jest.fn(() => docMock),
    } as unknown as CollectionReference;

    jest.spyOn(admin.firestore(), "doc").mockReturnValue(docMock);
    jest.spyOn(admin.firestore(), "collection").mockReturnValue(collectionMock);

    now = Timestamp.now();
    jest.spyOn(indexUtils._mockable, "createNowTimestamp").mockReturnValue(now);

    runBusinessLogicsSpy =
      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue("done");
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should skip when there is no logic result doc with 'simulate-submit-form' action", async () => {
    await indexUtils._mockable.simulateSubmitForm([], action, distributeFn);
    expect(console.debug).toHaveBeenCalledTimes(1);
    expect(console.debug).toHaveBeenCalledWith("Simulating submit form: ", 0);
    expect(runBusinessLogicsSpy).not.toHaveBeenCalled();
  });

  it("should skip when entity is undefined", async () => {
    const logicResults: LogicResult[] = [];
    const logicResultDoc: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "no-entity/sample-doc-id",
      doc: {
        "@id": "no-entity/sample-doc-id",
      },
    };
    const logicResult: LogicResult = {
      name: "sampleLogicResult",
      status: "finished",
      documents: [logicResultDoc],
    };
    logicResults.push(logicResult);

    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(console.warn).toHaveBeenCalledTimes(1);
    expect(console.warn).toHaveBeenCalledWith("No matching entity found for logic no-entity/sample-doc-id. Skipping");
    expect(runBusinessLogicsSpy).not.toHaveBeenCalled();
  });

  it("should skip when logic result doc is undefined", async () => {
    const logicResults: LogicResult[] = [];
    const logicResultDoc: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "servers/sample-server-id",
    };
    const logicResult: LogicResult = {
      name: "sampleLogicResult",
      status: "finished",
      documents: [logicResultDoc],
    };
    logicResults.push(logicResult);

    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(console.warn).toHaveBeenCalledTimes(1);
    expect(console.warn).toHaveBeenCalledWith("LogicResultDoc.doc should not be undefined. Skipping");
    expect(runBusinessLogicsSpy).not.toHaveBeenCalled();
  });

  it("should skip when @actionType is undefined", async () => {
    const logicResults: LogicResult[] = [];
    const logicResultDoc: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "servers/sample-server-id",
      doc: {"@id": "sample-server-id"},
    };
    const logicResult: LogicResult = {
      name: "sampleLogicResult",
      status: "finished",
      documents: [logicResultDoc],
    };
    logicResults.push(logicResult);

    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(console.warn).toHaveBeenCalledTimes(1);
    expect(console.warn).toHaveBeenCalledWith("No @actionType found. Skipping");
    expect(runBusinessLogicsSpy).not.toHaveBeenCalled();
  });

  it("should skip when submitFormAs data is undefined", async () => {
    dataMock.mockReturnValueOnce(undefined);
    const logicResults: LogicResult[] = [];
    const logicResultDoc: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "servers/sample-server-id",
      doc: {
        "@actionType": "create",
        "@submitFormAs": "test-user-id",
      },
    };
    const logicResult: LogicResult = {
      name: "sampleLogicResult",
      status: "finished",
      documents: [logicResultDoc],
    };
    logicResults.push(logicResult);

    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(console.warn).toHaveBeenCalledTimes(1);
    expect(console.warn).toHaveBeenCalledWith("User test-user-id not found. Skipping");
    expect(runBusinessLogicsSpy).not.toHaveBeenCalled();
  });

  it("should simulate submit form correctly when submitFormAs is defined", async () => {
    dataMock.mockReturnValueOnce({"@id": "test-user-id"});
    const logicResults: LogicResult[] = [];
    const logicResultDoc: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "servers/sample-server-id",
      doc: {
        "@actionType": "create",
        "@submitFormAs": "test-user-id",
        "name": "sample-server-name",
        "createdAt": now,
      },
    };
    const logicResult: LogicResult = {
      name: "sampleLogicResult",
      status: "finished",
      documents: [logicResultDoc],
    };
    logicResults.push(logicResult);

    const expectedEventContext: EventContext = {
      id: action.eventContext.id + "-1",
      uid: action.eventContext.uid,
      formId: action.eventContext.formId + "-1",
      docId: "sample-server-id",
      docPath: logicResultDoc.dstPath,
      entity: "server",
    };
    const expectedAction: Action = {
      eventContext: expectedEventContext,
      actionType: "create",
      document: {},
      modifiedFields: {
        "name": "sample-server-name",
        "createdAt": now,
      },
      user: {"@id": "test-user-id"},
      status: "new",
      timeCreated: now,
    };

    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(docMock.set).toHaveBeenCalledTimes(1);
    expect(docMock.set).toHaveBeenCalledWith(expectedAction);
    expect(runBusinessLogicsSpy).toHaveBeenCalledTimes(1);
    expect(runBusinessLogicsSpy).toHaveBeenCalledWith(docMock, expectedAction, distributeFn);
  });

  it("should simulate submit form correctly when submitFormAs is undefined", async () => {
    const logicResults: LogicResult[] = [];
    const logicResultDoc: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "servers/sample-server-id",
      doc: {
        "@actionType": "create",
        "name": "sample-server-name",
        "createdAt": now,
      },
    };
    const logicResult: LogicResult = {
      name: "sampleLogicResult",
      status: "finished",
      documents: [logicResultDoc],
    };
    logicResults.push(logicResult);

    const expectedEventContext: EventContext = {
      id: action.eventContext.id + "-1",
      uid: action.eventContext.uid,
      formId: action.eventContext.formId + "-1",
      docId: "sample-server-id",
      docPath: logicResultDoc.dstPath,
      entity: "server",
    };
    const expectedAction: Action = {
      eventContext: expectedEventContext,
      actionType: "create",
      document: {},
      modifiedFields: {
        "name": "sample-server-name",
        "createdAt": now,
      },
      user: user,
      status: "new",
      timeCreated: now,
    };

    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(docMock.set).toHaveBeenCalledTimes(1);
    expect(docMock.set).toHaveBeenCalledWith(expectedAction);
    expect(runBusinessLogicsSpy).toHaveBeenCalledTimes(1);
    expect(runBusinessLogicsSpy).toHaveBeenCalledWith(docMock, expectedAction, distributeFn);
  });

  it("should simulate submit form correctly with multiple logic result docs", async () => {
    const logicResults: LogicResult[] = [];
    const logicResultDoc1: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "servers/sample-server-id",
      doc: {
        "@actionType": "create",
        "name": "sample-server-name",
        "createdAt": now,
      },
    };
    const logicResultDoc2: LogicResultDoc = {
      action: "merge",
      dstPath: "servers/merge-server-id",
      doc: {
        "@actionType": "update",
        "name": "sample-server-name",
      },
    };
    const logicResultDoc3: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "users/sample-user-id",
      doc: {
        "@actionType": "create",
        "name": "sample-user-name",
      },
    };
    const logicResult1: LogicResult = {
      name: "serverLogicResult",
      status: "finished",
      documents: [logicResultDoc1, logicResultDoc2],
    };
    const logicResult2: LogicResult = {
      name: "userLogicResult",
      status: "finished",
      documents: [logicResultDoc3],
    };
    logicResults.push(logicResult1);
    logicResults.push(logicResult2);

    const expectedServerEventContext: EventContext = {
      id: action.eventContext.id + "-1",
      uid: action.eventContext.uid,
      formId: action.eventContext.formId + "-1",
      docId: "sample-server-id",
      docPath: logicResultDoc1.dstPath,
      entity: "server",
    };
    const expectedServerAction: Action = {
      eventContext: expectedServerEventContext,
      actionType: "create",
      document: {},
      modifiedFields: {
        "name": "sample-server-name",
        "createdAt": now,
      },
      user: user,
      status: "new",
      timeCreated: now,
    };

    const expectedUserEventContext: EventContext = {
      id: action.eventContext.id + "-2",
      uid: action.eventContext.uid,
      formId: action.eventContext.formId + "-2",
      docId: "sample-user-id",
      docPath: logicResultDoc3.dstPath,
      entity: "user",
    };
    const expectedUserAction: Action = {
      eventContext: expectedUserEventContext,
      actionType: "create",
      document: {},
      modifiedFields: {
        "name": "sample-user-name",
      },
      user: user,
      status: "new",
      timeCreated: now,
    };

    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(docMock.set).toHaveBeenCalledTimes(2);
    expect(docMock.set).toHaveBeenNthCalledWith(1, expectedServerAction);
    expect(docMock.set).toHaveBeenNthCalledWith(2, expectedUserAction);
    expect(runBusinessLogicsSpy).toHaveBeenCalledTimes(2);
    expect(runBusinessLogicsSpy).toHaveBeenNthCalledWith(1, docMock, expectedServerAction, distributeFn);
    expect(runBusinessLogicsSpy).toHaveBeenNthCalledWith(2, docMock, expectedUserAction, distributeFn);
  });

  it("should skip when maximum retry count is reached", async () => {
    runBusinessLogicsSpy.mockResolvedValueOnce("cancel-then-retry")
      .mockResolvedValueOnce("cancel-then-retry")
      .mockResolvedValueOnce("cancel-then-retry")
      .mockResolvedValueOnce("cancel-then-retry")
      .mockResolvedValueOnce("cancel-then-retry")
      .mockResolvedValueOnce("cancel-then-retry");
    const logicResults: LogicResult[] = [];
    const doc: DocumentData = {
      "@actionType": "create",
    };
    const logicResultDoc: LogicResultDoc = {
      action: "simulate-submit-form",
      dstPath: "servers/sample-server-id",
      doc: doc,
    };
    const logicResult: LogicResult = {
      name: "sampleLogicResult",
      status: "finished",
      documents: [logicResultDoc],
    };
    logicResults.push(logicResult);

    const dateSpy: SpyInstance = jest.spyOn(Date, "now");
    dateSpy.mockReturnValueOnce(now.toMillis())
      .mockReturnValueOnce(now.toMillis() + 2000)
      .mockReturnValueOnce(now.toMillis())
      .mockReturnValueOnce(now.toMillis() + 4000)
      .mockReturnValueOnce(now.toMillis())
      .mockReturnValueOnce(now.toMillis() + 8000)
      .mockReturnValueOnce(now.toMillis())
      .mockReturnValueOnce(now.toMillis() + 16000)
      .mockReturnValueOnce(now.toMillis())
      .mockReturnValueOnce(now.toMillis() + 32000);
    await indexUtils._mockable.simulateSubmitForm(logicResults, action, distributeFn);
    expect(console.warn).toHaveBeenCalledTimes(1);
    expect(console.warn).toHaveBeenCalledWith("Maximum retry count reached for logic servers/sample-server-id");
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

    const results = indexUtils.groupDocsByUserAndDstPath(docsByDstPath, userId);

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
          {action: "merge", priority: "normal", dstPath: "path11/doc11", doc: {field3: "value3"}},
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
          {action: "create", priority: "normal", dstPath: "path10/doc10", doc: {field10: "value10"}, instructions: {field6: "++"}},
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
          {action: "create", priority: "normal", dstPath: "path9/doc9", instructions: {field4: "++"}},
          {action: "create", priority: "normal", dstPath: "path10/doc10", instructions: {field3: "--"}},
        ],
      },
      {
        name: "logic 4",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "delete", priority: "normal", dstPath: "path2/doc2"},
          {action: "copy", priority: "normal", srcPath: "path3/doc3", dstPath: "path7/doc7"},
          {action: "merge", priority: "normal", dstPath: "path9/doc9", doc: {field9: "value9"}},
          {action: "merge", priority: "normal", dstPath: "path11/doc11", doc: {field1: "value1"}, instructions: {field6: "++"}},
        ],
      },
    ];

    // Act
    const logicResultDocs = logicResults.map((logicResult) => logicResult.documents).flat();
    const result = await indexUtils.expandConsolidateAndGroupByDstPath(logicResultDocs);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc[]>([
      ["path1/doc1", [{action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a", field3: "value3"}, instructions: {field2: "--", field4: "--"}}]],
      ["path2/doc2", [{action: "delete", priority: "normal", dstPath: "path2/doc2"}]],
      ["path4/doc4", [{action: "merge", priority: "normal", dstPath: "path4/doc4", doc: {}, instructions: {}}]],
      ["path7/doc7", [{action: "delete", priority: "normal", dstPath: "path7/doc7"}]],
      ["path6/doc6", [{action: "merge", priority: "normal", dstPath: "path6/doc6", doc: {}}]],
      ["path8/doc8", [{action: "create", priority: "normal", dstPath: "path8/doc8", doc: {field1: "value1", field3: "value3"}, instructions: {field4: "--"}}]],
      ["path9/doc9", [{action: "create", priority: "normal", dstPath: "path9/doc9", doc: {field9: "value9"}, instructions: {field4: "++"}}]],
      ["path10/doc10", [{action: "create", priority: "normal", dstPath: "path10/doc10", doc: {field10: "value10"}, instructions: {field3: "--", field6: "++"}}]],
      ["path11/doc11", [{action: "merge", priority: "normal", dstPath: "path11/doc11", doc: {field3: "value3", field1: "value1"}, instructions: {field6: "++"}}]],
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
    const result = await indexUtils.expandConsolidateAndGroupByDstPath(logicResultDocs);

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
    const result = await indexUtils.expandConsolidateAndGroupByDstPath(logicResultDocs);

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
    const result = await indexUtils.expandConsolidateAndGroupByDstPath(logicResultDocs);

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
    jest.spyOn(indexUtils._mockable, "getViewLogicsConfig").mockReturnValue(customViewLogicsConfig);
  });

  it("should run view logics properly", async () => {
    const logicResult1: LogicResultDoc = {
      action: "merge" as LogicResultDocAction,
      priority: "normal",
      doc: {name: "value1", sampleField2: "value2"},
      dstPath: "users/user123",
    };
    const logicResult2: LogicResultDoc = {
      action: "delete" as LogicResultDocAction,
      priority: "normal",
      dstPath: "users/user124",
    };
    viewLogicFn1.mockResolvedValue({});
    viewLogicFn2.mockResolvedValue({});

    const results1 = await indexUtils.runViewLogics(logicResult1);
    const results2 = await indexUtils.runViewLogics(logicResult2);
    const results = [...results1, ...results2];

    expect(viewLogicFn1).toHaveBeenCalledTimes(2);
    expect(viewLogicFn1.mock.calls[0][0]).toBe(logicResult1);
    expect(viewLogicFn1.mock.calls[1][0]).toBe(logicResult2);
    expect(viewLogicFn2).toHaveBeenCalledTimes(1);
    expect(viewLogicFn2).toHaveBeenCalledWith(logicResult2);
    expect(results).toHaveLength(3);
  });
});

describe("updateLogicMetrics", () => {
  let colSpy: jest.SpyInstance;
  let docMock: jest.Mock;
  let queueInstructionsSpy: jest.SpyInstance;
  let warnSpy: jest.SpyInstance;
  let setMock: jest.Mock;

  beforeEach(() => {
    setMock = jest.fn().mockResolvedValue({});
    warnSpy = jest.spyOn(console, "warn").mockImplementation();
    docMock = jest.fn().mockImplementation((doc: string) => {
      return {
        path: `@metrics/${doc}`,
        collection: jest.fn().mockReturnValue({
          doc: jest.fn().mockReturnValue({
            set: setMock,
          }),
        }),
      } as unknown as DocumentReference;
    });
    colSpy = jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      doc: docMock,
    } as unknown as CollectionReference);
    queueInstructionsSpy = jest.spyOn(distribution, "queueInstructions").mockResolvedValue();
  });

  afterEach(() => {
    colSpy.mockRestore();
    docMock.mockRestore();
    queueInstructionsSpy.mockRestore();
    warnSpy.mockRestore();
  });

  it("should skip logic result when execTime is undefined", async () => {
    const logicResults: LogicResult[] = [];
    const logicResult = {
      name: "sampleLogicResult",
    } as LogicResult;
    logicResults.push(logicResult);

    await indexUtils._mockable.updateLogicMetrics(logicResults);
    expect(colSpy).toHaveBeenCalledTimes(1);
    expect(colSpy).toHaveBeenCalledWith("@metrics");
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy).toHaveBeenCalledWith("No execTime found for logic sampleLogicResult");
    expect(docMock).not.toHaveBeenCalled();
    expect(setMock).not.toHaveBeenCalled();
    expect(queueInstructionsSpy).not.toHaveBeenCalled();
  });

  it("should queue logic result metrics", async () => {
    const logicResults: LogicResult[] = [];
    const logicResult1 = {
      name: "sampleLogicResult",
      execTime: 100,
    } as LogicResult;
    const logicResult2 = {
      name: "anotherLogicResult",
      execTime: 200,
    } as LogicResult;
    logicResults.push(logicResult1);
    logicResults.push(logicResult2);

    await indexUtils._mockable.updateLogicMetrics(logicResults);
    expect(colSpy).toHaveBeenCalledTimes(1);
    expect(colSpy).toHaveBeenCalledWith("@metrics");
    expect(warnSpy).toHaveBeenCalledWith("anotherLogicResult took 200ms to execute");
    expect(docMock).toHaveBeenCalledTimes(2);
    expect(docMock).toHaveBeenNthCalledWith(1, "sampleLogicResult");
    expect(docMock).toHaveBeenNthCalledWith(2, "anotherLogicResult");
    expect(queueInstructionsSpy).toHaveBeenCalledTimes(2);
    expect(queueInstructionsSpy).toHaveBeenNthCalledWith(1,
      `@metrics/${logicResult1.name}`,
      {
        totalExecTime: `+${logicResult1.execTime}`,
        totalExecCount: "++",
      },
    );
    expect(queueInstructionsSpy).toHaveBeenNthCalledWith(2,
      `@metrics/${logicResult2.name}`,
      {
        totalExecTime: `+${logicResult2.execTime}`,
        totalExecCount: "++",
      },
    );
    expect(setMock).toHaveBeenCalledTimes(2);
    expect(setMock).toHaveBeenNthCalledWith(1, {
      execDate: expect.any(Timestamp),
      execTime: logicResult1.execTime,
    });
    expect(setMock).toHaveBeenNthCalledWith(2, {
      execDate: expect.any(Timestamp),
      execTime: logicResult2.execTime,
    });
  });
});

describe("cleanLogicMetricsExecutions", () => {
  let colGetMock: jest.Mock;
  let deleteCollectionSpy: jest.SpyInstance;

  beforeEach(() => {
    colGetMock = jest.fn().mockResolvedValue({
      docs: [
        {
          ref: {
            collection: jest.fn().mockReturnValue({
              where: jest.fn().mockReturnValue({}),
            }),
          },
        },
      ],
    });
    jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      get: colGetMock,
    } as any);
    deleteCollectionSpy = jest.spyOn(misc, "deleteCollection")
      .mockImplementation(async (query, callback) => {
        if (callback) {
          await callback({size: 1} as unknown as firestore.QuerySnapshot);
        }
        return Promise.resolve();
      });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should clean logic metrics executions", async () => {
    jest.spyOn(console, "info").mockImplementation();
    await cleanLogicMetricsExecutions({} as ScheduledEvent);

    expect(console.info).toHaveBeenCalledWith("Running cleanLogicMetricsExecutions");
    expect(admin.firestore().collection).toHaveBeenCalledTimes(1);
    expect(admin.firestore().collection).toHaveBeenCalledWith("@metrics");
    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(deleteCollectionSpy).toHaveBeenCalled();
    expect(console.info).toHaveBeenCalledWith("Cleaned 1 logic metrics executions");
  });
});
