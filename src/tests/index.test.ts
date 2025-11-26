import {_mockable, db, initializeEmberFlow, onFormSubmit, onUserRegister} from "../index";
import * as indexutils from "../index-utils";
import * as transactionutils from "../utils/transaction";
import * as admin from "firebase-admin";
import * as viewLogics from "../logics/view-logics";
import * as patchLogics from "../logics/patch-logics";
import {database, firestore} from "firebase-admin";

import {
  LogicResult,
  LogicResultDocAction,
  LogicResultDoc,
  ProjectConfig,
  SecurityResult,
  ValidateFormResult, RunBusinessLogicStatus,
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
import {expandConsolidateAndGroupByDstPath, groupDocsByTargetDocPath} from "../index-utils";
import FieldValue = firestore.FieldValue;
import * as distribution from "../utils/distribution";
import {UserRecord} from "firebase-admin/lib/auth";
import Transaction = firestore.Transaction;

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

const updateMock: jest.Mock = jest.fn();
const setMock = jest.fn();
const docMock: DocumentReference = {
  set: setMock,
  get: jest.fn().mockResolvedValue(getMock),
  update: updateMock,
  collection: jest.fn(() => collectionMock),
} as unknown as DocumentReference;

const collectionMock: CollectionReference = {
  doc: jest.fn(() => docMock),
} as unknown as CollectionReference;

const txnGetFnMock = jest.fn();
const transactionSetMock = jest.fn();
const transactionUpdateMock = jest.fn();
const transactionMock = {
  get: txnGetFnMock,
  set: transactionSetMock,
  update: transactionUpdateMock,
  delete: jest.fn(),
} as any;

jest.spyOn(admin, "firestore")
  .mockImplementation(() => {
    return {
      Timestamp: {now: jest.fn(() => Timestamp.now())},
      collection: jest.fn(() => collectionMock),
      doc: jest.fn(() => docMock),
      runTransaction: jest.fn((fn) => fn(transactionMock)),
    } as unknown as Firestore;
  });

jest.spyOn(admin, "database")
  .mockImplementation(() => {
    return {
      ref: jest.fn(),
    } as unknown as Database;
  });

admin.initializeApp();

initializeEmberFlow(projectConfig, admin, dbStructure, Entity, [], [], [], []);

const refUpdateMock = jest.fn();
const refMock = {
  update: refUpdateMock,
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
  let parseEntityMock: jest.SpyInstance;

  const document = {
    "field1": "oldValue",
    "field2": "oldValue",
    "field3": {
      nestedField1: "oldValue",
      nestedField2: "oldValue",
    },
  };
  beforeEach(() => {
    jest.spyOn(indexutils._mockable, "saveMetricExecution").mockResolvedValue();
    jest.spyOn(_mockable, "createNowTimestamp").mockReturnValue(Timestamp.now());
    jest.spyOn(indexutils._mockable, "saveMetricExecution").mockResolvedValue();
    jest.spyOn(console, "log").mockImplementation();
    jest.spyOn(patchLogics, "queueRunPatchLogics").mockResolvedValue();
    jest.spyOn(console, "warn").mockImplementation();
    parseEntityMock = jest.spyOn(paths, "parseEntity").mockReturnValue({
      entity: "user",
      entityId: "test-uid",
    });
    refMock.update.mockReset();
    updateMock.mockReset();
    txnGetFnMock.mockResolvedValue({
      data: () => ({
        totalTodos: 3,
        notStartedCount: 1,
        inProgressCount: 1,
        doneCount: 1,
      }),
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should return when there's no app version in metadata", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@actionType": "update",
        "@docPath": "users/test-uid",
        "@metadata": {},
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    parseEntityMock.mockReturnValue({
      entity: undefined,
      entityId: "test-uid",
    });
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@messages": "No appVersion found in metadata",
    });
    expect(console.warn).toHaveBeenCalledWith("No appVersion found in metadata");
  });

  it("should return when there's no matched entity", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@actionType": "update",
        "@docPath": "users/test-uid",
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    parseEntityMock.mockReturnValue({
      entity: undefined,
      entityId: "test-uid",
    });
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@messages": "docPath does not match any known Entity",
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
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form, "user-id");
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@messages": "User id from path does not match user id from event params",
    });
    expect(console.warn).toHaveBeenCalledWith("User id from path does not match user id from event params");
  });

  it("should pass user validation when target docPath is user for service account", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@actionType": "update",
        "@docPath": "users/another-user-id",
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form, "service");
    await onFormSubmit(event);
    expect(updateMock.mock.calls[0][0]).toEqual({
      "execTime": expect.any(Number),
    });
  });

  it("should return when there's no provided @actionType", async () => {
    const form = {
      "formData": JSON.stringify({
        "field1": "newValue",
        "field2": "oldValue",
        "@docPath": "users/test-uid",
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@messages": "No @actionType found",
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
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    dataMock.mockReturnValueOnce(document).mockReturnValueOnce(undefined);
    txnGetFnMock.mockResolvedValue({
      data: () => undefined,
    });
    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockResolvedValue([false, {}] as ValidateFormResult);
    await onFormSubmit(event);
    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "error",
      "@messages": "No user data found",
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
      "@appVersion": "1.0.0",
    };
    const form = {
      "formData": JSON.stringify(formData),
      "@status": "submit",
    };

    const event = createEvent(form);
    const user = {
      "@id": "forDistribution",
      "username": "forDistribution",
    };

    txnGetFnMock
      .mockResolvedValueOnce({data: ()=> (document)})
      .mockReturnValueOnce({data: ()=> (user)});

    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockResolvedValue([false, {}] as ValidateFormResult);

    const extractedTxnGet = {
      get: txnGetFnMock,
    };
    jest.spyOn(transactionutils, "extractTransactionGetOnly").mockReturnValue( extractedTxnGet);
    await onFormSubmit(event);

    expect(getSecurityFnMock).toHaveBeenCalledWith(entity, "1.0.0");
    expect(validateFormMock).toHaveBeenCalledWith(entity, formData, "1.0.0");
    expect(securityFnMock).toHaveBeenCalledWith(extractedTxnGet, entity, docPath, document, "update", {field1: "newValue"}, user);
    expect(refMock.update).toHaveBeenCalledWith({
      "@status": "security-error",
      "@messages": "Unauthorized access",
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
      Promise.resolve({status: "allowed"}),
    );
    const delayFormSubmissionAndCheckIfCancelledSpy = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(true);

    const form = {
      "formData": JSON.stringify({
        "@delay": 1000,
        "@actionType": "create",
        "someField": "exampleValue",
        "@docPath": "users/test-uid",
        "@appVersion": "1.0.0",
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
      Promise.resolve({status: "allowed"}),
    );
    const delayFormSubmissionAndCheckIfCancelledMock = jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const setActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const initActionRefMock = jest.spyOn(_mockable, "initActionRef").mockReturnValue({
      set: setActionMock,
      update: updateActionMock,
    } as any as DocumentReference);

    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": "users/test-uid",
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({"@status": "processing"});
    expect(refMock.update).toHaveBeenCalledWith({"@status": "submitted"});

    // Test that addActionSpy is called with the correct parameters
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
    const delayFormSubmissionAndCheckIfCancelledMock =
      jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);

    const setActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const updateActionMock = jest.fn().mockResolvedValue({
      update: jest.fn(),
    });

    const initActionRefMock = jest.spyOn(_mockable, "initActionRef").mockReturnValue({
      set: setActionMock,
      update: updateActionMock,
    } as any as DocumentReference);

    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": "@internal/forDistribution/distributions/0",
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);
    await onFormSubmit(event);

    expect(refMock.update).toHaveBeenCalledWith({"@status": "processing"});
    expect(refMock.update).toHaveBeenCalledWith({"@status": "submitted"});

    // Test that addActionSpy is called with the correct parameters
    expect(updateActionMock).toHaveBeenCalled();

    getFormModifiedFieldsMock.mockReset();
    delayFormSubmissionAndCheckIfCancelledMock.mockReset();
    setActionMock.mockReset();
    updateActionMock.mockReset();
    initActionRefMock.mockReset();
  });

  it("should set action-status to 'finished-with-error' if there are logic error results", async () => {
    jest.clearAllMocks();
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
      async (txnGet, action) => {
        const logicResults: LogicResult[] = [
          {
            name: "testLogic",
            status: "error",
            message: errorMessage,
            timeFinished: _mockable.createNowTimestamp(),
            documents: [],
          },
        ];
        const result: RunBusinessLogicStatus= {
          status: "done",
          logicResults,
        };
        return result;
      },
    );

    const form = {
      "formData": JSON.stringify({
        "@docPath": "users/test-uid",
        "@actionType": "create",
        "name": "test",
        "@appVersion": "1.0.0",
      }),
      "@status": "submit",
    };
    const doc = {
      name: "test",
      description: "test description",
    };
    dataMock.mockReturnValue(doc);
    txnGetFnMock
      .mockResolvedValueOnce({data: ()=> (doc)})
      .mockReturnValueOnce({data: ()=> ({
        description: "test description",
        name: "test",
      })});
    const event = createEvent(form);

    await onFormSubmit(event);

    const expectedAction = {
      "actionType": "create",
      "document": {
        description: "test description",
        name: "test",
      },
      "eventContext": {
        docId: "test-uid",
        docPath: "users/test-uid",
        entity: "user",
        formId: "test-fid",
        id: "test-id",
        uid: "test-uid",
      },
      "modifiedFields": {
        field1: "value1",
        field2: "value2",
      },
      "timeCreated": _mockable.createNowTimestamp(),
      "user": {
        description: "test description",
        name: "test",
      },
      "status": "processed-with-errors",
      "metadata": {},
      "appVersion": "1.0.0",
    };

    // Test that the runBusinessLogics function was called with the correct parameters
    const actionRef = _mockable.initActionRef(event.params.formId);
    expect(transactionSetMock).toHaveBeenNthCalledWith(1, actionRef, expectedAction);

    // form should still finish successfully
    expect(refMock.update).toHaveBeenCalledTimes(3);
    expect(refMock.update.mock.calls[0][0]).toEqual({"@status": "processing"});
    expect(refMock.update.mock.calls[1][0]).toEqual({"@status": "submitted"});
    expect(refMock.update.mock.calls[2][0]).toEqual({"@status": "finished"});

    expect(docMock.update).toHaveBeenCalledTimes(1);
    const logicResultsRef = actionRef.collection("logicResults").doc("undefined-0");
    expect(transactionSetMock).toHaveBeenNthCalledWith(2, logicResultsRef, {
      status: "error",
      message: errorMessage,
      name: "testLogic",
      timeFinished: expect.any(Timestamp),
    });

    validateFormMock.mockReset();
    getFormModifiedFieldsMock.mockReset();
    getSecurityFnMock.mockReset();
    delayFormSubmissionAndCheckIfCancelledMock.mockReset();
    runBusinessLogicsMock.mockReset();
    (docMock.set as jest.Mock).mockReset();
    (docMock.update as jest.Mock).mockReset();
    txnGetFnMock.mockClear();
  });

  it("should execute the sequence of operations correctly", async () => {
    refUpdateMock.mockRestore();
    setMock.mockRestore();
    transactionSetMock.mockRestore();
    transactionUpdateMock.mockRestore();
    txnGetFnMock.mockResolvedValue({
      data: () => ({
        "@dataVersion": "1.0.0",
      }),
    });

    const highPriorityDocs: LogicResultDoc[] = [
      {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1",
        doc: {
          title: "High priority doc for user 1",
        },
        priority: "high",
      }, {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1",
        doc: {
          description: "High priority description for user 1",
        },
        priority: "high",
      }, {
        action: "create" as LogicResultDocAction,
        dstPath: "users/user-2",
        doc: {
          title: "High priority doc for user 2",
        },
        priority: "high",
      },
    ];
    const normalPriorityDocs: LogicResultDoc[] = [
      {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1",
        doc: {
          title: "Normal priority doc for user 1",
        },
        priority: "normal",
      },
    ];
    const lowPriorityDocs: LogicResultDoc[] = [
      {
        action: "create" as LogicResultDocAction,
        dstPath: "users/user-1",
        doc: {
          title: "Low priority doc for user 1",
        },
        priority: "low",
      }, {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-2",
        doc: {
          title: "Low priority doc for user 2",
        },
        priority: "low",
      }, {
        action: "delete" as LogicResultDocAction,
        dstPath: "users/user-3",
        doc: {
          title: "Low priority doc for user 3",
        },
        priority: "low",
      },
    ];
    const additionalNormalPriorityDocs: LogicResultDoc[] = [
      {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1/activities/activity-1",
        doc: {
          title: "Normal priority activity for user 1",
        },
        priority: "normal",
      },
    ];
    const transactionalDocs: LogicResultDoc[] = [
      {
        action: "create" as LogicResultDocAction,
        dstPath: "transactions/transaction-1",
        doc: {
          title: "Transaction doc for user 1",
        },
        priority: "high",
      },
      {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1",
        doc: {
          lastActivity: _mockable.createNowTimestamp(),
        },
        instructions: {
          transactions: "--",
        },
        priority: "normal",
      },
      {
        action: "delete" as LogicResultDocAction,
        dstPath: "messages/message-1",
        doc: {
          title: "Message doc for transaction 1",
        },
        priority: "low",
      },
    ];
    const docPath = "users/user-1";
    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": docPath,
        "@appVersion": "4.0.0",
      }),
      "@status": "submit",
    };

    const event = createEvent(form);

    const transactionalDstPathLogicDocsMap =
      await expandConsolidateAndGroupByDstPath(transactionalDocs);

    const highPriorityDstPathLogicDocsMap =
      await expandConsolidateAndGroupByDstPath(highPriorityDocs);
    const {
      docsByDocPath: highPriorityDocsByDocPath,
      otherDocsByDocPath: highPriorityOtherDocsByDocPath,
    } = groupDocsByTargetDocPath(highPriorityDstPathLogicDocsMap, docPath);

    const normalPriorityDstPathLogicDocsMap =
      await expandConsolidateAndGroupByDstPath([...normalPriorityDocs, ...additionalNormalPriorityDocs]);
    const {
      docsByDocPath: normalPriorityDocsByDocPath,
      otherDocsByDocPath: normalPriorityOtherDocsByDocPath,
    } = groupDocsByTargetDocPath(normalPriorityDstPathLogicDocsMap, docPath);

    const lowPriorityDstPathLogicDocsMap =
      await expandConsolidateAndGroupByDstPath(lowPriorityDocs);
    const {
      docsByDocPath: lowPriorityDocsByDocPath,
      otherDocsByDocPath: lowPriorityOtherDocsByDocPath,
    } = groupDocsByTargetDocPath(lowPriorityDstPathLogicDocsMap, docPath);

    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue({"field1": "value1", "field2": "value2"});
    jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() => Promise.resolve({status: "allowed"}));
    jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);
    jest.spyOn(indexutils, "distributeFnNonTransactional")
      .mockResolvedValueOnce([...highPriorityDstPathLogicDocsMap.values()].flat())
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([...normalPriorityDstPathLogicDocsMap.values()].flat());
    jest.spyOn(indexutils, "distributeLater").mockResolvedValue();
    const queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
    const queueRunPatchLogicsSpy = jest.spyOn(patchLogics, "queueRunPatchLogics").mockResolvedValue();
    jest.spyOn(distribution, "convertInstructionsToDbValues").mockResolvedValue({
      updateData: {
        transactions: FieldValue.increment(-1),
      },
      removeData: {},
    });

    const logicResults: LogicResult[] = [
      {
        name: "testLogic",
        status: "finished",
        timeFinished: _mockable.createNowTimestamp(),
        documents: [...highPriorityDocs, ...normalPriorityDocs, ...lowPriorityDocs],
      }, {
        name: "additionalLogic",
        status: "finished",
        timeFinished: _mockable.createNowTimestamp(),
        documents: [...additionalNormalPriorityDocs],
      }, {
        name: "transactionalLogic",
        status: "finished",
        timeFinished: _mockable.createNowTimestamp(),
        documents: [...transactionalDocs],
        transactional: true,
      },
    ];

    const runBusinessLogicsSpy =
      jest.spyOn(indexutils, "runBusinessLogics").mockImplementation(
        async (txnGet, action) => {
          return {
            status: "done",
            logicResults: logicResults,
          };
        },
      );

    const expandConsolidateAndGroupByDstPathMock =
      jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath")
        .mockResolvedValueOnce(transactionalDstPathLogicDocsMap)
        .mockResolvedValueOnce(highPriorityDstPathLogicDocsMap)
        .mockResolvedValueOnce(normalPriorityDstPathLogicDocsMap)
        .mockResolvedValueOnce(lowPriorityDstPathLogicDocsMap);
    jest.spyOn(indexutils, "groupDocsByTargetDocPath")
      .mockReturnValueOnce({
        docsByDocPath: highPriorityDocsByDocPath,
        otherDocsByDocPath: highPriorityOtherDocsByDocPath,
      })
      .mockReturnValueOnce({
        docsByDocPath: normalPriorityDocsByDocPath,
        otherDocsByDocPath: normalPriorityOtherDocsByDocPath,
      })
      .mockReturnValueOnce({
        docsByDocPath: lowPriorityDocsByDocPath,
        otherDocsByDocPath: lowPriorityOtherDocsByDocPath,
      });
    jest.spyOn(indexutils, "distributeFnNonTransactional");
    jest.spyOn(indexutils, "distributeLater");

    await onFormSubmit(event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsSpy).toHaveBeenCalled();
    expect(refMock.update).toHaveBeenCalledTimes(3);
    expect(refMock.update).toHaveBeenNthCalledWith(1, {"@status": "processing"});
    expect(refMock.update).toHaveBeenNthCalledWith(2, {"@status": "submitted"});
    expect(refMock.update).toHaveBeenNthCalledWith(3, {"@status": "finished"});

    // Test that the functions are called in the correct sequence
    expect(transactionMock.set).toHaveBeenCalledTimes(18);
    expect(transactionMock.update).toHaveBeenCalledTimes(0);

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenNthCalledWith(1, transactionalDocs);
    expect(transactionMock.delete).toHaveBeenCalledTimes(1);

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenNthCalledWith(2, highPriorityDocs);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(1, highPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(1, highPriorityDocsByDocPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(2, highPriorityOtherDocsByDocPath);

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenNthCalledWith(3, [...normalPriorityDocs, ...additionalNormalPriorityDocs]);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(2, normalPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(3, normalPriorityDocsByDocPath);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(1, normalPriorityOtherDocsByDocPath, "4.0.0", "1.0.0");

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenNthCalledWith(4, lowPriorityDocs);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(3, lowPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(2, lowPriorityDocsByDocPath, "4.0.0", "1.0.0",);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(3, lowPriorityOtherDocsByDocPath, "4.0.0", "1.0.0",);

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenCalledTimes(4);
    expect(updateMock).toHaveBeenCalledTimes(1);
    expect(updateMock.mock.calls[0][0]).toEqual({
      execTime: expect.any(Number),
    });

    const distributedLogicResultDocs = [
      ...transactionalDstPathLogicDocsMap.values(),
      ...highPriorityDstPathLogicDocsMap.values(),
      ...normalPriorityDstPathLogicDocsMap.values(),
    ].flat();

    // Should run views update using the target version
    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith(
      "1.0.0",
      distributedLogicResultDocs,
    );

    // Should run patches update using the app version along with the consolidated paths
    expect(queueRunPatchLogicsSpy).toHaveBeenCalledWith(
      "4.0.0",
      "users/user-1",
      "transactions/transaction-1",
      "messages/message-1",
      "users/user-2",
      "users/user-1/activities/activity-1",
    );
  });
});

describe("onUserRegister", () => {
  const user = {
    uid: "userId",
    displayName: "John Doe",
    photoURL: "https://example.com/photo.jpg",
    email: "john@example.com",
    providerData: [
      {
        displayName: "John Doe",
        photoURL: "https://example.com/provider-photo.jpg",
        email: "provider@example.com",
      },
    ],
  } as unknown as UserRecord;


  let runTransactionSpy: jest.SpyInstance;
  let distributeFnTransactionalSpy: jest.SpyInstance;

  const customUserRegisterLogicResult: LogicResult = {
    status: "finished",
    name: "customUserRegisterLogic",
    documents: [{
      action: "create",
      dstPath: `users/${user.uid}`,
      doc: {newField: "newValue"},
    }],
  };

  const createMetricExecutionSpy =
    jest.spyOn(indexutils._mockable, "saveMetricExecution");
  const transactionSetMock = jest.fn();
  const mockTxn = {
    set: transactionSetMock,
    get: jest.fn().mockResolvedValue({}),
  } as unknown as Transaction;

  beforeEach(() => {
    admin.initializeApp({databaseURL: "https://test-project.firebaseio.com"});
    jest.spyOn(indexutils._mockable, "saveMetricExecution").mockResolvedValue();
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("should run the onUserRegister correctly", async () => {
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, [], [], [], []);
    runTransactionSpy = jest.spyOn(db, "runTransaction")
      .mockImplementationOnce(async (callback: any) => callback(mockTxn));
    jest.spyOn(_mockable, "createNowTimestamp").mockReturnValue(Timestamp.now());
    distributeFnTransactionalSpy = jest.spyOn(indexutils, "distributeFnTransactional")
      .mockResolvedValueOnce([
        {
          "action": "create",
          "dstPath": "users/userId",
          "doc": {
            "newField": "newValue",
            "@id": user["uid"],
            "avatarUrl": user["photoURL"],
            "username": user["displayName"],
            "email": user["email"],
            "registeredAt": Timestamp.now(),
          },
        },
      ]);

    await onUserRegister(user);

    expect(runTransactionSpy).toHaveBeenCalledTimes(1);
    expect(transactionSetMock).toHaveBeenCalledTimes(1);
    expect(createMetricExecutionSpy).toHaveBeenNthCalledWith(1, [
      {name: "onUserRegister", execTime: expect.any(Number)},
    ]);
  });

  it("should run the onUserRegister along with the customUserRegisterFn", async () => {
    const customUserRegisterLogicFn = jest.fn().mockResolvedValue(customUserRegisterLogicResult);
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, [], [], [], [], customUserRegisterLogicFn);
    runTransactionSpy = jest.spyOn(db, "runTransaction")
      .mockImplementationOnce(async (callback: any) => callback(mockTxn));
    jest.spyOn(_mockable, "createNowTimestamp").mockReturnValue(Timestamp.now());
    distributeFnTransactionalSpy = jest.spyOn(indexutils, "distributeFnTransactional")
      .mockResolvedValueOnce([
        {
          "action": "create",
          "dstPath": "users/userId",
          "doc": {
            "newField": "newValue",
            "@id": user["uid"],
            "avatarUrl": user["photoURL"],
            "username": user["displayName"],
            "email": user["email"],
            "registeredAt": Timestamp.now(),
          },
        },
      ]);
    await onUserRegister(user);

    expect(runTransactionSpy).toHaveBeenCalledTimes(1);
    expect(transactionSetMock).toHaveBeenCalledTimes(1);
    expect(customUserRegisterLogicFn).toHaveBeenCalled();
    expect(distributeFnTransactionalSpy).toHaveBeenNthCalledWith(1, mockTxn, [customUserRegisterLogicResult]);
    expect(createMetricExecutionSpy).toHaveBeenNthCalledWith(1, [
      {name: "onUserRegister", execTime: expect.any(Number)},
      {name: "customUserRegisterLogic", execTime: expect.any(Number)},
    ]);
  });
});
