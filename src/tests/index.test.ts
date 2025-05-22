import {_mockable, db, initializeEmberFlow, onFormSubmit} from "../index";
import * as indexutils from "../index-utils";
import * as transactionutils from "../utils/transaction";
import * as admin from "firebase-admin";
import * as viewLogics from "../logics/view-logics";
import {database, firestore} from "firebase-admin";

import {
  LogicResult,
  LogicResultDocAction,
  LogicResultDoc,
  ProjectConfig,
  SecurityResult,
  ValidateFormResult, JournalEntry, RunBusinessLogicStatus,
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
import * as indexUtils from "../index-utils";
import * as distribution from "../utils/distribution";

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

initializeEmberFlow(projectConfig, admin, dbStructure, Entity, {}, {}, []);

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
  const equation = "totalTodos = notStartedCount + inProgressCount + toVerifyCount + requestChangesCount + doneCount";
  const now = Timestamp.fromDate(new Date());
  const journalEntry: JournalEntry = {
    date: now,
    ledgerEntries: [
      {
        account: "totalTodos",
        debit: 1,
        credit: 0,
      },
      {
        account: "notStartedCount",
        debit: 0,
        credit: 1,
      },
    ],
    equation: equation,
  };
  const recordedJournalEntry: JournalEntry = {
    date: now,
    ledgerEntries: [
      {
        account: "inProgressCount",
        debit: 1,
        credit: 0,
      },
      {
        account: "doneCount",
        debit: 0,
        credit: 1,
      },
    ],
    equation: equation,
    recordEntry: true,
  };

  beforeEach(() => {
    jest.spyOn(indexUtils._mockable, "createMetricExecution").mockResolvedValue();
    jest.spyOn(_mockable, "createNowTimestamp").mockReturnValue(Timestamp.now());
    jest.spyOn(indexutils._mockable, "createMetricExecution").mockResolvedValue();
    jest.spyOn(console, "log").mockImplementation();
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

    expect(getSecurityFnMock).toHaveBeenCalledWith(entity);
    expect(validateFormMock).toHaveBeenCalledWith(entity, formData);
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
      status: "processed-with-errors",
    };

    // Test that the runBusinessLogics function was called with the correct parameters
    const actionRef = _mockable.initActionRef(event.params.formId);
    expect(transactionSetMock).toHaveBeenNthCalledWith(4, actionRef, expectedAction);

    // form should still finish successfully
    expect(refMock.update).toHaveBeenCalledTimes(3);
    expect(refMock.update.mock.calls[0][0]).toEqual({"@status": "processing"});
    expect(refMock.update.mock.calls[1][0]).toEqual({"@status": "submitted"});
    expect(refMock.update.mock.calls[2][0]).toEqual({"@status": "finished"});

    expect(docMock.update).toHaveBeenCalledTimes(1);
    const logicResultsRef = actionRef.collection("logicResults").doc("undefined-0");
    expect(transactionSetMock).toHaveBeenNthCalledWith(5, logicResultsRef, {
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

  it("should write journal entries first", async () => {
    transactionSetMock.mockReset();
    const consoleErrorSpy = jest.spyOn(console, "error").mockImplementation();
    const consoleInfoSpy = jest.spyOn(console, "info").mockImplementation();
    const queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
    jest.spyOn(indexutils, "runBusinessLogics").mockImplementation(
      async (txnGet, action) => {
        return {
          status: "done",
          logicResults,
        };
      },
    );
    const distributionMock = jest.spyOn(distribution, "queueForDistributionLater").mockImplementation(async ()=>{
      console.log("distribute");
    });
    let logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1"},
        ],
      },
    ];
    const docPath = "users/user-1";
    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": docPath,
      }),
      "@status": "submit",
    };
    const event = createEvent(form);
    await onFormSubmit(event);
    expect(consoleInfoSpy).toHaveBeenCalledWith("No journal entries to write");
    expect(transactionUpdateMock).not.toHaveBeenCalled();
    expect(txnGetFnMock).toBeCalledTimes(2);

    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "delete", priority: "normal", dstPath: "path1/doc1", journalEntries: [journalEntry]},
        ],
      },
    ];
    await onFormSubmit(event);
    expect(consoleInfoSpy).toHaveBeenCalledWith("No journal entries to write");
    expect(transactionUpdateMock).not.toHaveBeenCalled();
    expect(transactionSetMock).toHaveBeenCalledTimes(6);
    expect(txnGetFnMock).toBeCalledTimes(4);
    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "", journalEntries: [journalEntry]},
        ],
      },
    ];
    await onFormSubmit(event);
    expect(consoleErrorSpy).toHaveBeenCalledWith("Dst path has no docId");
    expect(transactionUpdateMock).not.toHaveBeenCalled();
    expect(transactionSetMock).toHaveBeenCalledTimes(9);
    expect(txnGetFnMock).toHaveBeenCalledTimes(6);

    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "high", dstPath: "path1/doc5", doc: {totalTodos: 1}, journalEntries: [journalEntry]},
        ],
      },
    ];
    await onFormSubmit(event);
    expect(consoleErrorSpy).toHaveBeenCalledWith("Doc cannot have keys that are the same as account names");
    expect(transactionUpdateMock).not.toHaveBeenCalled();
    expect(transactionSetMock).toHaveBeenCalledTimes(12);
    expect(txnGetFnMock).toHaveBeenCalledTimes(8);

    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "high", dstPath: "path1/doc5", instructions: {totalTodos: "+1"}, journalEntries: [journalEntry]},
        ],
      },
    ];
    await onFormSubmit(event);
    expect(consoleErrorSpy).toHaveBeenCalledWith("Instructions cannot have keys that are the same as account names");
    expect(transactionUpdateMock).not.toHaveBeenCalled();
    expect(transactionSetMock).toHaveBeenCalledTimes(15);
    expect(txnGetFnMock).toHaveBeenCalledTimes(10);

    const unbalancedJournalEntry: JournalEntry = {
      date: _mockable.createNowTimestamp(),
      ledgerEntries: [
        {
          account: "totalTodos",
          debit: 1,
          credit: 0,
        },
        {
          account: "notStartedCount",
          debit: 0,
          credit: 1,
        },
        {
          account: "inProgressCount",
          debit: 0,
          credit: 1,
        },
      ],
      equation: equation,
    };
    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "high", dstPath: "path1/doc5", journalEntries: [unbalancedJournalEntry]},
        ],
      },
    ];
    await onFormSubmit(event);
    expect(consoleErrorSpy).toHaveBeenCalledWith("Debit and credit should be equal");
    expect(transactionUpdateMock).not.toHaveBeenCalled();
    expect(transactionSetMock).toHaveBeenCalledTimes(18);
    expect(txnGetFnMock).toHaveBeenCalledTimes(12);

    txnGetFnMock.mockRestore();
    transactionSetMock.mockRestore();
    transactionUpdateMock.mockRestore();
    queueRunViewLogicsSpy.mockReset();
    const docRef = db.doc("path1/doc1");
    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field: "value"}, journalEntries: [journalEntry]},
        ],
      },
    ];

    const user = {
      "@id": "forDistribution",
      "username": "forDistribution",
    };
    txnGetFnMock
      .mockResolvedValueOnce({data: ()=> (document)})
      .mockResolvedValueOnce({data: ()=> (user)})
      .mockReturnValueOnce({data: () => undefined});

    await onFormSubmit(event);
    expect(txnGetFnMock).toHaveBeenCalledTimes(3);
    expect(txnGetFnMock).toHaveBeenCalledWith(docRef);
    expect(transactionSetMock).toHaveBeenCalledTimes(4);
    expect(transactionSetMock).toHaveBeenCalledWith(docRef, {"field": "value", "@forDeletionLater": true});
    expect(transactionUpdateMock).toHaveBeenCalledTimes(2);
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(1, docRef, {
      "totalTodos": 1,
      "@forDeletionLater": FieldValue.delete(),
    });
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(2, docRef, {
      "notStartedCount": 1,
      "@forDeletionLater": FieldValue.delete(),
    });
    expect(queueRunViewLogicsSpy).toHaveBeenCalledTimes(1);
    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith(logicResults);

    queueRunViewLogicsSpy.mockReset();
    txnGetFnMock.mockRestore();
    transactionSetMock.mockRestore();
    transactionUpdateMock.mockRestore();
    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field: "value"}, journalEntries: [journalEntry]},
        ],
      },
    ];
    txnGetFnMock
      .mockResolvedValueOnce({data: ()=> (document)})
      .mockResolvedValueOnce({data: ()=> (user)})
      .mockResolvedValueOnce({
        data: () => ({
          "totalTodos": 1,
          "notStartedCount": 1,
        }),
      });
    await onFormSubmit(event);
    expect(txnGetFnMock).toHaveBeenCalledTimes(3);
    expect(txnGetFnMock).toHaveBeenCalledWith(docRef);
    expect(transactionSetMock).toHaveBeenCalledTimes(3);
    expect(transactionUpdateMock).toHaveBeenCalledTimes(3);
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(1, docRef, {"field": "value", "@forDeletionLater": true});
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(2, docRef, {
      "totalTodos": 2,
      "@forDeletionLater": FieldValue.delete(),
    });
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(3, docRef, {
      "notStartedCount": 2,
      "@forDeletionLater": FieldValue.delete(),
    });
    expect(queueRunViewLogicsSpy).toHaveBeenCalledTimes(1);
    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith(logicResults);

    queueRunViewLogicsSpy.mockReset();
    txnGetFnMock.mockRestore();
    transactionSetMock.mockRestore();
    transactionUpdateMock.mockRestore();
    txnGetFnMock
      .mockResolvedValueOnce({data: ()=> (document)})
      .mockResolvedValueOnce({data: ()=> (user)})
      .mockResolvedValueOnce({
        data: () => ({
          "totalTodos": 2,
          "notStartedCount": 0,
          "inProgressCount": 2,
        }),
      });
    const zeroJournalEntry: JournalEntry = {
      date: _mockable.createNowTimestamp(),
      ledgerEntries: [
        {
          account: "notStartedCount",
          debit: 1,
          credit: 1,
        },
        {
          account: "inProgressCount",
          debit: 1,
          credit: 1,
        },
      ],
      equation: equation,
    };
    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", journalEntries: [zeroJournalEntry]},
        ],
      },
    ];
    await onFormSubmit(event);
    expect(txnGetFnMock).toHaveBeenCalledTimes(3);
    expect(txnGetFnMock).toHaveBeenCalledWith(docRef);
    expect(transactionSetMock).toHaveBeenCalledTimes(3);
    expect(transactionUpdateMock).toHaveBeenCalledTimes(3);
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(1, docRef, {"@forDeletionLater": true});
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(2, docRef, {"@forDeletionLater": FieldValue.delete()});
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(3, docRef, {"@forDeletionLater": FieldValue.delete()});

    distributionMock.mockRestore();
    const errorMock = jest.spyOn(global, "Error").mockImplementation();
    txnGetFnMock.mockRestore();
    transactionSetMock.mockRestore();
    transactionUpdateMock.mockRestore();

    txnGetFnMock
      .mockResolvedValueOnce({data: ()=> (document)})
      .mockResolvedValueOnce({data: ()=> (user)})
      .mockResolvedValueOnce({
        data: () => ({
          "totalTodos": 2,
          "notStartedCount": 0,
          "inProgressCount": 2,
        }),
      });

    const changeStatusJournalEntry: JournalEntry = {
      date: _mockable.createNowTimestamp(),
      ledgerEntries: [
        {
          account: "notStartedCount",
          debit: 1,
          credit: 0,
        },
        {
          account: "inProgressCount",
          debit: 0,
          credit: 1,
        },
      ],
      equation: equation,
    };
    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", journalEntries: [changeStatusJournalEntry]},
        ],
      },
    ];
    try {
      await onFormSubmit(event);
    } catch (e) {
      console.log("Expect this error");
    }
    expect(txnGetFnMock).toHaveBeenCalledTimes(3);
    expect(txnGetFnMock).toHaveBeenCalledWith(docRef);
    expect(transactionSetMock).toHaveBeenCalledTimes(3);
    expect(transactionUpdateMock).toHaveBeenCalledTimes(1);
    expect(transactionUpdateMock).toHaveBeenCalledWith(docRef, {"@forDeletionLater": true});
    expect(errorMock).toHaveBeenCalledTimes(1);
    expect(errorMock).toHaveBeenCalledWith("Account value cannot be negative");
    queueRunViewLogicsSpy.mockReset();
    errorMock.mockRestore();
    txnGetFnMock.mockRestore();
    transactionSetMock.mockRestore();
    transactionUpdateMock.mockRestore();
    txnGetFnMock
      .mockResolvedValueOnce({data: ()=> (document)})
      .mockResolvedValueOnce({data: ()=> (user)})
      .mockResolvedValueOnce({
        data: () => ({
          "totalTodos": 2,
          "notStartedCount": 1,
          "inProgressCount": 1,
          "doneCount": 0,
        }),
      });

    logicResults = [
      {
        name: "logic 1",
        timeFinished: _mockable.createNowTimestamp(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path1/doc1", journalEntries: [recordedJournalEntry]},
        ],
      },
    ];
    await onFormSubmit(event);
    expect(txnGetFnMock).toHaveBeenCalledTimes(3);
    expect(txnGetFnMock).toHaveBeenCalledWith(docRef);
    expect(transactionUpdateMock).toHaveBeenCalledTimes(3);
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(1, docRef, {"@forDeletionLater": true});
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(2, docRef, {
      "inProgressCount": 0,
      "@forDeletionLater": FieldValue.delete(),
    });
    expect(transactionUpdateMock).toHaveBeenNthCalledWith(3, docRef, {
      "doneCount": 1,
      "@forDeletionLater": FieldValue.delete(),
    });
    expect(transactionSetMock).toHaveBeenCalledTimes(5);
    expect(transactionSetMock).toHaveBeenNthCalledWith(4, db.doc("path/doc1/@ledgers/doc100"), {
      "journalEntryId": "doc10",
      "account": "inProgressCount",
      "debit": 1,
      "credit": 0,
      "date": expect.any(Timestamp),
      "equation": equation,
    });
    expect(transactionSetMock).toHaveBeenNthCalledWith(5, db.doc("path/doc1/@ledgers/doc101"), {
      "journalEntryId": "doc10",
      "account": "doneCount",
      "debit": 0,
      "credit": 1,
      "date": expect.any(Timestamp),
      "equation": equation,
    });
    expect(queueRunViewLogicsSpy).toHaveBeenCalledTimes(1);
    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith(logicResults);
  }, 15000);

  it("should execute the sequence of operations correctly", async () => {
    refUpdateMock.mockRestore();
    setMock.mockRestore();
    transactionSetMock.mockRestore();
    transactionUpdateMock.mockRestore();
    jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
    jest.spyOn(indexutils, "getFormModifiedFields").mockReturnValue({"field1": "value1", "field2": "value2"});
    jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() => Promise.resolve({status: "allowed"}));
    jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(false);
    jest.spyOn(indexutils, "distributeFnNonTransactional").mockResolvedValue();
    jest.spyOn(indexutils, "distributeLater").mockResolvedValue();
    jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
    jest.spyOn(distribution, "convertInstructionsToDbValues").mockResolvedValue({
      updateData: {
        transactions: FieldValue.increment(-1),
      },
      removeData: {},
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

    const recordedJournalEntry: JournalEntry = {
      date: _mockable.createNowTimestamp(),
      ledgerEntries: [
        {
          account: "inProgressCount",
          debit: 1,
          credit: 0,
        },
        {
          account: "doneCount",
          debit: 0,
          credit: 1,
        },
      ],
      equation: equation,
      recordEntry: true,
    };
    const journalDocs: LogicResultDoc[] = [
      {
        action: "merge",
        priority: "normal",
        dstPath: "journal/doc1",
        journalEntries: [recordedJournalEntry, journalEntry],
      },
      {
        action: "merge",
        priority: "normal",
        dstPath: "journal/doc2",
        journalEntries: [journalEntry],
      },
    ];

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
      }, {
        name: "journalLogic",
        status: "finished",
        timeFinished: _mockable.createNowTimestamp(),
        documents: [...journalDocs],
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

    const docPath = "users/user-1";
    const form = {
      "formData": JSON.stringify({
        "@actionType": "create",
        "name": "test",
        "@docPath": docPath,
      }),
      "@status": "submit",
    };

    const event = createEvent(form);

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

    const transactionalDstPathLogicDocsMap =
      await expandConsolidateAndGroupByDstPath(transactionalDocs);

    const journalDstPathLogicDocsMap =
      await expandConsolidateAndGroupByDstPath(journalDocs);

    const viewLogicResults: LogicResult[] = [{
      name: "User ViewLogic",
      status: "finished",
      timeFinished: _mockable.createNowTimestamp(),
      documents: logicResults.map((result) => result.documents).flat(),
    }];

    jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath")
      .mockResolvedValueOnce(journalDstPathLogicDocsMap)
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
    jest.spyOn(indexutils, "runViewLogics").mockResolvedValue(viewLogicResults);
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
    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(1, journalDocs);
    console.debug("set", transactionSetMock.mock.calls);
    console.debug("update", transactionUpdateMock.mock.calls);
    expect(transactionMock.set).toHaveBeenCalledTimes(23);
    expect(transactionMock.update).toHaveBeenCalledTimes(9);

    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(2, transactionalDocs);
    expect(transactionMock.delete).toHaveBeenCalledTimes(1);

    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(3, highPriorityDocs);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(1, highPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(1, highPriorityDocsByDocPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(2, highPriorityOtherDocsByDocPath);

    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(4, [...normalPriorityDocs, ...additionalNormalPriorityDocs]);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(2, normalPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(3, normalPriorityDocsByDocPath);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(1, normalPriorityOtherDocsByDocPath);

    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenNthCalledWith(5, lowPriorityDocs);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(3, lowPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(2, lowPriorityDocsByDocPath);
    expect(indexutils.distributeLater).toHaveBeenNthCalledWith(3, lowPriorityOtherDocsByDocPath);

    expect(indexutils.expandConsolidateAndGroupByDstPath).toHaveBeenCalledTimes(5);
    expect(updateMock).toHaveBeenCalledTimes(1);
    expect(updateMock.mock.calls[0][0]).toEqual({
      execTime: expect.any(Number),
    });
  });
});
