import {
  _mockable,
  db,
  hourlyLogicConfigs,
  initializeEmberFlow,
  onFormSubmit,
  onUserRegister,
  projectConfig as globalProjectConfig,
} from "../index";
import * as indexutils from "../index-utils";
import * as transactionutils from "../utils/transaction";
import * as admin from "firebase-admin";
import * as viewLogics from "../logics/view-logics";
import * as patchLogics from "../logics/patch-logics";
import {database, firestore} from "firebase-admin";

import {findMatchingViewLogics} from "../logics/view-logics";
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
import type {DatabaseEvent, DataSnapshot} from "firebase-functions/v2/database";
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

initializeEmberFlow({
  projectConfig,
  admin,
  dbStructure,
  Entity,
  securityConfigs: [],
  validatorConfigs: [],
  logicConfigs: [],
  patchLogicConfigs: [],
});

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
      "@appVersion": "4.0.0",
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
      .mockResolvedValueOnce({data: ()=> ({})})
      .mockReturnValueOnce({data: ()=> (user)});

    const validateFormMock = jest.spyOn(indexutils, "validateForm");
    validateFormMock.mockResolvedValue([false, {}] as ValidateFormResult);

    const extractedTxnGet = {
      get: txnGetFnMock,
    };
    jest.spyOn(transactionutils, "extractTransactionGetOnly").mockReturnValue( extractedTxnGet);
    await onFormSubmit(event);

    expect(getSecurityFnMock).toHaveBeenCalledWith(entity, "4.0.0");
    expect(validateFormMock).toHaveBeenCalledWith(entity, formData, "4.0.0");
    expect(securityFnMock).toHaveBeenCalledWith(extractedTxnGet, entity, docPath, {}, "update", {field1: "newValue", field2: "oldValue"}, user);
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
    expect(transactionSetMock).toHaveBeenNthCalledWith(1, actionRef, expectedAction, {merge: true});

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
      documentsCount: 0,
      dstPaths: [],
    }, {merge: true});

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
            logicResults,
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
    const findMatchingPatchLogicsByEntitySpy = jest.spyOn(patchLogics, "findMatchingPatchLogicsByEntity")
      .mockReturnValue([{} as any]);
    const findMatchingDocPathRegexSpy = jest.spyOn(paths, "findMatchingDocPathRegex")
      .mockReturnValue({entity: "dummyEntity", regex: /dummy/});

    await onFormSubmit(event);

    // Test that the runBusinessLogics function was called with the correct parameters
    expect(runBusinessLogicsSpy).toHaveBeenCalledWith(expect.anything(), expect.anything(), "4.0.0");
    expect(refMock.update).toHaveBeenCalledTimes(3);
    expect(refMock.update).toHaveBeenNthCalledWith(1, {"@status": "processing"});
    expect(refMock.update).toHaveBeenNthCalledWith(2, {"@status": "submitted"});
    expect(refMock.update).toHaveBeenNthCalledWith(3, {"@status": "finished"});

    // Test that the functions are called in the correct sequence
    expect(transactionMock.set).toHaveBeenCalledTimes(9);
    expect(transactionMock.update).toHaveBeenCalledTimes(0);

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenNthCalledWith(1, transactionalDocs);
    expect(transactionMock.delete).toHaveBeenCalledTimes(1);

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenNthCalledWith(2, highPriorityDocs);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(1, highPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(1, highPriorityDocsByDocPath, "4.0.0");
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(2, highPriorityOtherDocsByDocPath, "4.0.0");

    expect(expandConsolidateAndGroupByDstPathMock).toHaveBeenNthCalledWith(3, [...normalPriorityDocs, ...additionalNormalPriorityDocs]);
    expect(indexutils.groupDocsByTargetDocPath).toHaveBeenNthCalledWith(2, normalPriorityDstPathLogicDocsMap, docPath);
    expect(indexutils.distributeFnNonTransactional).toHaveBeenNthCalledWith(3, normalPriorityDocsByDocPath, "4.0.0");
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
    ].flat().filter((doc) => findMatchingViewLogics(doc, "1.0.0")?.size);

    // Should run views update using the target version
    expect(queueRunViewLogicsSpy).toHaveBeenCalledWith(
      "1.0.0",
      "4.0.0",
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
    findMatchingPatchLogicsByEntitySpy.mockRestore();
    findMatchingDocPathRegexSpy.mockRestore();
  });

  it("should distribute submit-form logic docs after transaction", async () => {
    const docPath = "users/user-1";
    const form = {
      "formData": JSON.stringify({
        "@actionType": "update",
        "@docPath": docPath,
        "@appVersion": "4.0.0",
      }),
      "@status": "submit",
    };
    const event = createEvent(form);

    const submitFormDoc: LogicResultDoc = {
      action: "submit-form",
      dstPath: "users/user-1/forms/test-form",
      doc: {some: "data"},
      priority: "normal",
    };

    const logicResults: LogicResult[] = [
      {
        name: "testLogic",
        status: "finished",
        timeFinished: _mockable.createNowTimestamp(),
        documents: [submitFormDoc],
      },
    ];

    jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({
      status: "done",
      logicResults,
    });
    jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(new Map());
    jest.spyOn(indexutils, "distributeFnTransactional").mockResolvedValue([submitFormDoc]);
    const distributeDocSpy = jest.spyOn(indexutils, "distributeDoc").mockResolvedValue();
    jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({entity: "user", regex: /users\/.*/});

    await onFormSubmit(event);

    expect(distributeDocSpy).toHaveBeenCalledWith(submitFormDoc, "4.0.0");
  });

  describe("logicResultDocuments option and saveLogicResults", () => {
    const docPath = "users/user-1";
    const form = {
      "formData": JSON.stringify({
        "@actionType": "update",
        "@docPath": docPath,
        "@appVersion": "4.0.0",
      }),
      "@status": "submit",
    };

    beforeEach(() => {
      jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({entity: "user", regex: /users\/.*/});
      jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(new Map());
      jest.spyOn(indexutils, "distributeFnTransactional").mockResolvedValue([]);
      jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
      jest.spyOn(patchLogics, "queueRunPatchLogics").mockResolvedValue();
      jest.spyOn(_mockable, "initActionRef").mockImplementation((actionId: string) => {
        const actionPath = `@actions/${actionId}`;
        return {
          id: actionId,
          path: actionPath,
          update: jest.fn(),
          collection: (col: string) => ({
            doc: (docId: string) => {
              const lrPath = `${actionPath}/${col}/${docId}`;
              return {
                id: docId,
                path: lrPath,
                collection: (subCol: string) => ({
                  doc: (subDocId: string) => ({
                    id: subDocId,
                    path: `${lrPath}/${subCol}/${subDocId}`,
                  }),
                }),
              };
            },
          }),
        } as unknown as DocumentReference;
      });
    });

    afterEach(() => {
      globalProjectConfig.logicResultDocuments = undefined;
    });

    it("should copy all documents and write documentsCount and dstPaths (first 50) by default ('all')", async () => {
      globalProjectConfig.logicResultDocuments = undefined; // default is 'all'
      transactionSetMock.mockClear();

      const docsForLogic1: LogicResultDoc[] = Array.from({length: 60}, (_, i) => ({
        action: "merge" as LogicResultDocAction,
        dstPath: `users/user-1/items/item-${i}`,
        doc: {val: i},
      }));

      const logicResults: LogicResult[] = [
        {
          name: "logic1",
          status: "finished",
          timeFinished: _mockable.createNowTimestamp(),
          documents: docsForLogic1,
        },
      ];

      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({
        status: "done",
        logicResults,
      });

      const event = createEvent(form);
      await onFormSubmit(event);

      // Call 1 is actionRef
      // Call 2 is logicResult summary doc
      // Calls 3..62 are the 60 documents
      expect(transactionSetMock).toHaveBeenCalledTimes(1 + 1 + 60);

      const summaryCall = transactionSetMock.mock.calls[1];
      expect(summaryCall[1]).toEqual(expect.objectContaining({
        name: "logic1",
        status: "finished",
        documentsCount: 60,
        dstPaths: docsForLogic1.slice(0, 50).map((d) => d.dstPath),
      }));
      expect(summaryCall[1].dstPaths).toHaveLength(50);

      // Verify that individual documents were set and target the correct path
      const docCall = transactionSetMock.mock.calls[2];
      expect(docCall[0].path).toMatch(/\/documents\/.*-0-0$/);
      expect(docCall[0].path).toBe("@actions/test-fid/logicResults/test-fid-0/documents/test-fid-0-0");
      expect(docCall[1]).toEqual(docsForLogic1[0]);
    });

    it("should not copy documents for finished results in 'errors-only' mode when action succeeds", async () => {
      globalProjectConfig.logicResultDocuments = "errors-only";
      transactionSetMock.mockClear();

      const doc1: LogicResultDoc = {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1/items/item-1",
        doc: {val: 1},
      };

      const logicResults: LogicResult[] = [
        {
          name: "finishedLogic",
          status: "finished",
          timeFinished: _mockable.createNowTimestamp(),
          documents: [doc1],
        },
      ];

      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({
        status: "done",
        logicResults,
      });

      const event = createEvent(form);
      await onFormSubmit(event);

      // Call 1 is actionRef
      // Call 2 is logicResult summary doc
      // No documents collection set calls!
      expect(transactionSetMock).toHaveBeenCalledTimes(2);

      const summaryCall = transactionSetMock.mock.calls[1];
      expect(summaryCall[1]).toEqual(expect.objectContaining({
        name: "finishedLogic",
        status: "finished",
        documentsCount: 1,
        dstPaths: ["users/user-1/items/item-1"],
      }));
    });

    it("should copy documents in 'errors-only' mode when action ends processed-with-errors", async () => {
      globalProjectConfig.logicResultDocuments = "errors-only";
      transactionSetMock.mockClear();

      const docFinished: LogicResultDoc = {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1/items/item-finished",
        doc: {val: "finished"},
      };
      const docError: LogicResultDoc = {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1/items/item-error",
        doc: {val: "error"},
      };

      const logicResults: LogicResult[] = [
        {
          name: "finishedLogic",
          status: "finished",
          timeFinished: _mockable.createNowTimestamp(),
          documents: [docFinished],
        },
        {
          name: "errorLogic",
          status: "error",
          message: "something broke",
          timeFinished: _mockable.createNowTimestamp(),
          documents: [docError],
        },
      ];

      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({
        status: "done",
        logicResults,
      });

      const event = createEvent(form);
      await onFormSubmit(event);

      // Call 1 is actionRef (action.status = 'processed-with-errors')
      // Call 2 is finishedLogic summary doc
      // Call 3 is docFinished (copied because action ended processed-with-errors)
      // Call 4 is errorLogic summary doc
      // Call 5 is docError (copied)
      expect(transactionSetMock).toHaveBeenCalledTimes(1 + 2 + 2);

      const finishedSummary = transactionSetMock.mock.calls[1];
      expect(finishedSummary[1]).toEqual(expect.objectContaining({
        name: "finishedLogic",
        documentsCount: 1,
        dstPaths: ["users/user-1/items/item-finished"],
      }));

      const errorSummary = transactionSetMock.mock.calls[3];
      expect(errorSummary[1]).toEqual(expect.objectContaining({
        name: "errorLogic",
        documentsCount: 1,
        dstPaths: ["users/user-1/items/item-error"],
      }));

      // Verify that individual documents were set and target the correct path
      expect(transactionSetMock.mock.calls[2][0].path).toMatch(/\/documents\/.*-0-0$/);
      expect(transactionSetMock.mock.calls[2][0].path).toBe("@actions/test-fid/logicResults/test-fid-0/documents/test-fid-0-0");
    });

    it("should never copy documents in 'none' mode even with error results", async () => {
      globalProjectConfig.logicResultDocuments = "none";
      transactionSetMock.mockClear();

      const docError: LogicResultDoc = {
        action: "merge" as LogicResultDocAction,
        dstPath: "users/user-1/items/item-error",
        doc: {val: "error"},
      };

      const logicResults: LogicResult[] = [
        {
          name: "errorLogic",
          status: "error",
          message: "failed",
          timeFinished: _mockable.createNowTimestamp(),
          documents: [docError],
        },
      ];

      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({
        status: "done",
        logicResults,
      });

      const event = createEvent(form);
      await onFormSubmit(event);

      // Call 1 is actionRef
      // Call 2 is logicResult summary doc
      // No document copy calls!
      expect(transactionSetMock).toHaveBeenCalledTimes(2);

      const summaryCall = transactionSetMock.mock.calls[1];
      expect(summaryCall[1]).toEqual(expect.objectContaining({
        name: "errorLogic",
        documentsCount: 1,
        dstPaths: ["users/user-1/items/item-error"],
      }));
    });

    it("should preserve unchanged post-commit order: formRef.update(finished) -> queueRunViewLogics -> distributeDoc (submit-form) -> queueRunPatchLogics", async () => {
      const callOrder: string[] = [];

      refUpdateMock.mockImplementation((data: Record<string, unknown>) => {
        if (data && data["@status"] === "finished") {
          callOrder.push("formFinished");
        }
        return Promise.resolve();
      });

      const queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockImplementation(async () => {
        callOrder.push("queueRunViewLogics");
      });

      const submitFormDoc: LogicResultDoc = {
        action: "submit-form",
        dstPath: "users/user-1/forms/test-form",
        doc: {some: "data"},
      };

      const distributeDocSpy = jest.spyOn(indexutils, "distributeDoc").mockImplementation(async () => {
        callOrder.push("distributeSubmitFormDoc");
      });

      const queueRunPatchLogicsSpy = jest.spyOn(patchLogics, "queueRunPatchLogics").mockImplementation(async () => {
        callOrder.push("queueRunPatchLogics");
      });

      jest.spyOn(indexutils, "distributeFnTransactional").mockResolvedValue([submitFormDoc]);

      const logicResults: LogicResult[] = [
        {
          name: "testLogic",
          status: "finished",
          timeFinished: _mockable.createNowTimestamp(),
          documents: [submitFormDoc],
        },
      ];

      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({
        status: "done",
        logicResults,
      });

      const event = createEvent(form);
      await onFormSubmit(event);

      expect(callOrder).toEqual([
        "formFinished",
        "queueRunViewLogics",
        "distributeSubmitFormDoc",
        "queueRunPatchLogics",
      ]);

      queueRunViewLogicsSpy.mockRestore();
      distributeDocSpy.mockRestore();
      queueRunPatchLogicsSpy.mockRestore();
    });
  });

  describe("logicErrorFormStatus option", () => {
    const docPath = "users/user-1";
    const form = {
      "formData": JSON.stringify({
        "@actionType": "update",
        "@docPath": docPath,
        "@appVersion": "4.0.0",
      }),
      "@status": "submit",
    };
    const retryWords = /contention|aborted/i;
    const okDoc: LogicResultDoc = {
      action: "merge" as LogicResultDocAction,
      dstPath: "users/user-1/items/item-ok",
      doc: {val: "ok"},
    };

    const okResult = (): LogicResult => ({
      name: "okLogic",
      status: "finished",
      timeFinished: _mockable.createNowTimestamp(),
      documents: [okDoc],
    });
    const thrownResult = (message = "Cannot read properties of undefined (reading 'menuItems')"): LogicResult => ({
      name: "thrownLogic",
      status: "error",
      message,
      timeFinished: _mockable.createNowTimestamp(),
      documents: [],
      thrown: true,
    });
    const returnedErrorResult = (): LogicResult => ({
      name: "returnedErrorLogic",
      status: "error",
      message: "Topic not found",
      timeFinished: _mockable.createNowTimestamp(),
      documents: [],
    });

    const formStatusUpdates = () => refUpdateMock.mock.calls
      .map((call) => call[0])
      .filter((data) => data && ["finished", "error"].includes(data["@status"]));

    const runWith = async (logicResults: LogicResult[], status: RunBusinessLogicStatus["status"] = "done") => {
      jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({status, logicResults});
      await onFormSubmit(createEvent(form));
    };

    let expandSpy: jest.SpyInstance;
    let distributeFnTransactionalSpy: jest.SpyInstance;

    beforeEach(() => {
      jest.spyOn(console, "error").mockImplementation();
      jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({entity: "user", regex: /users\/.*/});
      expandSpy = jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath").mockResolvedValue(new Map());
      distributeFnTransactionalSpy = jest.spyOn(indexutils, "distributeFnTransactional").mockResolvedValue([]);
      jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
      jest.spyOn(patchLogics, "queueRunPatchLogics").mockResolvedValue();
      transactionSetMock.mockClear();
    });

    afterEach(() => {
      globalProjectConfig.logicErrorFormStatus = undefined;
    });

    it("default (unset): a returned error and a thrown error still end the form 'finished' with no @messages", async () => {
      globalProjectConfig.logicErrorFormStatus = undefined;
      await runWith([okResult(), returnedErrorResult(), thrownResult()]);

      expect(formStatusUpdates()).toEqual([{"@status": "finished"}]);
      expect(transactionSetMock.mock.calls[0][1].status).toBe("processed-with-errors");
    });

    it("'finished' set explicitly behaves exactly like the default", async () => {
      globalProjectConfig.logicErrorFormStatus = "finished";
      await runWith([okResult(), returnedErrorResult(), thrownResult()]);

      expect(formStatusUpdates()).toEqual([{"@status": "finished"}]);
      expect(transactionSetMock.mock.calls[0][1].status).toBe("processed-with-errors");
    });

    it("'thrown-only' with a thrown logic ends the form 'error' with a 'logic-error:' message and still distributes the other logics' documents", async () => {
      globalProjectConfig.logicErrorFormStatus = "thrown-only";
      const logicResults = [okResult(), thrownResult()];
      await runWith(logicResults);

      const updates = formStatusUpdates();
      expect(updates).toHaveLength(1);
      expect(updates[0]["@status"]).toBe("error");
      expect(updates[0]["@messages"].startsWith("logic-error:")).toBe(true);
      expect(updates[0]["@messages"]).toContain("thrownLogic: Cannot read properties of undefined (reading 'menuItems')");
      expect(updates[0]["@messages"]).not.toContain("okLogic");
      // @actions status unchanged
      expect(transactionSetMock.mock.calls[0][1].status).toBe("processed-with-errors");
      // sibling logic's documents are still distributed
      expect(distributeFnTransactionalSpy).toHaveBeenCalledWith(expect.anything(), logicResults, "4.0.0");
      expect(expandSpy).toHaveBeenCalledWith([okDoc]);
      // the saved logicResults summary does not gain the in-memory marker
      const thrownSummary = transactionSetMock.mock.calls
        .map((call) => call[1])
        .find((data) => data && data.name === "thrownLogic");
      expect(thrownSummary).toBeDefined();
      expect(thrownSummary).not.toHaveProperty("thrown");
    });

    it("'thrown-only' never sends the client retry words, even for a thrown contention error", async () => {
      globalProjectConfig.logicErrorFormStatus = "thrown-only";
      await runWith([thrownResult("10 ABORTED: Aborted due to cross-transaction contention. Too much contention.")]);

      const updates = formStatusUpdates();
      expect(updates).toHaveLength(1);
      expect(updates[0]["@status"]).toBe("error");
      expect(updates[0]["@messages"].startsWith("logic-error:")).toBe(true);
      expect(updates[0]["@messages"]).not.toMatch(retryWords);
    });

    it("'thrown-only' with a logic that returns status 'error' (no throw) ends the form 'finished'", async () => {
      globalProjectConfig.logicErrorFormStatus = "thrown-only";
      await runWith([okResult(), returnedErrorResult()]);

      expect(formStatusUpdates()).toEqual([{"@status": "finished"}]);
    });

    it("'all' with a logic that returns status 'error' ends the form 'error' with the prefixed message", async () => {
      globalProjectConfig.logicErrorFormStatus = "all";
      await runWith([okResult(), returnedErrorResult()]);

      const updates = formStatusUpdates();
      expect(updates).toHaveLength(1);
      expect(updates[0]["@status"]).toBe("error");
      expect(updates[0]["@messages"].startsWith("logic-error:")).toBe(true);
      expect(updates[0]["@messages"]).toContain("returnedErrorLogic: Topic not found");
    });

    it("'all' lists several failing logics in one message", async () => {
      globalProjectConfig.logicErrorFormStatus = "all";
      await runWith([returnedErrorResult(), okResult(), thrownResult()]);

      const updates = formStatusUpdates();
      expect(updates).toHaveLength(1);
      expect(updates[0]["@messages"]).toContain("returnedErrorLogic: Topic not found");
      expect(updates[0]["@messages"]).toContain("thrownLogic: ");
    });

    it.each(["thrown-only", "all"] as const)("'%s' ends the form 'finished' when every logic succeeds", async (mode) => {
      globalProjectConfig.logicErrorFormStatus = mode;
      await runWith([okResult()]);

      expect(formStatusUpdates()).toEqual([{"@status": "finished"}]);
      expect(transactionSetMock.mock.calls[0][1].status).toBe("processed");
    });

    it.each([undefined, "finished", "thrown-only", "all"] as const)("mode %s: no matching logics still ends the form 'finished'", async (mode) => {
      globalProjectConfig.logicErrorFormStatus = mode;
      await runWith([], "no-matching-logics");

      expect(formStatusUpdates()).toEqual([{"@status": "finished"}]);
      expect(transactionSetMock.mock.calls[0][1].status).toBe("processed-with-errors");
    });

    it("keeps the post-commit order when the form ends 'error': form status -> queueRunViewLogics -> distributeDoc (submit-form) -> queueRunPatchLogics", async () => {
      globalProjectConfig.logicErrorFormStatus = "thrown-only";
      const callOrder: string[] = [];
      refUpdateMock.mockImplementation((data: Record<string, unknown>) => {
        if (data && data["@status"] === "error") {
          callOrder.push("formError");
        }
        return Promise.resolve();
      });
      jest.spyOn(viewLogics, "queueRunViewLogics").mockImplementation(async () => {
        callOrder.push("queueRunViewLogics");
      });
      const submitFormDoc: LogicResultDoc = {
        action: "submit-form",
        dstPath: "users/user-1/forms/test-form",
        doc: {some: "data"},
      };
      jest.spyOn(indexutils, "distributeDoc").mockImplementation(async () => {
        callOrder.push("distributeSubmitFormDoc");
      });
      jest.spyOn(patchLogics, "queueRunPatchLogics").mockImplementation(async () => {
        callOrder.push("queueRunPatchLogics");
      });
      distributeFnTransactionalSpy.mockResolvedValue([submitFormDoc]);

      await runWith([
        {name: "submitLogic", status: "finished", timeFinished: _mockable.createNowTimestamp(), documents: [submitFormDoc]},
        thrownResult(),
      ]);

      expect(callOrder).toEqual([
        "formError",
        "queueRunViewLogics",
        "distributeSubmitFormDoc",
        "queueRunPatchLogics",
      ]);
    });

    it("'thrown-only' ignores a thrown marker a logic returns itself (form ends 'finished')", async () => {
      globalProjectConfig.logicErrorFormStatus = "thrown-only";
      jest.spyOn(indexutils, "runBusinessLogics").mockImplementation(async () => ({
        status: "done",
        logicResults: [
          await indexutils.executeLogicWithMetrics("selfMarkedLogic", async () => ({
            name: "selfMarkedLogic", status: "error", message: "soft skip", documents: [], thrown: true,
          })),
        ],
      }));
      await onFormSubmit(createEvent(form));

      expect(formStatusUpdates()).toEqual([{"@status": "finished"}]);
      expect(transactionSetMock.mock.calls[0][1].status).toBe("processed-with-errors");
    });

    it("an unknown value (e.g. 'thrown_only') logs one warning and falls back to 'finished'", async () => {
      const warnSpy = console.warn as unknown as jest.Mock;
      globalProjectConfig.logicErrorFormStatus = "thrown_only" as never;
      await runWith([okResult(), thrownResult()]);
      await runWith([okResult(), thrownResult()]);

      expect(formStatusUpdates()).toEqual([{"@status": "finished"}, {"@status": "finished"}]);
      const unknownWarnings = warnSpy.mock.calls.filter((call) =>
        String(call[0]).includes("Unknown projectConfig.logicErrorFormStatus value \"thrown_only\""));
      expect(unknownWarnings).toHaveLength(1);
    });

    describe.each([undefined, "finished", "thrown-only", "all"] as const)("early exits are unchanged in mode %s", (mode) => {
      beforeEach(() => {
        globalProjectConfig.logicErrorFormStatus = mode;
      });

      it("early error (no @appVersion)", async () => {
        await onFormSubmit(createEvent({
          "formData": JSON.stringify({"@actionType": "update", "@docPath": docPath}),
          "@status": "submit",
        }));
        expect(refUpdateMock.mock.calls.map((c) => c[0])).toEqual([
          {"@status": "error", "@messages": "No appVersion found in metadata"},
        ]);
      });

      it("cancelled", async () => {
        jest.spyOn(indexutils, "delayFormSubmissionAndCheckIfCancelled").mockResolvedValue(true);
        await onFormSubmit(createEvent({
          "formData": JSON.stringify({"@actionType": "update", "@docPath": docPath, "@appVersion": "4.0.0", "@delay": 1000}),
          "@status": "submit",
        }));
        expect(refUpdateMock.mock.calls.map((c) => c[0])).toEqual([{"@status": "cancelled"}]);
      });

      it("validation-error", async () => {
        jest.spyOn(indexutils, "validateForm").mockResolvedValue([true, {field1: ["bad"]}]);
        const runBusinessLogicsSpy = jest.spyOn(indexutils, "runBusinessLogics");
        await onFormSubmit(createEvent(form));
        expect(refUpdateMock.mock.calls.map((c) => c[0])).toEqual([
          {"@status": "validation-error", "@messages": JSON.stringify({field1: ["bad"]})},
        ]);
        expect(runBusinessLogicsSpy).not.toHaveBeenCalled();
      });

      it("security-error", async () => {
        jest.spyOn(indexutils, "validateForm").mockResolvedValue([false, {}]);
        jest.spyOn(indexutils, "getSecurityFn").mockReturnValue(() =>
          Promise.resolve({status: "rejected", message: "not allowed"} as SecurityResult));
        await onFormSubmit(createEvent(form));
        expect(refUpdateMock.mock.calls.map((c) => c[0])).toEqual([
          {"@status": "security-error", "@messages": "not allowed"},
        ]);
      });
    });
  });

  describe("outer catch", () => {
    const contentionText = "10 ABORTED: Too much contention on these documents. Please try again.";
    const retryWords = /contention|aborted/i;
    const outerCatchForm = {
      "formData": JSON.stringify({"@actionType": "update", "@docPath": "users/user-1", "@appVersion": "4.0.0"}),
      "@status": "submit",
    };
    const errorUpdates = () => refUpdateMock.mock.calls.map((c) => c[0]).filter((data) => data["@status"] === "error");

    afterEach(() => {
      globalProjectConfig.logicErrorFormStatus = undefined;
    });

    it.each([undefined, "finished", "thrown-only"] as const)(
      "after commit (mode %s): rewrites contention/aborted in @messages and logs the original text", async (mode) => {
        globalProjectConfig.logicErrorFormStatus = mode;
        const errorSpy = jest.spyOn(console, "error").mockImplementation();
        const warnSpy = console.warn as unknown as jest.Mock;
        jest.spyOn(paths, "findMatchingDocPathRegex").mockReturnValue({entity: "user", regex: /users\/.*/});
        jest.spyOn(indexutils, "distributeFnTransactional").mockResolvedValue([]);
        transactionSetMock.mockClear();
        jest.spyOn(indexutils, "runBusinessLogics").mockResolvedValue({
          status: "done",
          logicResults: [{
            name: "okLogic", status: "finished", timeFinished: _mockable.createNowTimestamp(),
            documents: [{action: "merge", dstPath: "users/user-1/items/a", doc: {a: 1}}],
          }],
        });
        // The non-transactional distribution runs after the transaction has committed.
        const contentionError = Object.assign(new Error(contentionText), {code: 10});
        jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath").mockRejectedValue(contentionError);

        await onFormSubmit(createEvent(outerCatchForm));

        // the transaction committed (action doc written)
        expect(transactionSetMock).toHaveBeenCalled();
        const updates = errorUpdates();
        expect(updates).toHaveLength(1);
        expect(typeof updates[0]["@messages"]).toBe("string");
        expect(updates[0]["@messages"]).not.toMatch(retryWords);
        expect(updates[0]["@messages"]).toBe("10 stopped: Too much conflict on these documents. Please try again.");
        // the full original text stays in the server log
        expect(errorSpy).toHaveBeenCalledWith("Error in onFormSubmit", contentionError);
        expect(warnSpy.mock.calls.some((call) => String(call[0]).includes(`Original: ${contentionText}`))).toBe(true);
      });

    it.each([undefined, "finished", "thrown-only"] as const)(
      "before commit (mode %s): leaves a contention message unchanged so the client can retry", async (mode) => {
        globalProjectConfig.logicErrorFormStatus = mode;
        jest.spyOn(console, "error").mockImplementation();
        const distributeSpy = jest.spyOn(indexutils, "expandConsolidateAndGroupByDstPath");
        jest.spyOn(indexutils, "runBusinessLogics").mockRejectedValue(
          Object.assign(new Error(contentionText), {code: 10}));

        await onFormSubmit(createEvent(outerCatchForm));

        expect(distributeSpy).not.toHaveBeenCalled();
        const updates = errorUpdates();
        expect(updates).toHaveLength(1);
        expect(updates[0]["@messages"]).toBe(contentionText);
      });

    it.each([undefined, "thrown-only"] as const)("writes the error message as a readable string in @messages (mode %s)", async (mode) => {
      globalProjectConfig.logicErrorFormStatus = mode;
      jest.spyOn(console, "error").mockImplementation();
      jest.spyOn(indexutils, "runBusinessLogics").mockRejectedValue(new Error("Unexpected failure in runBusinessLogics"));

      await onFormSubmit(createEvent({
        "formData": JSON.stringify({"@actionType": "update", "@docPath": "users/user-1", "@appVersion": "4.0.0"}),
        "@status": "submit",
      }));

      const errorUpdate = refUpdateMock.mock.calls.map((c) => c[0]).find((data) => data["@status"] === "error");
      expect(errorUpdate).toBeDefined();
      expect(typeof errorUpdate["@messages"]).toBe("string");
      expect(errorUpdate["@messages"]).toBe("Unexpected failure in runBusinessLogics");
      expect(typeof errorUpdate.execTime).toBe("number");
      globalProjectConfig.logicErrorFormStatus = undefined;
    });

    it("never writes [object Object] when a non-Error value is thrown", async () => {
      jest.spyOn(console, "error").mockImplementation();
      jest.spyOn(indexutils, "runBusinessLogics").mockRejectedValue({code: "internal"});

      await onFormSubmit(createEvent({
        "formData": JSON.stringify({"@actionType": "update", "@docPath": "users/user-1", "@appVersion": "4.0.0"}),
        "@status": "submit",
      }));

      const errorUpdate = refUpdateMock.mock.calls.map((c) => c[0]).find((data) => data["@status"] === "error");
      expect(typeof errorUpdate["@messages"]).toBe("string");
      expect(errorUpdate["@messages"]).not.toContain("[object Object]");
      expect(errorUpdate["@messages"]).toBe("{\"code\":\"internal\"}");
    });
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

  let saveMetricExecution : jest.SpyInstance;
  const transactionSetMock = jest.fn();
  const mockTxn = {
    set: transactionSetMock,
    get: jest.fn().mockResolvedValue({}),
  } as unknown as Transaction;

  beforeEach(() => {
    admin.initializeApp({databaseURL: "https://test-project.firebaseio.com"});
    saveMetricExecution = jest.spyOn(indexutils._mockable, "saveMetricExecution").mockResolvedValue();
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("should run the onUserRegister correctly", async () => {
    initializeEmberFlow({
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs: [],
      validatorConfigs: [],
      logicConfigs: [],
      patchLogicConfigs: [],
    });
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
    expect(saveMetricExecution).toHaveBeenNthCalledWith(1, [
      {name: "onUserRegister", execTime: expect.any(Number)},
    ]);
  });

  it("should run the onUserRegister along with the customUserRegisterFn", async () => {
    const customUserRegisterLogicFn = jest.fn().mockResolvedValue(customUserRegisterLogicResult);
    initializeEmberFlow({
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs: [],
      validatorConfigs: [],
      logicConfigs: [],
      patchLogicConfigs: [],
      userRegisterFn: customUserRegisterLogicFn,
    });
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
    expect(distributeFnTransactionalSpy).toHaveBeenNthCalledWith(1, mockTxn, [customUserRegisterLogicResult], "no_app_version");
    expect(saveMetricExecution).toHaveBeenNthCalledWith(1, [
      {name: "onUserRegister", execTime: expect.any(Number)},
      {name: "customUserRegisterLogic", execTime: expect.any(Number)},
    ]);
  });
});

describe("initializeEmberFlow with hourlyLogicConfigs", () => {
  it("should register hourlyBusinessLogics in functionsConfig and populate hourlyLogicConfigs", () => {
    const mockHourlyLogic = {
      name: "TestHourlyLogic",
      hourlyLogicFn: jest.fn(),
    };

    const {functionsConfig} = initializeEmberFlow({
      projectConfig,
      admin,
      dbStructure,
      Entity,
      securityConfigs: [],
      validatorConfigs: [],
      logicConfigs: [],
      patchLogicConfigs: [],
      hourlyLogicConfigs: [mockHourlyLogic],
    });

    expect(functionsConfig.hourlyBusinessLogics).toBeDefined();
    expect(hourlyLogicConfigs).toEqual([mockHourlyLogic]);
  });
});
