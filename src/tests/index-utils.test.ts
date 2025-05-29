import * as admin from "firebase-admin";
import * as indexUtils from "../index-utils";
import {firestore} from "firebase-admin";
import {initializeEmberFlow, _mockable, db} from "../index";
import {
  Action,
  LogicConfig,
  LogicResult,
  LogicResultDocAction,
  LogicResultDoc,
  ProjectConfig,
  ViewLogicConfig, TxnGet,
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
import {DocumentReference} from "firebase-admin/lib/firestore";
import CollectionReference = firestore.CollectionReference;
import * as misc from "../utils/misc";
import {ScheduledEvent} from "firebase-functions/lib/v2/providers/scheduler";
import {
  cleanMetricComputations,
  cleanMetricExecutions,
  createMetricComputation,
} from "../index-utils";
import * as viewLogics from "../logics/view-logics";

// should mock when using initializeEmberFlow and testing db.doc() calls count
jest.spyOn(indexUtils, "createMetricLogicDoc").mockResolvedValue();

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

const txnGet: TxnGet = {
  get: jest.fn(),
};

const txnGetFnMock = jest.fn();
const transactionSetMock = jest.fn();
const transactionUpdateMock = jest.fn();
const transactionMock = {
  get: txnGetFnMock,
  set: transactionSetMock,
  update: transactionUpdateMock,
  delete: jest.fn(),
} as any;

jest.mock("../utils/paths", () => {
  const originalModule = jest.requireActual("../utils/paths");

  return {
    ...originalModule,
    expandAndGroupDocPathsByEntity: jest.fn(),
  };
});

describe("distributeDoc", () => {
  let dbSpy: jest.SpyInstance;
  let queueRunViewLogicsSpy: jest.SpyInstance;
  let queueInstructionsSpy: jest.SpyInstance;
  let dbDoc: admin.firestore.DocumentReference<admin.firestore.DocumentData>;
  const batch = BatchUtil.getInstance();
  jest.spyOn(BatchUtil, "getInstance").mockImplementation(() => batch);

  beforeEach(() => {
    dbDoc = ({
      set: jest.fn().mockResolvedValue({}),
      update: jest.fn().mockResolvedValue({}),
      delete: jest.fn().mockResolvedValue({}),
      get: jest.fn().mockResolvedValue({exists: true, data: () => ({})}),
      id: "test-doc-id",
    } as unknown) as admin.firestore.DocumentReference<admin.firestore.DocumentData>;
    dbSpy = jest.spyOn(admin.firestore(), "doc").mockReturnValue(dbDoc);
    queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
    queueInstructionsSpy = jest.spyOn(distribution, "queueInstructions").mockResolvedValue();
  });

  afterEach(() => {
    dbSpy.mockRestore();
    queueInstructionsSpy.mockRestore();
    queueRunViewLogicsSpy.mockRestore();
  });

  it("should create a document to dstPath", async () => {
    const logicResultDoc: LogicResultDoc = {
      action: "create",
      priority: "normal",
      dstPath: "/users/test-user-id/documents/test-doc-id",
      doc: {name: "test-doc-name-updated"},
    };
    const expectedData = {
      ...logicResultDoc.doc,
      "@id": "test-doc-id",
      "@dateCreated": expect.any(Timestamp),
    };

    await indexUtils.distributeDoc(logicResultDoc);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(dbDoc.set).toHaveBeenCalledTimes(1);
    expect(dbDoc.set).toHaveBeenCalledWith(expectedData, {merge: true});
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

    const dstDocRef = db.doc("/users/test-user-id/documents/test-doc-id");
    expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
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

    const dstDocRef = db.doc(logicResultDoc.dstPath);
    expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
  });

  it("should process instructions if using transaction", async () => {
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

    await indexUtils.distributeDoc(logicResultDoc, undefined, transactionMock);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(queueInstructionsSpy).not.toHaveBeenCalled();

    const expectedInstructions = {
      "count": admin.firestore.FieldValue.increment(1),
      "score": admin.firestore.FieldValue.increment(5),
      "minusCount": admin.firestore.FieldValue.increment(-1),
      "minusScore": admin.firestore.FieldValue.increment(-3),
    };
    const dstDocRef = db.doc(logicResultDoc.dstPath);
    expect(transactionSetMock).toHaveBeenNthCalledWith(1, dstDocRef, expectedInstructions, {merge: true});
    expect(transactionSetMock).toHaveBeenNthCalledWith(2, dstDocRef, expectedData, {merge: true});
  });

  it("should add parsed instructions to destPropId field if has destPropId", async () => {
    transactionSetMock.mockReset();
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
      dstPath: "/users/test-user-id/documents/test-doc-id#destination[sampleDestPropId]",
    };
    const expectedData = {
      destination: {
        sampleDestPropId: {
          ...logicResultDoc.doc,
        },
      },
    };

    await indexUtils.distributeDoc(logicResultDoc, undefined, transactionMock);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(queueInstructionsSpy).not.toHaveBeenCalled();

    const expectedInstructions = {
      destination: {
        sampleDestPropId: {
          "count": admin.firestore.FieldValue.increment(1),
          "score": admin.firestore.FieldValue.increment(5),
          "minusCount": admin.firestore.FieldValue.increment(-1),
          "minusScore": admin.firestore.FieldValue.increment(-3),
        },
      },
    };

    const dstDocRef = db.doc(logicResultDoc.dstPath);
    expect(transactionSetMock).toHaveBeenNthCalledWith(1, dstDocRef, expectedInstructions, {merge: true});
    expect(transactionSetMock).toHaveBeenNthCalledWith(2, dstDocRef, expectedData, {merge: true});
  });

  it("should not merge when document is undefined", async () => {
    const logicResultDoc: LogicResultDoc = {
      action: "merge",
      priority: "normal",
      dstPath: "/users/test-user-id/documents/test-doc-id",
    };

    await indexUtils.distributeDoc(logicResultDoc);
    expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
    expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
    expect(dbDoc.update).not.toHaveBeenCalled();
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
    expect(dbDoc.delete).toHaveBeenCalledTimes(1);
    expect(dbDoc.delete).toHaveBeenCalled();
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
    expect(queueRunViewLogicsSpy).toHaveBeenCalledTimes(0);

    queueSubmitFormSpy.mockRestore();
  });

  describe("batched", () => {
    it("should create a document to dstPath", async () => {
      const batchSetSpy = jest.spyOn(batch, "set").mockResolvedValue(undefined);
      const logicResultDoc: LogicResultDoc = {
        action: "create",
        priority: "normal",
        dstPath: "/users/test-user-id/documents/test-doc-id",
        doc: {name: "test-doc-name-updated"},
      };
      const expectedData = {
        ...logicResultDoc.doc,
        "@id": "test-doc-id",
        "@dateCreated": expect.any(Timestamp),
      };

      await indexUtils.distributeDoc(logicResultDoc, batch);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
      expect(batchSetSpy).toHaveBeenCalledTimes(1);
      expect(batchSetSpy.mock.calls[0][1]).toEqual(expectedData);

      batchSetSpy.mockRestore();
    });

    it("should merge a document to dstPath", async () => {
      const batchSetSpy = jest.spyOn(batch, "set").mockResolvedValue(undefined);
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

      await indexUtils.distributeDoc(logicResultDoc, batch);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
      expect(batchSetSpy).toHaveBeenCalledTimes(1);
      expect(batchSetSpy.mock.calls[0][1]).toEqual(expectedData);

      batchSetSpy.mockRestore();
    });

    it("should a delete document from dstPath", async () => {
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
  });

  describe("map destProp", () => {
    it("should create destProp", async () => {
      const logicResultDoc: LogicResultDoc = {
        action: "create",
        priority: "normal",
        dstPath: "/users/test-user-id/documents/test-doc-id#createdBy",
        doc: {name: "test-doc-name-updated"},
      };
      const expectedData = {
        "createdBy": logicResultDoc.doc,
      };

      await indexUtils.distributeDoc(logicResultDoc);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");

      const dstDocRef = db.doc(logicResultDoc.dstPath);
      expect(dstDocRef.set).toHaveBeenCalledTimes(1);
      expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
    });

    it("should update destProp", async () => {
      const logicResultDoc: LogicResultDoc = {
        action: "merge",
        priority: "normal",
        dstPath: "/users/test-user-id/documents/test-doc-id#createdBy",
        doc: {name: "test-doc-name-updated"},
      };
      const expectedData = {
        "createdBy": {
          name: "test-doc-name-updated",
        },
      };

      await indexUtils.distributeDoc(logicResultDoc);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");

      const dstDocRef = db.doc(logicResultDoc.dstPath);
      expect(dstDocRef.set).toHaveBeenCalledTimes(1);
      expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
    });

    it("should delete destProp", async () => {
      const logicResultDoc: LogicResultDoc = {
        action: "delete",
        priority: "normal",
        dstPath: "/users/test-user-id/documents/test-doc-id#createdBy",
      };
      const expectedData = {
        "createdBy": admin.firestore.FieldValue.delete(),
      };

      await indexUtils.distributeDoc(logicResultDoc);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");

      const dstDocRef = db.doc(logicResultDoc.dstPath);
      expect(dstDocRef.set).toHaveBeenCalledTimes(1);
      expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
      expect(dbDoc.delete).not.toHaveBeenCalled();
    });
  });

  describe("array map destProp", () => {
    it("should create destProp", async () => {
      const logicResultDoc: LogicResultDoc = {
        action: "create",
        priority: "normal",
        dstPath: "/users/test-user-id/documents/test-doc-id#followers[test-another-user]",
        doc: {name: "test-doc-name-updated"},
      };
      const expectedData = {
        "followers": {
          "test-another-user": logicResultDoc.doc,
        },
      };

      await indexUtils.distributeDoc(logicResultDoc);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");

      const dstDocRef = db.doc(logicResultDoc.dstPath);
      expect(dstDocRef.set).toHaveBeenCalledTimes(1);
      expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
    });

    it("should update destProp", async () => {
      const logicResultDoc: LogicResultDoc = {
        action: "merge",
        priority: "normal",
        dstPath: "/users/test-user-id/documents/test-doc-id#followers[test-another-user]",
        doc: {name: "test-doc-name-updated"},
      };
      const expectedData = {
        "followers": {
          "test-another-user": {
            "name": "test-doc-name-updated",
          },
        },
      };

      await indexUtils.distributeDoc(logicResultDoc);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
      const dstDocRef = db.doc(logicResultDoc.dstPath);
      expect(dstDocRef.set).toHaveBeenCalledTimes(1);
      expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
    });

    it("should delete destProp", async () => {
      const logicResultDoc: LogicResultDoc = {
        action: "delete",
        priority: "normal",
        dstPath: "/users/test-user-id/documents/test-doc-id#followers[test-another-user]",
      };
      const expectedData = {
        "followers": {
          "test-another-user": admin.firestore.FieldValue.delete(),
        },
      };

      await indexUtils.distributeDoc(logicResultDoc);
      expect(admin.firestore().doc).toHaveBeenCalledTimes(1);
      expect(admin.firestore().doc).toHaveBeenCalledWith("/users/test-user-id/documents/test-doc-id");
      const dstDocRef = db.doc(logicResultDoc.dstPath);
      expect(dstDocRef.set).toHaveBeenCalledTimes(1);
      expect(dstDocRef.set).toHaveBeenCalledWith(expectedData, {merge: true});
      expect(dbDoc.delete).not.toHaveBeenCalled();
    });
  });
});

describe("distribute", () => {
  let dbSpy: jest.SpyInstance;
  let colSpy: jest.SpyInstance;
  let queueInstructionsSpy: jest.SpyInstance;
  let queueRunViewLogicsSpy: jest.SpyInstance;
  let dbDoc: admin.firestore.DocumentReference<admin.firestore.DocumentData>;
  const batch = BatchUtil.getInstance();
  jest.spyOn(BatchUtil, "getInstance").mockImplementation(() => batch);

  beforeEach(() => {
    dbDoc = ({
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
    queueRunViewLogicsSpy = jest.spyOn(viewLogics, "queueRunViewLogics").mockResolvedValue();
  });

  afterEach(() => {
    dbSpy.mockRestore();
    colSpy.mockRestore();
    queueInstructionsSpy.mockRestore();
    queueRunViewLogicsSpy.mockRestore();
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
    await indexUtils.distributeFnNonTransactional(userDocsByDstPath);

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
    await indexUtils.distributeFnNonTransactional(userDocsByDstPath);

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
  beforeEach(() => {
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, []);
  });

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
  let logicFn4: jest.Mock;
  let logicFn5: jest.Mock;
  let logicFn6: jest.Mock;

  let dbSpy: jest.SpyInstance;
  let actionRef: DocumentReference;

  beforeEach(() => {
    distributeFn = jest.fn();
    logicFn1 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn2 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn3 = jest.fn().mockResolvedValue({status: "error", message: "Error message"});
    logicFn4 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn5 = jest.fn().mockResolvedValue({status: "finished"});
    logicFn6 = jest.fn().mockResolvedValue({status: "finished"});

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
      {
        name: "Logic 4",
        actionTypes: ["create"],
        modifiedFields: ["field1"],
        entities: ["user"],
        logicFn: logicFn4,
        addtlFilterFn(actionType) {
          return actionType !== "create";
        },
      },
      {
        name: "Logic 5",
        actionTypes: ["create"],
        modifiedFields: ["field2"],
        entities: ["user"],
        logicFn: logicFn5,
        addtlFilterFn(actionType, modifiedFields) {
          return !Object.prototype.hasOwnProperty.call(modifiedFields, "field1");
        },
      },
      {
        name: "Logic 6",
        actionTypes: ["create"],
        modifiedFields: ["field2"],
        entities: ["user"],
        logicFn: logicFn6,
        addtlFilterFn(actionType, modifiedFields, document) {
          return !Object.prototype.hasOwnProperty.call(document, "field3");
        },
      },
    ];
    initializeEmberFlow(projectConfig, admin, dbStructure, Entity, securityConfig, validatorConfig, logics);
    const runStatus = await indexUtils.runBusinessLogics(txnGet, action);

    expect(logicFn1).toHaveBeenCalledWith(txnGet, action, new Map());
    expect(logicFn2).toHaveBeenCalledWith(txnGet, action, new Map());
    expect(logicFn3).not.toHaveBeenCalled();
    expect(logicFn4).not.toHaveBeenCalled();
    expect(logicFn5).not.toHaveBeenCalled();
    expect(logicFn6).not.toHaveBeenCalled();
    expect(runStatus).toEqual({
      status: "done",
      logicResults: [{
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }, {
        status: "finished",
        execTime: expect.any(Number),
        timeFinished: expect.any(Timestamp),
      }],
    });
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
    const runStatus = await indexUtils.runBusinessLogics(txnGet, action);

    expect(logicFn1).not.toHaveBeenCalled();
    expect(logicFn2).not.toHaveBeenCalled();
    expect(logicFn3).not.toHaveBeenCalled();
    expect(runStatus).toEqual({
      status: "no-matching-logics",
      logicResults: [],
    });
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
      logicFn2.mockImplementation((txnGet, action, sharedMap) => {
        sharedMap.set("another-document-id", expectedSharedMap.get("another-document-id"));
        return {status: "finished"};
      });
      logicFn3.mockImplementation((txnGet, action, sharedMap) => {
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
      const runStatus = await indexUtils.runBusinessLogics(txnGet, action);

      expect(logicFn1).toHaveBeenCalledWith(txnGet, action, expectedSharedMap);
      expect(logicFn2).toHaveBeenCalledWith(txnGet, action, expectedSharedMap);
      expect(logicFn3).toHaveBeenCalledWith(txnGet, action, expectedSharedMap);

      expect(runStatus.status).toEqual("done");

      const logicResults = runStatus.logicResults;
      expect(logicResults.length).toEqual(3);
      expect(logicResults[0].status).toEqual("finished");
      expect(logicResults[1].status).toEqual("finished");
      expect(logicResults[2].status).toEqual("finished");
    });
});

describe("groupDocsByTargetDocPath", () => {
  initializeEmberFlow(projectConfig, admin, dbStructure, Entity, {}, {}, []);
  const docsByDstPath = new Map<string, LogicResultDoc[]>([
    ["users/user123/document1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}}]],
    ["users/user123/document1/threads/thread1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1/threads/thread1", doc: {field3: "value3", field6: "value6"}}]],
    ["users/user123/document1/messages/message1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1/messages/message1", doc: {field4: "value4"}}]],
    ["users/user123/document1/images/image1", [{action: "merge", priority: "low", dstPath: "users/user123/document1/images/image1", doc: {field7: "value7"}}]],
    ["othercollection/document2", [{action: "merge", priority: "normal", dstPath: "othercollection/document2", doc: {field5: "value5"}}]],
    ["othercollection/document3", [{action: "delete", priority: "normal", dstPath: "othercollection/document3"}]],
  ]);

  it("should group documents by docPath", () => {
    const docPath = "users/user123/document1";
    const expectedResults = {
      docsByDocPath: new Map<string, LogicResultDoc[]>([
        ["users/user123/document1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1", doc: {field1: "value1", field2: "value2"}}]],
        ["users/user123/document1/threads/thread1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1/threads/thread1", doc: {field3: "value3", field6: "value6"}}]],
        ["users/user123/document1/messages/message1", [{action: "merge", priority: "normal", dstPath: "users/user123/document1/messages/message1", doc: {field4: "value4"}}]],
        ["users/user123/document1/images/image1", [{action: "merge", priority: "low", dstPath: "users/user123/document1/images/image1", doc: {field7: "value7"}}]],
      ]),
      otherDocsByDocPath: new Map<string, LogicResultDoc[]>([
        ["othercollection/document2", [{action: "merge", priority: "normal", dstPath: "othercollection/document2", doc: {field5: "value5"}}]],
        ["othercollection/document3", [{action: "delete", priority: "normal", dstPath: "othercollection/document3"}]],
      ]),
    };

    const results = indexUtils.groupDocsByTargetDocPath(docsByDstPath, docPath);
    expect(results).toEqual(expectedResults);
  });
});

describe("expandConsolidateAndGroupByDstPath", () => {
  let dbSpy: jest.SpyInstance;
  let consoleWarnSpy: jest.SpyInstance;
  let consoleErrorSpy: jest.SpyInstance;

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
    consoleErrorSpy = jest.spyOn(console, "error").mockImplementation(jest.fn());

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
    consoleErrorSpy.mockRestore();
  });

  it("should consolidate logic results documents correctly", async () => {
    // Arrange
    const logicResults: LogicResult[] = [
      {
        name: "logic 1",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "create", priority: "normal", dstPath: "path8/doc8", doc: {field1: "value1"},
            instructions: {numberField: "-23", arrayField: "arr(+entry2)", toBeDeleted: "+24"},
          },
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1"}},
          {action: "merge", priority: "normal", dstPath: "path11/doc11", doc: {field3: "value3"}},
        ],
      },
      {
        name: "logic 2",
        timeFinished: firestore.Timestamp.now(),
        status: "finished",
        documents: [
          {action: "merge", priority: "normal", dstPath: "path8/doc8", doc: {field3: "value3"},
            instructions: {numberField: "+25", arrayField: "arr(-entry1)", toBeDeleted: "del"}},
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a"}},
          {action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field3: "value3"}},
          {action: "copy", priority: "normal", srcPath: "path3/doc3", dstPath: "path4/doc4"},
          {action: "merge", priority: "normal", dstPath: "path2/doc2", doc: {field4: "value4"}},
          {action: "merge", priority: "normal", dstPath: "path7/doc7", doc: {field6: "value7"}},
          {action: "create", priority: "normal", dstPath: "path10/doc10", doc: {field10: "value10"}},
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
          {action: "create", priority: "normal", dstPath: "path9/doc9"},
          {action: "create", priority: "normal", dstPath: "path10/doc10"},
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
          {action: "merge", priority: "normal", dstPath: "path11/doc11", doc: {field1: "value1"}},
        ],
      },
    ];

    // Act
    const logicResultDocs = logicResults.map((logicResult) => logicResult.documents).flat();
    const result = await indexUtils.expandConsolidateAndGroupByDstPath(logicResultDocs);

    // Assert
    const expectedResult = new Map<string, LogicResultDoc[]>([
      ["path1/doc1", [{action: "merge", priority: "normal", dstPath: "path1/doc1", doc: {field1: "value1a", field3: "value3"}}]],
      ["path2/doc2", [{action: "delete", priority: "normal", dstPath: "path2/doc2"}]],
      ["path4/doc4", [{action: "merge", priority: "normal", dstPath: "path4/doc4", doc: {}}]],
      ["path7/doc7", [{action: "delete", priority: "normal", dstPath: "path7/doc7"}]],
      ["path6/doc6", [{action: "merge", priority: "normal", dstPath: "path6/doc6", doc: {}}]],
      ["path8/doc8", [{action: "create", priority: "normal", dstPath: "path8/doc8", doc: {field1: "value1", field3: "value3"},
        instructions: {numberField: "+2", arrayField: "arr(+entry2,-entry1)", toBeDeleted: "del"}}]],
      ["path9/doc9", [{action: "create", priority: "normal", dstPath: "path9/doc9", doc: {field9: "value9"}}]],
      ["path10/doc10", [{action: "create", priority: "normal", dstPath: "path10/doc10", doc: {field10: "value10"}}]],
      ["path11/doc11", [{action: "merge", priority: "normal", dstPath: "path11/doc11", doc: {field3: "value3", field1: "value1"}}]],
    ]);

    expect(result).toEqual(expectedResult);

    // Verify that console.warn was called
    expect(consoleWarnSpy).toHaveBeenCalledTimes(5);
    expect(consoleWarnSpy.mock.calls[0][0]).toBe("Overwriting key \"field1\" in doc for dstPath \"path1/doc1\"");
    expect(consoleWarnSpy.mock.calls[1][0]).toBe("Action merge for dstPath \"path7/doc7\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[2][0]).toBe("Action delete for dstPath \"path7/doc7\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[3][0]).toBe("Action merge for dstPath \"path2/doc2\" is being overwritten by action \"delete\"");
    expect(consoleWarnSpy.mock.calls[4][0]).toBe("Action merge ignored because a \"delete\" for dstPath \"path7/doc7\" already exists");
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
      actionTypes: ["merge", "delete"],
      modifiedFields: ["sampleField1", "sampleField2"],
      viewLogicFn: viewLogicFn1,
    },
    {
      name: "logic 2",
      entity: "user",
      actionTypes: ["merge", "delete"],
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

describe("createMetricExecution", () => {
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

    await indexUtils._mockable.createMetricExecution(logicResults);
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

    await indexUtils._mockable.createMetricExecution(logicResults);
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

describe("cleanMetricExecutions", () => {
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

  it("should clean metric executions", async () => {
    jest.spyOn(console, "info").mockImplementation();
    await cleanMetricExecutions({} as ScheduledEvent);

    expect(console.info).toHaveBeenCalledWith("Running cleanMetricExecutions");
    expect(admin.firestore().collection).toHaveBeenCalledTimes(1);
    expect(admin.firestore().collection).toHaveBeenCalledWith("@metrics");
    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(deleteCollectionSpy).toHaveBeenCalled();
    expect(console.info).toHaveBeenCalledWith("Cleaned 1 logic metric executions");
  });
});

describe("createMetricComputation", () => {
  let colGetMock: jest.Mock;
  let setMock: jest.Mock;
  let getMock: jest.Mock;
  const maxExecTime = 50.125;
  const minExecTime = 5.5;
  const jitterTime = maxExecTime - minExecTime;
  const execDates = {
    docs: [
      {
        data: () => ({
          execTime: 30.25,
        }),
      },
      {
        data: () => ({
          execTime: maxExecTime,
        }),
      },
      {
        data: () => ({
          execTime: minExecTime,
        }),
      },
      {
        data: () => ({
          execTime: 10,
        }),
      },
    ],
  };

  beforeEach(() => {
    setMock = jest.fn().mockResolvedValue({});
    getMock = jest.fn().mockResolvedValue(execDates);
    colGetMock = jest.fn().mockResolvedValue({
      docs: [
        {
          id: "metricId",
          ref: {
            collection: jest.fn().mockReturnValue({
              where: jest.fn().mockReturnValue({
                get: getMock,
              }),
              doc: jest.fn().mockReturnValue({
                set: setMock,
              }),
            }),
          },
        },
      ],
    });

    jest.spyOn(admin.firestore(), "collection").mockReturnValue({
      get: colGetMock,
    } as any);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should log when there are no executions found", async () => {
    jest.spyOn(console, "info").mockImplementation();
    getMock.mockResolvedValueOnce({empty: true});

    await createMetricComputation({} as ScheduledEvent);

    expect(console.info).toHaveBeenCalledWith("Creating metric computation");
    expect(admin.firestore().collection).toHaveBeenCalledTimes(1);
    expect(admin.firestore().collection).toHaveBeenCalledWith("@metrics");
    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(console.info).toHaveBeenCalledWith("No executions found for metricId");
    expect(setMock).not.toHaveBeenCalled();
  });

  it("should create metric computation", async () => {
    jest.spyOn(console, "info").mockImplementation();
    const execTimes = execDates.docs.map((doc) => doc.data().execTime);
    const totalExecTime = execTimes.reduce((a, b) => a + b, 0);
    const execCount = execTimes.length;
    const avgExecTime = totalExecTime / execCount;

    await createMetricComputation({} as ScheduledEvent);

    expect(console.info).toHaveBeenCalledWith("Creating metric computation");
    expect(admin.firestore().collection).toHaveBeenCalledTimes(1);
    expect(admin.firestore().collection).toHaveBeenCalledWith("@metrics");
    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(setMock).toHaveBeenCalledTimes(1);
    expect(setMock).toHaveBeenCalledWith({
      createdAt: expect.any(Timestamp),
      maxExecTime,
      minExecTime,
      totalExecTime,
      execCount,
      avgExecTime,
      jitterTime,
    });
  });
});

describe("cleanMetricComputations", () => {
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

  it("should clean metric computations", async () => {
    jest.spyOn(console, "info").mockImplementation();
    await cleanMetricComputations({} as ScheduledEvent);

    expect(console.info).toHaveBeenCalledWith("Running cleanMetricComputations");
    expect(admin.firestore().collection).toHaveBeenCalledTimes(1);
    expect(admin.firestore().collection).toHaveBeenCalledWith("@metrics");
    expect(colGetMock).toHaveBeenCalledTimes(1);
    expect(deleteCollectionSpy).toHaveBeenCalled();
    expect(console.info).toHaveBeenCalledWith("Cleaned 1 logic metric computations");
  });
});
