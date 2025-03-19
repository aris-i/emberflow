import * as admin from "firebase-admin";
import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;
import DocumentData = firestore.DocumentData;
import {DocumentReference} from "firebase-admin/lib/firestore";
import Query = firestore.Query;
import QuerySnapshot = firestore.QuerySnapshot;
import DocumentSnapshot = firestore.DocumentSnapshot;

export type FirebaseAdmin = typeof admin;

export interface ProjectConfig {
    projectId: string;
    region: string;
    rtdbName: string;
    budgetAlertTopicName: string;
    maxCostLimitPerFunction: number;
    specialCostLimitPerFunction: { [key: string]: number };
}
export type ActionType = "create" | "update" | "delete";
export interface Action{
    eventContext: EventContext,
    actionType: ActionType;
    document: FirebaseFirestore.DocumentData;
    modifiedFields: DocumentData;
    user: DocumentData;
    status: "new" | "processing" | "processed" | "processed-with-errors";
    timeCreated: Timestamp;
    message?: string
}

type txnGet1 = <T>(query: Query<T>)=> Promise<QuerySnapshot<T>>;
type txnGet2 = <T>(documentRef: DocumentReference<T>)=> Promise<DocumentSnapshot<T>>;
export type TxnGet = txnGet1 | txnGet2;

export type LogicResultDocAction = "create" | "merge" | "delete" | "copy" | "recursive-copy" | "recursive-delete"
    | "submit-form" | "simulate-submit-form";
export type LogicResultDocPriority = "high" | "normal" | "low";
export interface LogicResultDoc{
    action: LogicResultDocAction;
    dstPath: string;
    priority?: LogicResultDocPriority;
    srcPath?: string;
    doc?: FirebaseFirestore.DocumentData;
    instructions?: { [key: string]: string };
    journalEntries?: JournalEntry[];
    skipEntityDuringRecursion?: string[];
    skipRunViewLogics?: boolean;
}

export interface LedgerEntry {
    account: string;
    debit: number;
    credit: number;
    description?: string;
}

export interface JournalEntry {
    date: Timestamp;
    ledgerEntries: LedgerEntry[];
    equation: string;
    recordEntry?: boolean;
}

export type Instructions = { [key: string]: string };
export interface InstructionsMessage{
    dstPath: string;
    instructions: Instructions;
}

export interface LogicResult{
    name: string;
    status: "finished" | "error";
    nextPage?: AnyObject;
    message?: string;
    execTime?: number;
    timeFinished?: Timestamp;
    documents: LogicResultDoc[];
    transactional?: boolean;
}
export type LogicFn = (txnGet: TxnGet, action: Action, sharedMap: Map<string, any>) => Promise<LogicResult>;
export type LogicActionType = "create" | "update" | "delete";
export type LogicConfigActionTypes = LogicActionType[] | "all";
export type LogicConfigEntities = string[] | "all";
export type LogicConfigModifiedFieldsType = "all" | string[];
export type LogicConfigFilterFn = (
    actionType: string,
    modifiedFields: FirebaseFirestore.DocumentData,
    document: FirebaseFirestore.DocumentData,
    entity: string,
) => boolean;
export interface LogicConfig{
    name: string;
    actionTypes: LogicConfigActionTypes;
    modifiedFields: LogicConfigModifiedFieldsType;
    entities: LogicConfigEntities;
    addtlFilterFn?: LogicConfigFilterFn;
    logicFn: LogicFn;
}

export type ViewLogicFn = (logicResultDoc: LogicResultDoc) => Promise<LogicResult>;
export interface ViewLogicConfig{
    name: string;
    actionTypes: LogicResultDocAction[];
    modifiedFields: LogicConfigModifiedFieldsType;
    entity: string;
    destProp?: string;
    viewLogicFn: ViewLogicFn;
}

export type SecurityStatus = "allowed" | "rejected"
export interface SecurityResult {
    status: SecurityStatus;
    message?: string;
}

export type SecurityFn = (entity: string, docPath: string, document: FirebaseFirestore.DocumentData, actionType: LogicActionType,
                          modifiedFields: DocumentData, user: DocumentData, ) => Promise<SecurityResult>;
export type SecurityConfig = Record<string, SecurityFn>;
export interface ValidationResult {
    [key: string]: string[];
}
export type ValidatorFn = (document: DocumentData) => Promise<ValidationResult>;
export type ValidatorConfig = Record<string, ValidatorFn>;
export type ValidateFormResult = [hasValidationErrors: boolean, validationResult: ValidationResult];

export type DestPropType = "map"|"array-map";
export interface ViewDefinition {
    destEntity: string;
    destProp? : {
        name: string;
        type: DestPropType;
    }
    srcProps: string[];
    srcEntity: string;
}

export type IdGenerator = (collectionPath: string) => Promise<string[]>;

export interface QueryCondition {
    fieldName: string;
    operator: firestore.WhereFilterOp;
    value: any;
}

export type EntityCondition = Record<string, QueryCondition>;

export type AnyObject = { [key: string]: any };

export interface EventContext {
    id: string;
    uid: string;
    formId: string;
    docId: string;
    docPath: string;
    entity: string;
}

export type RunBusinessLogicStatus = {
    status: "running" | "done" | "cancel-then-retry" | "no-matching-logics",
    logicResults: LogicResult[],
};
