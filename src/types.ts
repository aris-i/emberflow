import * as admin from "firebase-admin";
import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;
import DocumentData = firestore.DocumentData;

export type FirebaseAdmin = typeof admin;

export interface ProjectConfig {
    projectId: string;
    region: string;
    rtdbName: string;
    budgetAlertTopicName: string;
    maxCostLimitPerFunction: number;
    specialCostLimitPerFunction: { [key: string]: number };
}

export interface Action{
    eventContext: EventContext,
    actionType: string;
    document: FirebaseFirestore.DocumentData;
    form: FirebaseFirestore.DocumentData;
    modifiedFields: DocumentData;
    user: DocumentData;
    status: "new" | "processing" | "processed" | "processed-with-errors";
    timeCreated: Timestamp;
    message?: string
}

export type LogicResultAction = "merge" | "delete" | "copy";
export interface LogicResultDoc{
    action: LogicResultAction;
    dstPath: string;
    srcPath?: string;
    doc?: FirebaseFirestore.DocumentData;
    instructions?: { [key: string]: string };
    skipEntityDuringRecursiveCopy?: string[];
    copyMode?: "shallow" | "recursive";
}

export interface LogicResult{
    name: string;
    status: "finished" | "error";
    message?: string;
    execTime?: number;
    timeFinished?: Timestamp;
    documents: LogicResultDoc[];
}
export type LogicFn = (action: Action) => Promise<LogicResult>;
export type LogicActionType = "create" | "update" | "delete";
export interface LogicConfig{
    name: string;
    actionTypes: LogicActionType[] | "all";
    modifiedFields: string[] | "all"
    entities: string[] | "all";
    logicFn: LogicFn;
}

export type ViewLogicFn = (logicResultDoc: LogicResultDoc) => Promise<LogicResult>;
export interface ViewLogicConfig{
    name: string;
    modifiedFields: string[];
    entity: string;
    viewLogicFn: ViewLogicFn;
}

export type SecurityStatus = "allowed" | "rejected"
export interface SecurityResult {
    status: SecurityStatus;
    message?: string;
}

export type SecurityFn = (entity: string, form: FirebaseFirestore.DocumentData,
                          document: FirebaseFirestore.DocumentData, actionType: LogicActionType,
                          modifiedFields: DocumentData, user: DocumentData, ) => Promise<SecurityResult>;
export type SecurityConfig = Record<string, SecurityFn>;
export interface ValidationResult {
    [key: string]: string[];
}
export type ValidatorFn = (document: DocumentData) => Promise<ValidationResult>;
export type ValidatorConfig = Record<string, ValidatorFn>;
export type ValidateFormResult = [hasValidationErrors: boolean, validationResult: ValidationResult];

export interface ViewDefinition {
    destEntity: string;
    destProp?: string;
    srcProps: string[];
    srcEntity: string;
}

export type IdGenerator = (collectionPath: string) => Promise<string[]>;

export interface QueryCondition {
    fieldName: string;
    operator: firestore.WhereFilterOp;
    value: any;
}

export type AnyObject = { [key: string]: any };

export interface ScheduledEntity {
    colPath: string;
    data: { [key: string]: any };
    runAt: Timestamp;
}

export interface EventContext {
    id: string;
    uid: string;
    formId: string;
    docId: string;
    docPath: string;
    entity: string;
}
