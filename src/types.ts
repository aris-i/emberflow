import * as admin from "firebase-admin";
import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;
import DocumentData = firestore.DocumentData;

export type FirebaseAdmin = typeof admin;

export interface Action{
    actionType: string;
    path: string;
    document: FirebaseFirestore.DocumentData;
    modifiedFields?: string[];
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
    timeFinished: Timestamp;
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

export type SecurityStatus = "allowed" | "rejected"
export interface SecurityResult {
    status: SecurityStatus;
    message?: string;
}

export type SecurityFn = (entity: string, doc: DocumentData, actionType: LogicActionType, modifiedFields?: string[]) => Promise<SecurityResult>;
export type SecurityConfig = Record<string, SecurityFn>;
export interface ValidationResult {
    [key: string]: string[];
}
export type ValidatorFn = (document: DocumentData) => ValidationResult;
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