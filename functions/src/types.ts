import {firestore} from "firebase-admin";
import {Entity} from "./custom/db-structure";
import Timestamp = firestore.Timestamp;
import DocumentData = firestore.DocumentData;

export type Action = {
    actionType: string;
    path: string;
    document: FirebaseFirestore.DocumentData;
    modifiedFields?: string[];
    status: "new" | "processing" | "processed" | "processed-with-errors";
    timeCreated: FirebaseFirestore.Timestamp;
    message?: string
};

export type LogicResultDoc = {
    dstPath: string;
    doc: FirebaseFirestore.DocumentData | string | null;
    instructions?: { [key: string]: string };
};
export type LogicResult = {
    name: string;
    status: "finished" | "error";
    message?: string;
    execTime: number;
    timeFinished: Timestamp;
    documents: LogicResultDoc[];
};
export type LogicFn = (action: Action) => Promise<LogicResult>;
export type ActionType = "create" | "update" | "delete";
export type LogicConfig = {
    name: string;
    actionTypes: ActionType[] | "all";
    modifiedFields: string[] | "all"
    entities: Entity[] | "all";
    logicFn: LogicFn;
};

export type SecurityStatus = "allowed" | "rejected"
export type SecurityResult = {
    status: SecurityStatus;
    message?: string;
};
export type SecurityFn = (entity: Entity, doc: DocumentData, actionType: ActionType, modifiedFields?: string[]) => Promise<SecurityResult>;
export type SecurityConfig = Record<Entity, SecurityFn>;
export type ValidationResult = {
    [key: string]: string[];
}
export type ValidatorFn = (document: DocumentData) => ValidationResult;
