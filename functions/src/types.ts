import {Entity} from "./db-structure";
import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;

export type Action = {
    actionType: string;
    path: string;
    document: FirebaseFirestore.DocumentData;
    modifiedFields: string[];
    status: "new" | "processing" | "processed" | "processed-with-errors";
    timeCreated: FirebaseFirestore.Timestamp;
};

type LogicResultDoc = {
    dstPath: string;
    doc: FirebaseFirestore.DocumentData | string | null;
    instructions: { [key: string]: string };
};
type LogicResult = {
    name: string;
    status: "finished" | "error";
    message?: string;
    execTime: number;
    timeFinished: Timestamp;
    documents: LogicResultDoc[];
};
export type LogicFn = (action: Action) => Promise<LogicResult>;
export type LogicConfig = {
    name: string;
    actionType: ("create" | "update" | "delete")[];
    modifiedFields: string[];
    docPaths: Entity[];
    logicFn: LogicFn;
};
