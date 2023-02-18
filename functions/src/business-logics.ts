import {Entity} from "./db-structure";
import {LogicConfig, LogicFn} from "./types";
import * as admin from "firebase-admin";
import {firestore} from "firebase-admin";
import DocumentData = firestore.DocumentData;

const echoLogic: LogicFn = async (action) => {
  const {document, timeCreated, path, modifiedFields} = action;
  console.log(`Executing EchoLogic on document at ${path}...`);

  const updatedDoc : DocumentData= {};

  // Copy modified fields of document's @form to the document
  if (document["@form"]) {
    for (const field of modifiedFields) {
      if (document["@form"][field]) {
        updatedDoc[field] = document["@form"][field];
      }
    }
  }

  // Return the result of the logic function
  return {
    name: "EchoLogic",
    status: "finished",
    execTime: Date.now() - timeCreated.toMillis(),
    timeFinished: admin.firestore.Timestamp.now(),
    documents: [
      {
        dstPath: path,
        doc: updatedDoc,
        instructions: {},
      },
    ],
  };
};


const logics: LogicConfig[] = [
  {
    name: "EchoLogic",
    actionType: ["create", "update"],
    modifiedFields: ["name", "email"],
    docPaths: [Entity.User],
    logicFn: echoLogic,
  },
  // more logics here
];

export {logics};
