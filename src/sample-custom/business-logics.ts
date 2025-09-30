import * as admin from "firebase-admin";
// You should import from the path to the ember-flow package in your project
import {LogicConfig, LogicFn} from "../types";

const echoLogic: LogicFn = async (txnGet, action, sharedMap) => {
  const {timeCreated, eventContext: {docPath}, modifiedFields} = action;
  console.log(`Executing EchoLogic on document at ${docPath}...`);

  // Return the result of the logic function
  return {
    name: "EchoLogic",
    status: "finished",
    execTime: Date.now() - timeCreated.toMillis(),
    timeFinished: admin.firestore.Timestamp.now(),
    documents: [
      {
        action: "merge",
        dstPath: docPath,
        doc: modifiedFields,
        instructions: {},
        priority: "normal",
      },
    ],
  };
};


const logics: LogicConfig[] = [
  {
    name: "EchoLogic",
    actionTypes: ["create", "update"],
    modifiedFields: "all",
    entities: "all",
    logicFn: echoLogic,
    version: "1",
  },
  // more logics here
];

export {logics};
