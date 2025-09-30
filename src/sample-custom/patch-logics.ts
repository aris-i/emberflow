import {PatchLogicConfig, PatchLogicFn} from "../types";
import * as admin from "firebase-admin";

const samplePatchLogic: PatchLogicFn = async (dstPath, data) => {
  console.log(`Executing patch logic on document at ${dstPath}...`);

  const {fullName} = data;
  const [firstName, lastName] = fullName.split(" ");

  // Return the result of the logic function
  return {
    name: "samplePatchLogic",
    status: "finished",
    execTime: Date.now(),
    timeFinished: admin.firestore.Timestamp.now(),
    documents: [
      {
        action: "merge",
        dstPath: dstPath,
        doc: {firstName, lastName},
        instructions: {fullName: "del"},
        priority: "normal",
      },
    ],
  };
};


export const patchLogicConfigs: PatchLogicConfig[] = [
  {
    name: "samplePatchLogic",
    entity: "user",
    patchLogicFn: samplePatchLogic,
    version: "1",
  },
];
