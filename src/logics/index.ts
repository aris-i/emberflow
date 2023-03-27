import {Action, LogicFn, LogicResultAction, LogicResultDoc, ViewDefinition} from "../types";
import {docPaths, hydrateDocPath} from "../index";
import * as admin from "firebase-admin";

export function createViewLogicFn(viewDefinition: ViewDefinition): LogicFn {
  return async (action: Action) => {
    const {srcProps, destEntity, destProp} = viewDefinition;
    const {document, path, actionType} = action;
    console.log(`Executing ViewLogic on document at ${path}...`);
    const docId = path.split("/").slice(-1)[0];

    const destDocPath = docPaths[destEntity];
    const destPaths = await hydrateDocPath(
      destDocPath,
      {
        [destEntity]: {
          fieldName: destProp ? `${destProp}.id` : "id",
          operator: "==",
          value: docId,
        },
      }
    );

    let documents: LogicResultDoc[];
    if (actionType === "delete") {
      documents = destPaths.map((destPath) => {
        return {
          action: "delete",
          dstPath: path,
        };
      });
    } else {
      const viewDoc: Record<string, any> = {};
      for (const srcProp of srcProps) {
        if (document[srcProp]) {
          viewDoc[`${destProp ? `${destProp}.` : ""}${srcProp}`] = document[srcProp];
        }
      }
      documents = destPaths.map((destPath) => {
        return {
          action: "merge" as LogicResultAction,
          dstPath: destPath,
          doc: viewDoc,
        };
      });
    }

    return {
      name: `${destEntity}#${destProp} ViewLogic`,
      status: "finished",
      timeFinished: admin.firestore.Timestamp.now(),
      documents,
    };
  };
}
