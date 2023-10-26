import {
  LogicResultAction,
  LogicResultDoc,
  LogicResultDocPriority,
  ViewDefinition,
  ViewLogicFn,
} from "../types";
import {docPaths} from "../index";
import * as admin from "firebase-admin";
import {findMatchingDocPathRegex, hydrateDocPath} from "../utils/paths";

export function createViewLogicFn(viewDefinition: ViewDefinition): ViewLogicFn {
  return async (logicResultDoc: LogicResultDoc) => {
    const {
      srcProps,
      destEntity,
      destProp,
    } = viewDefinition;
    const {
      doc,
      instructions,
      dstPath: actualSrcPath,
      action,
    } = logicResultDoc;
    console.log(`Executing ViewLogic on document at ${actualSrcPath}...`);
    let destPaths: string[];
    const destDocPath = docPaths[destEntity];
    const docId = actualSrcPath.split("/").slice(-1)[0];
    if (destProp) {
      destPaths = await hydrateDocPath(destDocPath, {
        [destEntity]: {
          fieldName: `${destProp}.id`,
          operator: "==",
          value: docId,
        },
      });
    } else {
      const dehydratedPath = `${destDocPath.split("/").slice(0, -1).join("/")}/${docId}`;
      destPaths = await hydrateDocPath(dehydratedPath, {});
    }

    if (action === "delete") {
      const documents = destPaths.map((destPath) => {
        return {
          action: "delete" as LogicResultAction,
          dstPath: destPath,
          priority: "normal" as LogicResultDocPriority,
        };
      });
      return {
        name: `${destEntity}#${destProp} ViewLogic`,
        status: "finished",
        timeFinished: admin.firestore.Timestamp.now(),
        documents,
      };
    } else {
      const viewDoc: Record<string, any> = {};
      const viewInstructions: Record<string, string> = {};
      for (const srcProp of srcProps) {
        if (doc?.[srcProp]) {
          viewDoc[`${destProp ? `${destProp}.` : ""}${srcProp}`] = doc[srcProp];
        }
        if (instructions?.[srcProp]) {
          viewInstructions[`${destProp ? `${destProp}.` : ""}${srcProp}`] = instructions[srcProp];
        }
      }
      const documents = destPaths.map((destPath) => {
        return {
          action: "merge" as LogicResultAction,
          dstPath: destPath,
          doc: viewDoc,
          instructions: viewInstructions,
          priority: "low" as LogicResultDocPriority,
        };
      });

      return {
        name: `${destEntity}#${destProp} ViewLogic`,
        status: "finished",
        timeFinished: admin.firestore.Timestamp.now(),
        documents,
      };
    }
  };
}

export const syncPeerViews: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
  const {
    dstPath,
    doc,
    instructions,
  } = logicResultDoc;
  const {entity, regex} = findMatchingDocPathRegex(dstPath);
  if (!entity) {
    return {
      name: "SyncPeerViews",
      status: "error",
      message: `No matching entity found for path ${dstPath}`,
      timeFinished: admin.firestore.Timestamp.now(),
      documents: [],
    };
  }
  const docPath = docPaths[entity];

  let matchedGroups = dstPath.match(regex);
  const matchedValues = matchedGroups?.slice(1);
  matchedGroups = docPath.match(regex);
  const matchedKeys = matchedGroups?.slice(1);

  if (!matchedKeys || !matchedValues) {
    return {
      name: "SyncPeerViews",
      status: "error",
      message: `No matching keys or values found for path ${dstPath}`,
      timeFinished: admin.firestore.Timestamp.now(),
      documents: [],
    };
  }

  const userIdKey = matchedKeys[0];
  const userId = matchedValues[0];
  const entityIdKey = matchedKeys[matchedKeys.length-1];
  const entityId = matchedValues[matchedValues.length-1];
  // Create a map combining matchedKeys and matchedValues
  const matchedKeyValues = matchedKeys.reduce((acc, key, index) => {
    acc[key] = matchedValues[index];
    return acc;
  }, {} as Record<string, string>);

  // Create a dehydrated path from docPath replacing keys with values except for the first key and the last key
  const dehydratedPath = docPath.replace(/({[^/]+Id})/g, (match, ...args) => {
    const matchedKey = args[0];
    if (matchedKey === userIdKey || matchedKey === entityIdKey) {
      return matchedKey;
    }
    return matchedKeyValues[matchedKey];
  });

  const forSyncPaths = await hydrateDocPath(dehydratedPath, {
    [entity]: {
      fieldName: "@id",
      operator: "==",
      value: entityId,
    },
    user: {
      fieldName: "@id",
      operator: "!=",
      value: userId,
    },
  });

  // Loop through all the paths and create a document for each
  const documents = forSyncPaths.map((forSyncPath) => {
    return {
      action: "merge" as LogicResultAction,
      dstPath: forSyncPath,
      doc: doc,
      instructions: instructions,
      priority: "low" as LogicResultDocPriority,
    };
  });

  // Return the result with the hydrated documents
  return {
    name: "SyncPeerViews",
    status: "finished",
    timeFinished: admin.firestore.Timestamp.now(),
    documents,
  };
};
