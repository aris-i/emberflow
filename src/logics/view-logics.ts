import {
  LogicResultAction,
  LogicResultDoc,
  LogicResultDocPriority,
  ViewDefinition,
  ViewLogicFn,
} from "../types";
import {docPaths, pubsub, PEER_SYNC_TOPIC_NAME, VIEW_LOGICS_TOPIC_NAME} from "../index";
import * as admin from "firebase-admin";
import {hydrateDocPath} from "../utils/paths";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {
  distribute,
  distributeLater,
  expandConsolidateAndGroupByDstPath,
  groupDocsByUserAndDstPath,
  runViewLogics,
} from "../index-utils";
import {pubsubUtils} from "../utils/pubsub";
import {convertStringDate} from "../utils/misc";

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
          fieldName: `${destProp}.@id`,
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
        if (destProp) {
          return {
            action: "merge" as LogicResultAction,
            dstPath: destPath,
            doc: {
              [destProp]: admin.firestore.FieldValue.delete(),
            },
            priority: "normal" as LogicResultDocPriority,
          };
        } else {
          return {
            action: "delete" as LogicResultAction,
            dstPath: destPath,
            priority: "normal" as LogicResultDocPriority,
          };
        }
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

export const syncPeerViews = async (logicResultDoc: LogicResultDoc) => {
  const {
    dstPath,
    doc,
    instructions,
  } = logicResultDoc;
  const splits = dstPath.split("/");
  const userId = splits[1];
  splits[1] = "{userId}";
  const dehydratedPath = splits.join("/");

  const forSyncPaths = await hydrateDocPath(dehydratedPath, {
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

export async function queueRunViewLogics(userLogicResultDocs: LogicResultDoc[]) {
  const topic = pubsub.topic(VIEW_LOGICS_TOPIC_NAME);

  try {
    for (const userLogicResultDoc of userLogicResultDocs) {
      if (userLogicResultDoc.action === "create") {
        // Since this is newly created, this means there's no existing view to sync
        continue;
      }
      const messageId = await topic.publishMessage({json: userLogicResultDoc});
      console.log(`Message ${messageId} published.`);
    }
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error(`Received error while publishing: ${error.message}`);
    } else {
      console.error("An unknown error occurred during publishing");
    }
    throw error;
  }
}

export async function onMessageViewLogicsQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(VIEW_LOGICS_TOPIC_NAME, event.id)) {
    console.log("Skipping duplicate message");
    return;
  }

  try {
    const userLogicResultDoc = convertStringDate(event.data.message.json) as LogicResultDoc;
    const userId = userLogicResultDoc.dstPath.split("/")[1];
    console.log("Received user logic result doc:", userLogicResultDoc);

    console.info("Running View Logics");
    const viewLogicResults = await runViewLogics(userLogicResultDoc);
    const viewLogicResultDocs = viewLogicResults.map((result) => result.documents).flat();
    const dstPathViewLogicDocsMap: Map<string, LogicResultDoc[]> = await expandConsolidateAndGroupByDstPath(viewLogicResultDocs);
    const {userDocsByDstPath, otherUsersDocsByDstPath} = groupDocsByUserAndDstPath(dstPathViewLogicDocsMap, userId);

    console.info("Distributing View Logic Results");
    await distribute(userDocsByDstPath);
    await distributeLater(otherUsersDocsByDstPath);

    console.info("Queue for Peer Sync");
    await queueForPeerSync(userLogicResultDoc);
    await pubsubUtils.trackProcessedIds(VIEW_LOGICS_TOPIC_NAME, event.id);
    return "Processed view logics";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}

export const queueForPeerSync = async (...userLogicResultDocs: LogicResultDoc[]) => {
  const topic = pubsub.topic(PEER_SYNC_TOPIC_NAME);

  try {
    for (const userLogicResultDoc of userLogicResultDocs) {
      const {dstPath} = userLogicResultDoc;
      if (!dstPath.startsWith("users")) {
        // Peer sync only applies to user documents
        continue;
      }
      const messageId = await topic.publishMessage({json: userLogicResultDoc});
      console.log(`Message ${messageId} published.`);
    }
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error(`Received error while publishing: ${error.message}`);
    } else {
      console.error("An unknown error occurred during publishing");
    }
    throw error;
  }
};

export async function onMessagePeerSyncQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(PEER_SYNC_TOPIC_NAME, event.id)) {
    console.log("Skipping duplicate message");
    return;
  }

  try {
    const userLogicResultDoc = convertStringDate(event.data.message.json) as LogicResultDoc;
    console.log("Received user logic result doc:", userLogicResultDoc);

    console.info("Running Peer Sync");
    const logicResult = await syncPeerViews(userLogicResultDoc);
    const logicResultDocs = logicResult.documents;
    const dstPathViewLogicDocsMap: Map<string, LogicResultDoc[]> = await expandConsolidateAndGroupByDstPath(logicResultDocs);

    console.info("Distributing Peer Sync Docs");
    await distributeLater(dstPathViewLogicDocsMap);
    await pubsubUtils.trackProcessedIds(PEER_SYNC_TOPIC_NAME, event.id);
    return "Processed peer sync";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}
