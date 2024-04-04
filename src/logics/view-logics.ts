import {
  LogicResultDocAction,
  LogicResultDoc,
  LogicResultDocPriority,
  ViewDefinition,
  ViewLogicFn, LogicResult,
} from "../types";
import {db, docPaths, VIEW_LOGICS_TOPIC, VIEW_LOGICS_TOPIC_NAME} from "../index";
import * as admin from "firebase-admin";
import {hydrateDocPath} from "../utils/paths";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {
  distribute,
  expandConsolidateAndGroupByDstPath,
  runViewLogics,
} from "../index-utils";
import {pubsubUtils} from "../utils/pubsub";
import {reviveDateAndTimestamp} from "../utils/misc";

export function createViewLogicFn(viewDefinition: ViewDefinition): ViewLogicFn[] {
  const {
    srcEntity,
    srcProps,
    destEntity,
    destProp,
  } = viewDefinition;
  function formViewsPath(docId: string) {
    const srcDocPath = docPaths[srcEntity];
    const srcPath = srcDocPath.split("/").slice(0, -1).join("/") + "/" + docId;
    const srcViewsPath = `${srcPath}/@views/${docId}+${destEntity}${destProp ? `#${destProp}` : ""}`;
    return srcViewsPath;
  }

  const srcToDstLogicFn: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
    async function buildViewsCollection() {
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
      for (const path of destPaths) {
        const docId = path.split("/").slice(-1)[0];
        const srcViewsPath = formViewsPath(docId);
        await db.doc(srcViewsPath).set({
          path,
          srcProps,
        });
      }
    }

    function syncDeleteToViews() {
      const documents = destPaths.map((destPath) => {
        if (destProp) {
          return {
            action: "merge" as LogicResultDocAction,
            dstPath: destPath,
            doc: {
              [destProp]: admin.firestore.FieldValue.delete(),
            },
            priority: "normal" as LogicResultDocPriority,
          };
        } else {
          return {
            action: "delete" as LogicResultDocAction,
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
      } as LogicResult;
    }

    function syncMergeToViews() {
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
          action: "merge" as LogicResultDocAction,
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
      } as LogicResult;
    }

    const {
      doc,
      instructions,
      dstPath: actualSrcPath,
      action,
    } = logicResultDoc;
    const modifiedFields = [
      ...Object.keys(doc || {}),
      ...Object.keys(instructions || {}),
    ];
    console.log(`Executing ViewLogic on document at ${actualSrcPath}...`);
    const viewPaths = (await db.doc(actualSrcPath)
      .collection("@views")
      .where("srcProps", "array-contains-any", modifiedFields)
      .get()).docs.map((doc) => doc.data());

    let destPaths = viewPaths.map((viewPath) => viewPath.path);

    if (viewPaths.length === 0) {
      // Check if the src doc has "@viewsAlreadyBuilt" field
      const srcRef = db.doc(actualSrcPath);
      const isViewsAlreadyBuilt = (await srcRef.get()).data()?.["@viewsAlreadyBuilt"];
      if (!isViewsAlreadyBuilt) {
        await buildViewsCollection();
        await srcRef.update({"@viewsAlreadyBuilt": true});
      }
    }

    for (const viewPath of viewPaths) {
      const {srcProps: viewPathSrcProps} = viewPath;
      if (viewPathSrcProps.join(",") === srcProps.sort().join(",")) {
        continue;
      }
      // Update the srcProps of View given @id
    }

    if (action === "delete") {
      return syncDeleteToViews();
    } else {
      return syncMergeToViews();
    }
  };

  const dstToSrcLogicFn: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
    const logicResult: LogicResult = {
      name: "ViewLogic Dst to Src",
      status: "finished",
      documents: [],
    };
    const docId = logicResultDoc.dstPath.split("/").slice(-1)[0];
    const srcViewsPath = formViewsPath(docId);
    if (srcViewsPath.includes("{")) {
      console.error("Cannot run Dst to Src ViewLogic on a path with a placeholder");
      return logicResult;
    }

    if (logicResultDoc.action === "delete") {
      const viewResultDoc: LogicResultDoc = {
        action: "delete",
        dstPath: srcViewsPath,
      };
      logicResult.documents.push(viewResultDoc);
    } else {
      const viewResultDoc: LogicResultDoc = {
        action: "create",
        dstPath: srcViewsPath,
        doc: {
          path: logicResultDoc.dstPath,
          srcProps,
        },
      };
      logicResult.documents.push(viewResultDoc);
    }
    // TODO:  Handle changing of dbStructure srcProps
    return logicResult;
  };

  return [srcToDstLogicFn, dstToSrcLogicFn];
}

export async function queueRunViewLogics(logicResultDoc: LogicResultDoc) {
  try {
    const messageId = await VIEW_LOGICS_TOPIC.publishMessage({json: logicResultDoc});
    console.log(`Message ${messageId} published.`);
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
    const logicResultDoc = reviveDateAndTimestamp(event.data.message.json) as LogicResultDoc;
    console.log("Received logic result doc:", logicResultDoc);

    console.info("Running View Logics");
    const viewLogicResults = await runViewLogics(logicResultDoc);
    const viewLogicResultDocs = viewLogicResults.map((result) => result.documents).flat();
    const dstPathViewLogicDocsMap: Map<string, LogicResultDoc[]> = await expandConsolidateAndGroupByDstPath(viewLogicResultDocs);

    console.info("Distributing View Logic Results");
    await distribute(dstPathViewLogicDocsMap);

    await pubsubUtils.trackProcessedIds(VIEW_LOGICS_TOPIC_NAME, event.id);
    return "Processed view logics";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}
