import {
  LogicResult,
  LogicResultDoc,
  LogicResultDocAction,
  LogicResultDocPriority,
  ViewDefinition,
  ViewLogicFn,
} from "../types";
import {db, docPaths, VIEW_LOGICS_TOPIC, VIEW_LOGICS_TOPIC_NAME} from "../index";
import * as admin from "firebase-admin";
import {hydrateDocPath} from "../utils/paths";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {_mockable, distribute, expandConsolidateAndGroupByDstPath, runViewLogics} from "../index-utils";
import {pubsubUtils} from "../utils/pubsub";
import {reviveDateAndTimestamp} from "../utils/misc";

export function createViewLogicFn(viewDefinition: ViewDefinition): ViewLogicFn[] {
  const {
    srcEntity,
    srcProps,
    destEntity,
  } = viewDefinition;
  function formViewsPath(docId: string) {
    const srcDocPath = docPaths[srcEntity];
    const srcPath = srcDocPath.split("/").slice(0, -1).join("/") + "/" + docId;
    return `${srcPath}/@views/${docId}+${destEntity}`;
  }

  const srcToDstLogicFn: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
    async function buildViewsCollection() {
      const destDocPath = docPaths[destEntity];
      const docId = actualSrcPath.split("/").slice(-1)[0];
      const dehydratedPath = `${destDocPath.split("/").slice(0, -1).join("/")}/${docId}`;
      destPaths = await hydrateDocPath(dehydratedPath, {});
      for (const path of destPaths) {
        const docId = path.split("/").slice(-1)[0];
        const srcViewsPath = formViewsPath(docId);
        await db.doc(srcViewsPath).set({
          path,
          srcProps: srcProps.sort(),
          destEntity,
        });
      }
    }

    function syncDeleteToViews() {
      const documents = destPaths.map((destPath) => {
        return {
          action: "delete" as LogicResultDocAction,
          dstPath: destPath,
          priority: "normal" as LogicResultDocPriority,
        };
      });
      return {
        name: `${destEntity} ViewLogic`,
        status: "finished",
        timeFinished: admin.firestore.Timestamp.now(),
        documents,
      } as LogicResult;
    }

    function syncMergeToViews() {
      const now = admin.firestore.Timestamp.now();
      const viewDoc: Record<string, any> = {
        "updatedByViewDefinitionAt": now,
      };
      const viewInstructions: Record<string, string> = {};
      for (const srcProp of srcProps) {
        if (doc?.[srcProp]) {
          viewDoc[srcProp] = doc[srcProp];
        }
        if (instructions?.[srcProp]) {
          viewInstructions[srcProp] = instructions[srcProp];
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
        name: `${destEntity} ViewLogic`,
        status: "finished",
        timeFinished: now,
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

    let query;
    if (action === "delete") {
      query = db.doc(actualSrcPath)
        .collection("@views")
        .where("destEntity", "==", destEntity);
    } else {
      query = db.doc(actualSrcPath)
        .collection("@views")
        .where("srcProps", "array-contains-any", modifiedFields)
        .where("destEntity", "==", destEntity);
    }
    const viewPaths = (await query.get()).docs.map((doc) => doc.data());

    let destPaths = viewPaths.map((viewPath) => viewPath.path);

    if (viewPaths.length === 0) {
      // Check if the src doc has "@viewsAlreadyBuilt" field
      const srcRef = db.doc(actualSrcPath);
      const isViewsAlreadyBuilt = (await srcRef.get()).data()?.[`@viewsAlreadyBuilt+${destEntity}`];
      if (!isViewsAlreadyBuilt) {
        await buildViewsCollection();
        await srcRef.update({[`@viewsAlreadyBuilt+${destEntity}`]: true});
      }
    }

    for (const viewPath of viewPaths) {
      const {srcProps: viewPathSrcProps, path} = viewPath;
      const sortedSrcProps = srcProps.sort();
      if (viewPathSrcProps.join(",") === sortedSrcProps.join(",")) {
        continue;
      }

      const docId = path.split("/").slice(-1)[0];
      const srcViewsPath = formViewsPath(docId);
      await db.doc(srcViewsPath).update({srcProps: sortedSrcProps});
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
          srcProps: srcProps.sort(),
          destEntity,
        },
      };
      logicResult.documents.push(viewResultDoc);
    }
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
    const start = performance.now();
    const viewLogicResults = await runViewLogics(logicResultDoc);
    const end = performance.now();
    const execTime = end - start;
    const distributeFnLogicResult: LogicResult = {
      name: "runViewLogics",
      status: "finished",
      documents: [],
      execTime: execTime,
    };
    await _mockable.createMetricExecution([...viewLogicResults, distributeFnLogicResult]);

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
