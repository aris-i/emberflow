import {
  LogicResult,
  LogicResultDoc,
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
  function formViewDocId(path: string) {
    let viewDocId = path.replace(/\//g, "+");
    if (viewDocId.startsWith("+")) {
      viewDocId = viewDocId.slice(1);
    }
    return viewDocId;
  }

  function formViewsPath(path: string) {
    const viewDocId = formViewDocId(path);
    const docId = path.split("/").slice(-1)[0];
    const srcDocPath = docPaths[srcEntity];
    const srcPath = srcDocPath.split("/").slice(0, -1).join("/") + "/" + docId;
    return `${srcPath}/@views/${viewDocId}`;
  }

  const srcToDstLogicFn: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
    const {
      doc,
      instructions,
      dstPath: actualSrcPath,
      action,
    } = logicResultDoc;
    console.log(`Executing ViewLogic on document at ${actualSrcPath}...`);

    if (action === "create") {
      const srcRef = db.doc(actualSrcPath);
      await srcRef.update({[`@viewsAlreadyBuilt+${destEntity}`]: true});

      return {
        name: `${destEntity} ViewLogic`,
        status: "finished",
        timeFinished: admin.firestore.Timestamp.now(),
        documents: [],
      } as LogicResult;
    }

    async function populateDestPathsAndBuildViewsCollection() {
      const destDocPath = docPaths[destEntity];
      const docId = actualSrcPath.split("/").slice(-1)[0];
      const dehydratedPath = `${destDocPath.split("/").slice(0, -1).join("/")}/${docId}`;
      destPaths = await hydrateDocPath(dehydratedPath, {});

      if (action === "delete") return;

      for (const path of destPaths) {
        const srcViewsPath = formViewsPath(path);
        await db.doc(srcViewsPath).set({
          path,
          srcProps: srcProps.sort(),
          destEntity,
        });
      }
    }

    function syncDeleteToDestPaths() {
      const documents: LogicResultDoc[] = destPaths.map((destPath) => {
        return {
          action: "delete",
          dstPath: destPath,
          priority: "normal",
          skipRunViewLogics: true,
        };
      });
      return {
        name: `${destEntity} ViewLogic`,
        status: "finished",
        timeFinished: admin.firestore.Timestamp.now(),
        documents,
      } as LogicResult;
    }

    function syncMergeToDestPaths() {
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
      const documents: LogicResultDoc[] = destPaths.map((destPath) => {
        return {
          action: "merge",
          dstPath: destPath,
          doc: viewDoc,
          instructions: viewInstructions,
          priority: "low",
          skipRunViewLogics: true,
        };
      });

      return {
        name: `${destEntity} ViewLogic`,
        status: "finished",
        timeFinished: now,
        documents,
      } as LogicResult;
    }

    async function syncViewSrcPropsIfDifferentFromViewDefinition() {
      for (const doc of viewPathDocs) {
        const {srcProps: viewPathSrcProps, path} = doc.data();
        const viewDocId = formViewDocId(path);
        const srcViewsPath = formViewsPath(path);
        const sortedSrcProps = srcProps.sort();

        if (viewDocId !== doc.id) {
          await doc.ref.delete();
          await db.doc(srcViewsPath).set({
            path,
            srcProps: sortedSrcProps,
            destEntity,
          });
          continue;
        }

        if (viewPathSrcProps.join(",") === sortedSrcProps.join(",")) {
          continue;
        }

        await db.doc(srcViewsPath).update({srcProps: sortedSrcProps});
      }
    }

    const modifiedFields = [
      ...Object.keys(doc || {}),
      ...Object.keys(instructions || {}),
    ];

    let query;
    if (action === "delete") {
      console.debug("action === delete");
      query = db.doc(actualSrcPath)
        .collection("@views")
        .where("destEntity", "==", destEntity);
    } else {
      query = db.doc(actualSrcPath)
        .collection("@views")
        .where("srcProps", "array-contains-any", modifiedFields)
        .where("destEntity", "==", destEntity);
    }
    const viewPathDocs = (await query.get()).docs;

    let destPaths = viewPathDocs.map((doc) => doc.data().path);

    if (viewPathDocs.length === 0) {
      console.debug("viewPathDocs.length === 0");
      // Check if the src doc has "@viewsAlreadyBuilt" field
      const srcRef = db.doc(actualSrcPath);
      let isViewsAlreadyBuilt;
      if (action === "delete") {
        console.debug("action === delete", "doc", doc);
        isViewsAlreadyBuilt = doc?.[`@viewsAlreadyBuilt+${destEntity}`];
      } else {
        isViewsAlreadyBuilt = (await srcRef.get()).data()?.[`@viewsAlreadyBuilt+${destEntity}`];
      }
      console.debug("isViewsAlreadyBuilt", isViewsAlreadyBuilt);
      if (!isViewsAlreadyBuilt) {
        await populateDestPathsAndBuildViewsCollection();
        if (action === "merge") {
          await srcRef.update({[`@viewsAlreadyBuilt+${destEntity}`]: true});
        }
      }
    }

    await syncViewSrcPropsIfDifferentFromViewDefinition();

    if (action === "delete") {
      return syncDeleteToDestPaths();
    } else {
      return syncMergeToDestPaths();
    }
  };

  const dstToSrcLogicFn: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
    const logicResult: LogicResult = {
      name: "ViewLogic Dst to Src",
      status: "finished",
      documents: [],
    };
    const srcViewsPath = formViewsPath(logicResultDoc.dstPath);
    if (srcViewsPath.includes("{")) {
      console.error("Cannot run Dst to Src ViewLogic on a path with a placeholder");
      return logicResult;
    }

    if (logicResultDoc.action === "delete") {
      const viewResultDoc: LogicResultDoc = {
        action: "delete",
        dstPath: srcViewsPath,
        skipRunViewLogics: true,
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
        skipRunViewLogics: true,
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
