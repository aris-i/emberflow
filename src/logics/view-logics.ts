import {
  LogicResult,
  LogicResultDoc,
  ViewDefinition,
  ViewLogicFn,
} from "../types";
import {db, docPaths, docPathsRegex, VIEW_LOGICS_TOPIC, VIEW_LOGICS_TOPIC_NAME} from "../index";
import * as admin from "firebase-admin";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {_mockable, distributeFnNonTransactional, expandConsolidateAndGroupByDstPath, runViewLogics} from "../index-utils";
import {pubsubUtils} from "../utils/pubsub";
import {reviveDateAndTimestamp} from "../utils/misc";
import {getDestPropAndDestPropId} from "../utils/paths";

export function createViewLogicFn(viewDefinition: ViewDefinition): ViewLogicFn[] {
  const {
    srcEntity,
    srcProps,
    destEntity,
    destProp,
  } = viewDefinition;

  const logicName = `${destEntity}${destProp ? `#${destProp.name}` : ""}`;

  function formViewDocId(viewDstPath: string) {
    let viewDocId = viewDstPath.replace(/[/#]/g, "+");
    if (viewDocId.startsWith("+")) {
      viewDocId = viewDocId.slice(1);
    }
    return viewDocId;
  }

  function formAtViewsPath(viewDstPath: string, srcPath: string) {
    const viewDocId = formViewDocId(viewDstPath);
    return `${srcPath}/@views/${viewDocId}`;
  }

  const srcToDstLogicFn: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
    const {
      doc,
      instructions,
      dstPath: srcPath,
      action,
    } = logicResultDoc;
    console.log(`Executing ViewLogic on document at ${srcPath}...`);

    function syncDeleteToViewDstPaths() {
      const documents: LogicResultDoc[] = viewDstPathDocs.map((viewDstPathDoc) => {
        return {
          action: "delete",
          dstPath: viewDstPathDoc.data().path,
        };
      });
      return {
        name: `${logicName} ViewLogic`,
        status: "finished",
        timeFinished: admin.firestore.Timestamp.now(),
        documents,
      } as LogicResult;
    }

    function syncMergeToDestPaths() {
      const now = admin.firestore.Timestamp.now();
      const viewDoc: Record<string, any> = {
        "@updatedByViewDefinitionAt": now,
      };
      const viewInstructions: Record<string, string> = {};
      for (const srcProp of srcProps) {
        if (doc?.[srcProp] !== undefined) {
          viewDoc[srcProp] = doc[srcProp];
        }
        if (instructions?.[srcProp]) {
          viewInstructions[srcProp] = instructions[srcProp];
        }
      }
      const documents: LogicResultDoc[] = viewDstPathDocs.map((viewDstPathDoc) => {
        return {
          action: "merge",
          dstPath: viewDstPathDoc.data().path,
          doc: viewDoc,
          instructions: viewInstructions,
        };
      });

      return {
        name: `${logicName} ViewLogic`,
        status: "finished",
        timeFinished: now,
        documents,
      } as LogicResult;
    }

    async function syncAtViewsSrcPropsIfDifferentFromViewDefinition() {
      for (const doc of viewDstPathDocs) {
        const {srcProps: atViewsSrcProps, path: atViewsDstPath} = doc.data();
        const viewDocId = formViewDocId(atViewsDstPath);
        const srcAtViewsPath = doc.ref.path;
        const sortedSrcProps = srcProps.sort();

        if (viewDocId !== doc.id) {
          await doc.ref.delete();
          await db.doc(srcAtViewsPath).set({
            path: atViewsDstPath,
            srcProps: sortedSrcProps,
            destEntity,
            ...(destProp ? {destProp: destProp.name} : {}),
          });
          continue;
        }

        if (atViewsSrcProps.join(",") === sortedSrcProps.join(",")) {
          continue;
        }

        await db.doc(srcAtViewsPath).update({srcProps: sortedSrcProps});
      }
    }

    const modifiedFields = [
      ...Object.keys(doc || {}),
      ...Object.keys(instructions || {}),
    ];

    let query = db.doc(srcPath)
      .collection("@views")
      .where("destEntity", "==", destEntity);
    if (action === "delete") {
      console.debug("action === delete");
    } else {
      query = query.where("srcProps", "array-contains-any", modifiedFields);
    }
    if (destProp) {
      query = query.where("destProp", "==", destProp.name);
    }
    const viewDstPathDocs = (await query.get()).docs;

    await syncAtViewsSrcPropsIfDifferentFromViewDefinition();

    if (action === "delete") {
      return syncDeleteToViewDstPaths();
    } else {
      return syncMergeToDestPaths();
    }
  };

  const dstToSrcLogicFn: ViewLogicFn = async (logicResultDoc: LogicResultDoc) => {
    const logicResult: LogicResult = {
      name: `${logicName} Dst-to-Src`,
      status: "finished",
      documents: [],
    };
    const {
      dstPath,
      action,
      doc,
    } = logicResultDoc;
    const srcDocId = doc?.["@id"];
    if (!srcDocId) {
      console.error("Document does not have an @id attribute");
      logicResult.status = "error";
      logicResult.message = "Document does not have an @id attribute";
      return logicResult;
    }

    function formSrcPath() {
      const srcDocPath = docPaths[srcEntity];
      let srcPath = srcDocPath.split("/").slice(0, -1).join("/") + "/" + srcDocId;

      const destDocPath = docPaths[destEntity];
      const destDocPathRegex = docPathsRegex[destEntity];
      const destDocPathMatches = dstPath.split("#")[0].match(destDocPathRegex);

      // let's create a map of the placeholders with their matching values from dstPath
      const dstPathKeyValuesMap: Record<string, string> = {};
      if (destDocPathMatches) {
        const destDocPathKeys = destDocPath.match(/{([^}]+)}/g);
        if (destDocPathKeys) {
          for (let i = 0; i < destDocPathKeys.length; i++) {
            const key = destDocPathKeys[i].replace(/[{}]/g, "");
            dstPathKeyValuesMap[key] = destDocPathMatches[i + 1];
          }
        }
      }

      const srcDocPathKeys = srcDocPath.match(/{([^}]+)}/g);
      if (srcDocPathKeys) {
        for (const srcDocPathKey of srcDocPathKeys) {
          const key = srcDocPathKey.replace(/[{}]/g, "");
          const value = doc?.[key] || dstPathKeyValuesMap[key];
          if (value) {
            srcPath = srcPath.replace(srcDocPathKey, value);
          }
        }
      }
      return srcPath;
    }

    const srcPath = formSrcPath();
    if (srcPath.includes("{")) {
      console.error("srcPath should not have a placeholder");
      logicResult.status = "error";
      logicResult.message = "srcPath should not have a placeholder";
      return logicResult;
    }

    const srcAtViewsPath = formAtViewsPath(dstPath, srcPath);

    const {destProp, destPropId} = getDestPropAndDestPropId(dstPath);
    const isArrayMap = !!destPropId;

    if (action === "delete") {
      logicResult.documents.push({
        action: "delete",
        dstPath: srcAtViewsPath,
        skipRunViewLogics: true,
      });
      if (destProp && isArrayMap) {
        const dstBasePath = dstPath.split("#")[0];
        logicResult.documents.push({
          action: "merge",
          dstPath: dstBasePath,
          instructions: {
            [`@${destProp}`]: `arr-(${srcDocId})`,
          },
          skipRunViewLogics: true,
        });
      }
    } else {
      logicResult.documents.push({
        action: "create",
        dstPath: srcAtViewsPath,
        doc: {
          path: logicResultDoc.dstPath,
          srcProps: srcProps.sort(),
          destEntity,
          ...(destProp ? {destProp} : {}),
        },
        skipRunViewLogics: true,
      });
      if (destProp && isArrayMap) {
        const dstBasePath = dstPath.split("#")[0];
        logicResult.documents.push({
          action: "merge",
          dstPath: dstBasePath,
          instructions: {
            [`@${destProp}`]: `arr+(${srcDocId})`,
          },
          skipRunViewLogics: true,
        });
      }
    }

    return logicResult;
  };

  return [srcToDstLogicFn, dstToSrcLogicFn];
}

export async function queueRunViewLogics(logicResultDoc: LogicResultDoc) {
  try {
    const messageId = await VIEW_LOGICS_TOPIC.publishMessage({json: logicResultDoc});
    console.log(`queueRunViewLogics: Message ${messageId} published.`);
    console.debug(`queueRunViewLogics: ${logicResultDoc}`);
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
    await distributeFnNonTransactional(dstPathViewLogicDocsMap);

    await pubsubUtils.trackProcessedIds(VIEW_LOGICS_TOPIC_NAME, event.id);
    return "Processed view logics";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}
