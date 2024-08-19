import {
  EntityCondition,
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
    destProp,
  } = viewDefinition;

  const logicName = `${destEntity}${destProp ? `#${destProp.name}` : ""}`;
  const flagName = `@viewsAlreadyBuilt+${logicName}`;

  function formViewDocId(viewDstPath: string) {
    let viewDocId = viewDstPath.replace(/\//g, "+");
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

    if (action === "create") {
      const srcRef = db.doc(srcPath);
      await srcRef.update({[flagName]: true});

      return {
        name: `${logicName} ViewLogic`,
        status: "finished",
        timeFinished: admin.firestore.Timestamp.now(),
        documents: [],
      } as LogicResult;
    }

    async function populateViewDstPathsAndBuildViewsCollection() {
      const destDocPath = docPaths[destEntity];
      const srcDocId = srcPath.split("/").slice(-1)[0];
      let dehydratedPath: string;
      const entityCondition: EntityCondition = {};
      if (!destProp) {
        dehydratedPath = `${destDocPath.split("/").slice(0, -1).join("/")}/${srcDocId}`;
      } else {
        dehydratedPath = destDocPath;
        if (destProp.type === "array-map") {
          entityCondition[destEntity] = {
            fieldName: `${destProp.name}.${srcDocId}`,
            operator: "!=",
            value: null,
          };
        } else {
          entityCondition[destEntity] = {
            fieldName: `${destProp.name}.@id`,
            operator: "==",
            value: srcDocId,
          };
        }
      }
      viewDstPaths = await hydrateDocPath(dehydratedPath, entityCondition);
      if (destProp) {
        if (destProp.type === "array-map") {
          viewDstPaths = viewDstPaths.map((path) => `${path}#${destProp.name}[${srcDocId}]`);
        } else {
          viewDstPaths = viewDstPaths.map((path) => `${path}#${destProp.name}`);
        }
      }

      if (action === "delete") {
        console.log("No need to create @views collection since it will be deleted anyways");
        return;
      }

      for (const viewDstPath of viewDstPaths) {
        const srcAtViewsPath = formAtViewsPath(viewDstPath, srcPath);
        await db.doc(srcAtViewsPath).set({
          path: viewDstPath,
          srcProps: srcProps.sort(),
          destEntity,
          ...(destProp ? {destProp: destProp.name} : {}),
        });
      }
    }

    function syncDeleteToViewDstPaths() {
      const documents: LogicResultDoc[] = viewDstPaths.map((viewDstPath) => {
        return {
          action: "delete",
          dstPath: viewDstPath,
          skipRunViewLogics: true,
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
        if (doc?.[srcProp]) {
          viewDoc[srcProp] = doc[srcProp];
        }
        if (instructions?.[srcProp]) {
          viewInstructions[srcProp] = instructions[srcProp];
        }
      }
      const documents: LogicResultDoc[] = viewDstPaths.map((viewDstPath) => {
        return {
          action: "merge",
          dstPath: viewDstPath,
          doc: viewDoc,
          instructions: viewInstructions,
          skipRunViewLogics: true,
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

    let query;
    if (action === "delete") {
      console.debug("action === delete");
      query = db.doc(srcPath)
        .collection("@views")
        .where("destEntity", "==", destEntity);
    } else {
      query = db.doc(srcPath)
        .collection("@views")
        .where("srcProps", "array-contains-any", modifiedFields)
        .where("destEntity", "==", destEntity);
    }
    if (destProp) {
      query = query.where("destProp", "==", destProp.name);
    }
    const viewDstPathDocs = (await query.get()).docs;

    let viewDstPaths = viewDstPathDocs.map((doc) => doc.data().path);

    if (viewDstPathDocs.length === 0) {
      console.debug("viewDstPathDocs.length === 0");
      // Check if the src doc has "@viewsAlreadyBuilt" field
      const srcRef = db.doc(srcPath);
      let isViewsAlreadyBuilt;
      let dateCreated;
      if (action === "delete") {
        console.debug("action === delete", "doc", doc);
        isViewsAlreadyBuilt = doc?.[flagName];
        dateCreated = doc?.["@dateCreated"];
      } else {
        const data = (await srcRef.get()).data();
        isViewsAlreadyBuilt = data?.[flagName];
        dateCreated = data?.["@dateCreated"];
      }
      console.debug("isViewsAlreadyBuilt", isViewsAlreadyBuilt);
      console.debug("dateCreated", dateCreated);
      if (!isViewsAlreadyBuilt && !dateCreated) {
        console.log("This means that the src doc has not been built yet and that doc is not newly created since " +
            "it doesn't have a dateCreated attribute yet");
        await populateViewDstPathsAndBuildViewsCollection();
        await srcRef.update({[flagName]: true});
      }
    }

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

    const srcDocPath = docPaths[srcEntity];
    const srcPath = srcDocPath.split("/").slice(0, -1).join("/") + "/" + srcDocId;
    if (srcPath.includes("{")) {
      console.error("srcPath should not have a placeholder");
      logicResult.status = "error";
      logicResult.message = "srcPath should not have a placeholder";
      return logicResult;
    }

    const srcAtViewsPath = formAtViewsPath(dstPath, srcPath);

    let destProp = "";
    let isArrayMap = false;
    if (dstPath.includes("#")) {
      destProp = dstPath.split("#")[1];
      if (destProp.includes("[") && destProp.endsWith("]")) {
        destProp = destProp.split("[")[0];
        isArrayMap = true;
      }
    }

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
