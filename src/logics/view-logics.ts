import {LogicResult, LogicResultDoc, ViewDefinition, ViewLogicConfig, ViewLogicFn} from "../types";
import {db, docPaths, docPathsRegex, VIEW_LOGICS_TOPIC, VIEW_LOGICS_TOPIC_NAME} from "../index";
import * as admin from "firebase-admin";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
import {
  _mockable,
  convertLogicResultsToMetricExecutions, distributeFnNonTransactional, expandConsolidateAndGroupByDstPath,
} from "../index-utils";
import {pubsubUtils} from "../utils/pubsub";
import {reviveDateAndTimestamp} from "../utils/misc";
import {
  _mockable as pathsMockable,
  findMatchingDocPathRegex,
  getDestPropAndDestPropId,
  getParentPath,
} from "../utils/paths";
import {versionCompare} from "./patch-logics";

export function createViewLogicFn(viewDefinition: ViewDefinition): ViewLogicFn[] {
  const {
    srcEntity: defSrcEntity,
    srcProps: defSrcProps,
    destEntity: defDestEntity,
    destProp: defDestProp,
    options: defOptions,
  } = viewDefinition;
  const {syncCreate = false} = defOptions || {};

  const logicName = `${defDestEntity}${defDestProp ? `#${defDestProp.name}` : ""}`;

  function formViewDocId(viewDstPath: string) {
    let viewDocId = viewDstPath.replace(/[/#]/g, "+");
    if (viewDocId.startsWith("+")) {
      viewDocId = viewDocId.slice(1);
    }
    return viewDocId;
  }

  function createLogicDocsWhenViewIsCreated(srcPath: string, viewDstPath: string) {
    const logicResultDocs: LogicResultDoc[] = [];


    const srcAtViewsPath = formAtViewsPath(viewDstPath, srcPath);
    const {
      destProp: viewDestProp,
      destPropId: viewDestPropId,
      isArrayMap: viewIsArrayMap,
      basePath: viewBasePath,
    } = getDestPropAndDestPropId(viewDstPath);
    logicResultDocs.push({
      action: "create",
      dstPath: srcAtViewsPath,
      doc: {
        path: viewDstPath,
        srcProps: defSrcProps.sort(),
        destEntity: defDestEntity,
        ...(viewDestProp ? {destProp: viewDestProp} : {}),
      },
    });

    if (viewDestProp && viewIsArrayMap) {
      logicResultDocs.push({
        action: "merge",
        dstPath: viewBasePath,
        instructions: {
          [`@${viewDestProp}`]: `arr+(${viewDestPropId})`,
        },
        skipRunViewLogics: true,
      });
    }

    return logicResultDocs;
  }

  function formAtViewsPath(viewDstPath: string, srcPath: string) {
    const viewDocId = formViewDocId(viewDstPath);
    return `${srcPath}/@views/${viewDocId}`;
  }

  const srcToDstLogicFn: ViewLogicFn = async (logicResultDoc, targetVersion, lastProcessedId) => {
    const {
      doc: srcDoc,
      instructions: srcInstructions,
      dstPath: srcPath,
      action: srcAction,
    } = logicResultDoc;
    console.info(`Executing ViewLogic on document at ${srcPath}...`);

    function syncDeleteToViewsDstPath() {
      const documents: LogicResultDoc[] = atViewsDocs.map((viewDstPathDoc) => {
        return {
          action: "delete",
          dstPath: viewDstPathDoc.data().path,
        };
      });
      return {
        name: `${logicName} ViewLogic`,
        status: "finished",
        documents,
      } as LogicResult;
    }

    async function syncMergeToViewsDstPath() {
      const now = admin.firestore.Timestamp.now();
      const viewDoc: Record<string, any> = {
        "@updatedByViewDefinitionAt": now,
      };
      const viewInstructions: Record<string, string> = {};
      for (const srcProp of [...defSrcProps, "@dataVersion"]) {
        if (srcDoc?.[srcProp] !== undefined) {
          viewDoc[srcProp] = srcDoc[srcProp];
        }
        if (srcInstructions?.[srcProp]) {
          viewInstructions[srcProp] = srcInstructions[srcProp];
        }
      }

      const viewLogicResultDocs: LogicResultDoc[] = [];
      for (const atViewsDoc of atViewsDocs) {
        const viewDstPath = atViewsDoc.data().path;
        console.debug("Processing viewDstPath: ", viewDstPath);
        const {basePath: viewBasePath, destProp: viewDestProp, destPropId: viewDestPropId} = getDestPropAndDestPropId(viewDstPath);

        const viewDocSnap = await db.doc(viewBasePath).get();

        // If the doc doesn't exist, delete dstPath and skip creating logicDoc
        if (!viewDocSnap.exists) {
          viewLogicResultDocs.push({
            action: "delete",
            dstPath: atViewsDoc.ref.path,
          });
          continue;
        }

        if (viewDestProp) {
          const viewDocData = viewDocSnap.data();
          // If has destPropId but destPropId doesn't exist, delete dstPath and skip creating logicDoc
          if (viewDestPropId && !viewDocData?.[viewDestProp]?.[viewDestPropId]) {
            viewLogicResultDocs.push({
              action: "delete",
              dstPath: atViewsDoc.ref.path,
            });
            continue;
          }

          // If destProp only and doesn't exist, delete dstPath and skip creating logicDoc
          if (!viewDocData?.[viewDestProp]) {
            viewLogicResultDocs.push({
              action: "delete",
              dstPath: atViewsDoc.ref.path,
            });
            continue;
          }
        }

        viewLogicResultDocs.push({
          action: "merge",
          dstPath: viewDstPath,
          doc: viewDoc,
          instructions: viewInstructions,
        });
      }

      return {
        name: `${logicName} ViewLogic`,
        status: "finished",
        documents: viewLogicResultDocs,
      } as LogicResult;
    }

    async function syncAtViewsSrcPropsIfDifferentFromViewDefinition() {
      for (const atViewsDoc of atViewsDocs) {
        const {srcProps: atViewSrcProps} = atViewsDoc.data();
        const atViewPath = atViewsDoc.ref.path;
        const defSortedSrcProps = defSrcProps.sort();

        if (atViewSrcProps.join(",") === defSortedSrcProps.join(",")) {
          continue;
        }

        await db.doc(atViewPath).update({srcProps: defSortedSrcProps});
      }
    }

    async function syncCreateToDstPaths() {
      const viewLogicResultDocs: LogicResultDoc[] = [];
      const logicResult: LogicResult = {
        name: `${logicName} ViewLogic`,
        status: "finished",
        documents: viewLogicResultDocs,
      };

      const srcParentPath = getParentPath(srcPath);

      const collectionRef = db.collection("@syncCreateViews")
        .where("srcPath", "==", srcParentPath);
      const syncCreateViewSnapshot = await collectionRef.get();
      const syncCreateViewDocs = syncCreateViewSnapshot.docs;

      const docId = srcPath.split("/").pop();
      if (!docId) {
        console.error("docId could not be determined from srcPath", srcPath);
        return {
          name: `${logicName} ViewLogic`,
          status: "error",
          message: "docId could not be determined from srcPath",
          documents: [],
        } as LogicResult;
      }

      for (const syncCreateViewDoc of syncCreateViewDocs) {
        const syncCreateViewData = syncCreateViewDoc.data();
        const {dstPath: viewBaseDstPath} = syncCreateViewData;
        const {destProp: viewDestProp} = getDestPropAndDestPropId(viewBaseDstPath);
        const viewDstPath = viewDestProp ? `${viewBaseDstPath}[${docId}]` : `${viewBaseDstPath}/${docId}`;

        viewLogicResultDocs.push({
          action: "create",
          dstPath: viewDstPath,
          doc: srcDoc,
        }, ...createLogicDocsWhenViewIsCreated(srcPath, viewDstPath));
      }

      return logicResult;
    }

    if (srcAction === "create" && syncCreate) {
      return syncCreateToDstPaths();
    }

    const modifiedFields = [
      ...Object.keys(srcDoc || {}),
      ...Object.keys(srcInstructions || {}),
    ];

    let query = db.doc(srcPath)
      .collection("@views")
      .where("destEntity", "==", defDestEntity);
    console.debug("srcPath: ", srcPath);
    console.debug("destEntity: ", defDestEntity);
    if (srcAction === "delete") {
      console.debug("action === delete");
    } else {
      query = query.where("srcProps", "array-contains-any", modifiedFields);
      console.debug("modifiedFields: ", modifiedFields.join(", "));
    }
    if (defDestProp) {
      query = query.where("destProp", "==", defDestProp.name);
      console.debug("destProp: ", defDestProp.name);
    }
    query = query.orderBy("@dateCreated").limit(100);

    if (lastProcessedId) {
      const lastDocRef = db.doc(`${srcPath}/@views/${lastProcessedId}`);
      const lastDocSnap = await lastDocRef.get();
      if (lastDocSnap.exists) {
        query = query.startAfter(lastDocSnap);
      }
    }
    const atViewsDocs = (await query.get()).docs;
    console.debug(`Found ${atViewsDocs.length} matching @view documents`);

    if (atViewsDocs.length === 100) {
      console.debug("Processing limit reached. The remaining views will be processed in the next batch");
      const newLastProcessedId = atViewsDocs[atViewsDocs.length - 1].id;
      exports.queueRunViewLogics(targetVersion, [logicResultDoc], newLastProcessedId);
    }

    await syncAtViewsSrcPropsIfDifferentFromViewDefinition();

    if (srcAction === "delete") {
      return syncDeleteToViewsDstPath();
    } else {
      return syncMergeToViewsDstPath();
    }
  };

  function createLogicDocsWhenViewIsDeleted(srcPath: string, viewDstPath: string) {
    const {
      destProp: viewDestProp,
      destPropId: viewDestPropId,
      isArrayMap: viewIsArrayMap,
      basePath: viewBasePath,
    } = getDestPropAndDestPropId(viewDstPath);

    const srcAtViewsPath = formAtViewsPath(viewDstPath, srcPath);
    const logicResultDocs: LogicResultDoc[] = [];
    logicResultDocs.push({
      action: "delete",
      dstPath: srcAtViewsPath,
    });
    if (viewDestProp && viewIsArrayMap) {
      logicResultDocs.push({
        action: "merge",
        dstPath: viewBasePath,
        instructions: {
          [`@${viewDestProp}`]: `arr-(${viewDestPropId})`,
        },
      });
    }
    return logicResultDocs;
  }

  const dstToSrcLogicFn: ViewLogicFn = async (logicResultDoc) => {
    const logicResult: LogicResult = {
      name: `${logicName} Dst-to-Src`,
      status: "finished",
      documents: [],
    };
    const {
      dstPath: viewDstPath,
      action: viewAction,
      doc: viewDoc,
    } = logicResultDoc;
    const srcDocId = viewDoc?.["@id"];
    if (!srcDocId) {
      console.error("Document does not have an @id attribute");
      logicResult.status = "error";
      logicResult.message = "Document does not have an @id attribute";
      return logicResult;
    }

    function formSrcPath() {
      const srcDocPath = docPaths[defSrcEntity];
      let srcPath = srcDocPath.split("/").slice(0, -1).join("/") + "/" + srcDocId;

      const destDocPath = docPaths[defDestEntity];
      const destDocPathRegex = docPathsRegex[defDestEntity];
      const destDocPathMatches = viewDstPath.split("#")[0].match(destDocPathRegex);

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
          const value = viewDoc?.[key] || dstPathKeyValuesMap[key];
          if (value) {
            srcPath = srcPath.replace(srcDocPathKey, value);
          }
        }
      }
      return srcPath;
    }

    async function rememberForSyncCreate() {
      const srcParentPath = getParentPath(srcPath);
      const dstParentPath = getParentPath(viewDstPath);

      const dstParentPathParts = dstParentPath.split(/[/#]/).filter(Boolean);
      const isDstParentPathPartsEven = dstParentPathParts.length % 2 === 0;
      if (isDstParentPathPartsEven) {
        console.error(`invalid syncCreate dstPath, ${viewDstPath}`);
        return;
      }

      const docId = formViewDocId(dstParentPath);
      const syncCreateDocPath = `@syncCreateViews/${docId}`;
      const isAlreadyCreated = await pathsMockable.doesPathExists(syncCreateDocPath);

      if (!isAlreadyCreated) {
        logicResult.documents.push({
          action: "create",
          dstPath: syncCreateDocPath,
          doc: {
            destEntity: defDestEntity,
            dstPath: dstParentPath,
            srcPath: srcParentPath,
          },
        });
      } else {
        console.info(`${syncCreateDocPath} already exists — skipping creation.`);
      }
      return;
    }

    const srcPath = formSrcPath();
    if (srcPath.includes("{")) {
      console.error("srcPath should not have a placeholder");
      logicResult.status = "error";
      logicResult.message = "srcPath should not have a placeholder";
      return logicResult;
    }

    if (viewAction === "delete") {
      logicResult.documents.push(
        ...createLogicDocsWhenViewIsDeleted(srcPath, viewDstPath)
      );
    } else {
      logicResult.documents.push(
        ...createLogicDocsWhenViewIsCreated(srcPath, viewDstPath)
      );

      if (syncCreate) {
        await rememberForSyncCreate();
      }
    }

    return logicResult;
  };

  return [srcToDstLogicFn, dstToSrcLogicFn];
}

export async function queueRunViewLogics(
  targetVersion: string, logicResultDocs: LogicResultDoc[], lastProcessedId?: string) {
  try {
    for (const logicResultDoc of logicResultDocs) {
      if (!findMatchingViewLogics(logicResultDoc, targetVersion)?.size) {
        continue;
      }

      const {
        action,
        dstPath,
        skipRunViewLogics,
      } = logicResultDoc;
      const {basePath, destProp, destPropId} = getDestPropAndDestPropId(dstPath);
      const dstDocRef = db.doc(basePath);
      if (!skipRunViewLogics && ["create", "merge", "delete"].includes(action)) {
        if (action === "delete") {
          const data = (await dstDocRef.get()).data() || {};
          if (destProp) {
            if (destPropId) {
              logicResultDoc.doc = {[destProp]: data[destProp]?.[destPropId] || {}};
            } else {
              logicResultDoc.doc = {[destProp]: data[destProp] || {}};
            }
          } else {
            logicResultDoc.doc = data;
          }
        }
        const messageId = await VIEW_LOGICS_TOPIC.publishMessage({json: {
          doc: logicResultDoc, targetVersion, lastProcessedId,
        }});
        console.log(`queueRunViewLogics: Message ${messageId} published.`);
        console.debug(`queueRunViewLogics: ${logicResultDoc}`);
      }
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

export async function runViewLogics(logicResultDoc: LogicResultDoc, targetVersion: string, lastProcessedId?: string): Promise<LogicResult[]> {
  const matchingLogics = findMatchingViewLogics(logicResultDoc, targetVersion);
  if (!matchingLogics || matchingLogics.size === 0) {
    console.log("No matching view logics found");
    return [];
  }

  const logicResults = [];
  for (const logic of matchingLogics.values()) {
    const start = performance.now();
    const viewLogicResult = await logic.viewLogicFn(logicResultDoc, targetVersion, lastProcessedId);
    const end = performance.now();
    const execTime = end - start;
    logicResults.push({
      ...viewLogicResult,
      execTime,
      timeFinished: admin.firestore.Timestamp.now(),
    });
  }
  return logicResults;
}

export async function onMessageViewLogicsQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(VIEW_LOGICS_TOPIC_NAME, event.id)) {
    console.log("Skipping duplicate message");
    return;
  }

  try {
    const {targetVersion, doc, lastProcessedId} = event.data.message.json;
    const logicResultDoc = reviveDateAndTimestamp(doc) as LogicResultDoc;
    console.log("Received logic result doc:", logicResultDoc);

    console.info("Running View Logics");
    const start = performance.now();
    const viewLogicResults: LogicResult[] = await exports.runViewLogics(logicResultDoc, targetVersion, lastProcessedId);
    const end = performance.now();
    const execTime = end - start;
    const distributeFnLogicResult: LogicResult = {
      name: "runViewLogics",
      status: "finished",
      documents: [],
      execTime: execTime,
    };
    const metricExecutions = convertLogicResultsToMetricExecutions([...viewLogicResults, distributeFnLogicResult]);
    await _mockable.saveMetricExecution(metricExecutions);

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

export const findMatchingViewLogics = (logicResultDoc: LogicResultDoc, targetVersion: string) => {
  const {
    action,
    doc,
    instructions,
    dstPath,
  } = logicResultDoc;
  const modifiedFields: string[] = [];
  if (doc) {
    modifiedFields.push(...Object.keys(doc));
  }
  if (instructions) {
    modifiedFields.push(...Object.keys(instructions));
  }
  const {entity} = findMatchingDocPathRegex(dstPath);
  if (!entity) {
    console.error("Entity should not be blank");
    return undefined;
  }
  const {destProp} = getDestPropAndDestPropId(dstPath);

  const matchingLogics = _mockable.getViewLogicConfigs().filter((viewLogicConfig) => {
    if (action === "delete") {
      return viewLogicConfig.entity === entity &&
                (destProp ? viewLogicConfig.destProp === destProp : true) &&
                versionCompare(viewLogicConfig.version, targetVersion) <= 0;
    }

    return viewLogicConfig.actionTypes.includes(action) &&
            (
              viewLogicConfig.modifiedFields === "all" ||
                viewLogicConfig.modifiedFields.some((field) => modifiedFields.includes(field))
            ) &&
            viewLogicConfig.entity === entity && (destProp ? viewLogicConfig.destProp === destProp : true) &&
            versionCompare(viewLogicConfig.version, targetVersion) <= 0;
  })
    .reduce((acc, viewLogicConfig) => {
      const {name} = viewLogicConfig;
      if (!acc.has(name)) {
        acc.set(name, viewLogicConfig);
      } else {
        const viewLogicConfigPrev = acc.get(name)!;
        if (versionCompare(viewLogicConfigPrev.version, viewLogicConfig.version) < 0) {
          acc.set(name, viewLogicConfig);
        }
      }
      return acc;
    }, new Map<string, ViewLogicConfig>());
  return matchingLogics;
};
