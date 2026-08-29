import {db, docPaths, docPathsRegex, viewDefinitions, VIEW_LOGICS_TOPIC, VIEW_LOGICS_TOPIC_NAME} from "../index";
import * as admin from "firebase-admin";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import type {MessagePublishedData} from "firebase-functions/v2/pubsub";
import {
  _mockable,
  convertLogicResultsToMetricExecutions, distributeFnNonTransactional, expandConsolidateAndGroupByDstPath,
} from "../index-utils";
import {pubsubUtils} from "../utils/pubsub";
import {queueInstructions} from "../utils/distribution";
import {logMemoryUsage, reviveDateAndTimestamp} from "../utils/misc";
import {chunkQuery} from "../utils/query";
import {
  LogicResult,
  LogicResultDoc,
  MetricExecution,
  ViewDefinition,
  ViewLogicConfig,
  ViewLogicFn,
} from "../types";
import {
  _mockable as pathsMockable,
  findMatchingDocPathRegex,
  getDestPropAndDestPropId,
  getParentPath,
} from "../utils/paths";
import {versionCompare} from "./patch-logics";

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

/*
 * Builds the LogicResultDoc(s) that materialize an `@views` document linking a view
 * destination (viewDstPath) back to its source document (srcPath), plus the array-map
 * bookkeeping instruction when the destination is an array-map property.
 *
 * This is the single source of truth for the shape of a "view created" `@views` document and is
 * shared between the automatic view-creation path (createViewLogicFn) and the manual
 * back-fill utility (createViewDoc).
 */
function buildViewCreatedLogicDocs(
  srcPath: string,
  viewDstPath: string,
  srcProps: string[],
  destEntity: string,
): LogicResultDoc[] {
  const logicResultDocs: LogicResultDoc[] = [];

  const srcAtViewsPath = formAtViewsPath(viewDstPath, srcPath);
  const {
    destProp: viewDestProp,
    destPropId: viewDestPropId,
    isArrayMap: viewIsArrayMap,
    basePath: viewBasePath,
  } = getDestPropAndDestPropId(viewDstPath);

  logicResultDocs.push({
    action: "merge",
    dstPath: srcAtViewsPath,
    doc: {
      path: viewDstPath,
      srcProps: [...srcProps].sort(),
      destEntity,
      ...(viewDestProp ? {destProp: viewDestProp} : {}),
    },
  });

  if (viewDestProp && viewIsArrayMap) {
    logicResultDocs.push({
      action: "merge",
      dstPath: viewBasePath,
      instructions: {
        [`@${viewDestProp}`]: `arr(+${viewDestPropId})`,
      },
      skipRunViewLogics: true,
    });
  }

  return logicResultDocs;
}

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

  function createLogicDocsWhenViewIsCreated(srcPath: string, viewDstPath: string) {
    return buildViewCreatedLogicDocs(srcPath, viewDstPath, defSrcProps, defDestEntity);
  }

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
          [`@${viewDestProp}`]: `arr(-${viewDestPropId})`,
        },
      });
    }

    return logicResultDocs;
  }

  const srcToDstLogicFn: ViewLogicFn = async (logicResultDoc, targetVersion, appVersion, lastProcessedId) => {
    const {
      doc: srcDoc,
      instructions: srcInstructions,
      dstPath: srcPath,
      action: srcAction,
    } = logicResultDoc;

    function syncDeleteToViewsDstPath() {
      const documents: LogicResultDoc[] = [];
      while (atViewsDocs.length > 0) {
        const viewDstPathDoc = atViewsDocs.shift()!;
        documents.push({
          action: "delete",
          dstPath: viewDstPathDoc.data().path,
        });
      }
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
      const srcPropsToCopy = [...defSrcProps, "@dataVersion"];

      const srcDocKeys = Object.keys(srcDoc || {});
      const srcInstructionsKeys = Object.keys(srcInstructions || {});

      for (const srcProp of srcPropsToCopy) {
        if (srcDoc?.[srcProp] !== undefined) {
          viewDoc[srcProp] = srcDoc[srcProp];
        }

        if (srcInstructions?.[srcProp]) {
          viewInstructions[srcProp] = srcInstructions[srcProp];
        }
      }

      // Check for dot-notation keys and log them
      for (const key of srcDocKeys) {
        if (key.includes(".")) {
          console.debug(`Dot-notation key detected in srcDoc: ${key}`);
        }
      }
      for (const key of srcInstructionsKeys) {
        if (key.includes(".")) {
          console.debug(`Dot-notation key detected in srcInstructions: ${key}`);
        }
      }

      const viewLogicResultDocs: LogicResultDoc[] = [];
      const viewBasePaths = atViewsDocs.map((atViewsDoc) => {
        const viewDstPath = atViewsDoc.data().path;
        const {basePath} = getDestPropAndDestPropId(viewDstPath);
        return db.doc(basePath);
      });

      const viewDocSnaps = viewBasePaths.length > 0 ? await db.getAll(...viewBasePaths) : [];

      for (let i = 0; i < atViewsDocs.length; i++) {
        const atViewsDoc = atViewsDocs[i];
        const viewDocSnap = viewDocSnaps[i];
        const viewDstPath = atViewsDoc.data().path;
        const {destProp: viewDestProp, destPropId: viewDestPropId} = getDestPropAndDestPropId(viewDstPath);

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
          const viewObj = viewDocData?.[viewDestProp];
          const {"@id": viewObjId} = viewObj || {};
          const sourceDocId = atViewsDoc.ref.parent.parent?.id;
          if (!viewObj || viewObjId && sourceDocId && viewObjId !== sourceDocId) {
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

      // Clear atViewsDocs and viewDocSnaps to free up memory
      atViewsDocs.length = 0;
      viewDocSnaps.length = 0;

      return {
        name: `${logicName} ViewLogic`,
        status: "finished",
        documents: viewLogicResultDocs,
      } as LogicResult;
    }

    async function syncAtViewsSrcPropsIfDifferentFromViewDefinition() {
      const batch = _mockable.getBatchUtil();
      let hasUpdates = false;
      const defSortedSrcProps = [...defSrcProps].sort();
      for (const atViewsDoc of atViewsDocs) {
        const {srcProps: atViewSrcProps} = atViewsDoc.data();
        const atViewPath = atViewsDoc.ref.path;

        if (atViewSrcProps.join(",") === defSortedSrcProps.join(",")) {
          continue;
        }

        await batch.update(db.doc(atViewPath), {srcProps: defSortedSrcProps});
        hasUpdates = true;
      }

      if (hasUpdates) {
        await batch.commit();
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
        const {destEntity: viewDestEntity, destProp: syncDestPropName, dstPath: viewBaseDstPath} = syncCreateViewData;

        // Skip if this sync configuration doesn't belong to the current view logic definition
        if (viewDestEntity !== defDestEntity) continue;
        if ((defDestProp?.name || null) !== (syncDestPropName || null)) continue;

        const {destProp: viewDestProp} = getDestPropAndDestPropId(viewBaseDstPath);
        const viewDstPath = viewDestProp ? `${viewBaseDstPath}[${docId}]` : `${viewBaseDstPath}/${docId}`;

        viewLogicResultDocs.push({
          action: "merge",
          dstPath: viewDstPath,
          doc: srcDoc,
        }, ...createLogicDocsWhenViewIsCreated(srcPath, viewDstPath));
      }

      return logicResult;
    }

    if (srcAction === "create" && syncCreate) {
      return syncCreateToDstPaths();
    }

    const atViewsDocs: admin.firestore.QueryDocumentSnapshot[] = [];
    const limitPerBatch = 50;

    let baseQuery = db.doc(srcPath)
      .collection("@views")
      .where("destEntity", "==", defDestEntity);
    if (defDestProp) {
      baseQuery = baseQuery.where("destProp", "==", defDestProp.name);
    }

    const lastDocSnap = lastProcessedId ? await db.doc(`${srcPath}/@views/${lastProcessedId}`).get() : null;

    async function getDocs(query: admin.firestore.Query) {
      let q = query.limit(limitPerBatch);
      if (lastDocSnap?.exists) {
        q = q.startAfter(lastDocSnap);
      }
      return (await q.get()).docs;
    }

    if (srcAction === "delete") {
      atViewsDocs.push(...await getDocs(baseQuery));
    } else {
      const modifiedFields = [
        ...Object.keys(srcDoc || {}),
        ...Object.keys(srcInstructions || {}),
      ];

      atViewsDocs.push(...await chunkQuery(baseQuery, "srcProps", "array-contains-any", modifiedFields, limitPerBatch, lastDocSnap as any));
    }

    if (atViewsDocs.length >= limitPerBatch) {
      const newLastProcessedId = atViewsDocs[atViewsDocs.length - 1].id;
      await exports.queueRunViewLogics(targetVersion, appVersion, [logicResultDoc], newLastProcessedId);
    }

    await syncAtViewsSrcPropsIfDifferentFromViewDefinition();

    if (srcAction === "delete") {
      return syncDeleteToViewsDstPath();
    } else {
      return syncMergeToViewsDstPath();
    }
  };

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

    const {destPropId} = getDestPropAndDestPropId(viewDstPath);
    const srcDocId = viewDoc?.["@id"] || destPropId || viewDstPath.split("/").pop();

    if (!srcDocId) {
      console.error("Document does not have an @id attribute and srcDocId could not be determined from dstPath");
      logicResult.status = "error";
      logicResult.message = "Document does not have an @id attribute and srcDocId could not be determined from dstPath";
      return logicResult;
    }

    function formSrcPath() {
      const srcDocPath = docPaths[defSrcEntity];
      let srcPathParent = srcDocPath.split("/").slice(0, -1).join("/");

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
            srcPathParent = srcPathParent.replace(srcDocPathKey, value);
          }
        }
      }
      return `${srcPathParent}/${srcDocId}`;
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
      return logicResult;
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
          action: "merge",
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

    logicResult.documents.push(
      ...createLogicDocsWhenViewIsCreated(srcPath, viewDstPath)
    );

    if (syncCreate) {
      await rememberForSyncCreate();
    }

    return logicResult;
  };

  return [srcToDstLogicFn, dstToSrcLogicFn];
}

export async function queueRunViewLogics(
  targetVersion: string,
  appVersion: string,
  logicResultDocs: LogicResultDoc[],
  lastProcessedId?: string,
) {
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
      const {basePath} = getDestPropAndDestPropId(dstPath);
      const dstDocRef = db.doc(basePath);
      if (!skipRunViewLogics && ["create", "merge", "delete"].includes(action)) {
        if (action === "delete") {
          const data = (await dstDocRef.get()).data() || {};
          const {destProp, destPropId} = getDestPropAndDestPropId(dstPath);
          if (destProp) {
            if (destPropId) {
              logicResultDoc.doc = data[destProp]?.[destPropId] || {};
            } else {
              logicResultDoc.doc = data[destProp] || {};
            }
          } else {
            logicResultDoc.doc = data;
          }
        }
        await VIEW_LOGICS_TOPIC.publishMessage({json: {
          doc: logicResultDoc, appVersion, targetVersion, lastProcessedId,
        }});
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

export async function runViewLogics(
  logicResultDoc: LogicResultDoc,
  targetVersion: string,
  appVersion: string,
  lastProcessedId?: string,
): Promise<LogicResult[]> {
  const matchingLogics = findMatchingViewLogics(logicResultDoc, targetVersion);
  if (!matchingLogics || matchingLogics.size === 0) {
    return [];
  }

  const logicResults = [];
  for (const logic of matchingLogics.values()) {
    const start = performance.now();
    try {
      const viewLogicResult = await logic.viewLogicFn(logicResultDoc, targetVersion, appVersion, lastProcessedId);
      const end = performance.now();
      const execTime = end - start;
      logicResults.push({
        ...viewLogicResult,
        name: logic.name,
        execTime,
        timeFinished: admin.firestore.Timestamp.now(),
      });
    } catch (e) {
      console.error(`Error in viewLogicFn "${logic.name}":`, e);
      const end = performance.now();
      const execTime = end - start;
      logicResults.push({
        name: logic.name,
        status: "error" as const,
        documents: [],
        execTime,
        message: (e as Error).message,
        timeFinished: admin.firestore.Timestamp.now(),
      });
    }
  }
  return logicResults;
}

export async function onMessageViewLogicsQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(VIEW_LOGICS_TOPIC_NAME, event.id)) {
    return;
  }

  try {
    if (!event.data.message.json) {
      throw new Error("No json in message");
    }
    const {appVersion, targetVersion, doc, lastProcessedId} = event.data.message.json;
    const srcLogicResultDoc = reviveDateAndTimestamp(doc) as LogicResultDoc;

    logMemoryUsage("Before Running View Logics");
    const start = performance.now();
    const viewLogicResults: LogicResult[] = await exports.runViewLogics(srcLogicResultDoc, targetVersion, appVersion, lastProcessedId);
    const end = performance.now();
    logMemoryUsage("After Running View Logics");
    const metricExecutions = convertLogicResultsToMetricExecutions([...viewLogicResults]);
    for (const result of viewLogicResults) {
      const {name, execTime, status, message, documents, timeFinished} = result;
      const viewLogicRef = db.collection("@emberflow").doc("internal").collection("viewLogics").doc(name);
      if (process.env.JEST_WORKER_ID === undefined) {
        await queueInstructions(viewLogicRef.path, {
          totalExecTime: `+${execTime || 0}`,
          totalExecCount: "++",
        });

        const batch = db.batch();
        const execRef = db.collection("@emberflow").doc("internal").collection("viewLogicExecutions").doc();
        batch.set(execRef, {
          name,
          execDate: timeFinished || admin.firestore.Timestamp.now(),
          execTime: execTime || 0,
          status,
          message: message || null,
          srcLogicResultDoc,
          documentsCount: documents.length,
        }, {merge: true});

        for (const doc of documents) {
          const docRef = execRef.collection("docs").doc();
          batch.set(docRef, doc, {merge: true});
        }
        await batch.commit();
      }
    }

    const runViewLogicsMetricExecution: MetricExecution = {
      name: "runViewLogics",
      execTime: end - start,
    };
    await _mockable.saveMetricExecution([...metricExecutions, runViewLogicsMetricExecution]);

    logMemoryUsage("Before Expanding and Grouping View Logic Results");
    const viewLogicResultDocs: LogicResultDoc[] = [];
    while (viewLogicResults.length > 0) {
      const result = viewLogicResults.shift();
      if (result) {
        viewLogicResultDocs.push(...result.documents);
        (result.documents as any).length = 0;
      }
    }

    const dstPathViewLogicDocsMap: Map<string, LogicResultDoc[]> = await expandConsolidateAndGroupByDstPath(viewLogicResultDocs);
    logMemoryUsage("After Expanding and Grouping View Logic Results");

    await distributeFnNonTransactional(dstPathViewLogicDocsMap, appVersion, true);
    logMemoryUsage("After Distributing View Logic Results");

    await pubsubUtils.trackProcessedIds(VIEW_LOGICS_TOPIC_NAME, event.id);
    return "Processed view logics";
  } catch (e) {
    console.error("Error in onMessageViewLogicsQueue", e);
    if (e instanceof Error && e.message === "No json in message") {
      throw e;
    }
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
  const {basePath, destProp} = getDestPropAndDestPropId(dstPath);
  const {entity} = findMatchingDocPathRegex(basePath);
  if (!entity) {
    console.error("Entity should not be blank");
    return undefined;
  }

  const allConfigs = _mockable.getViewLogicConfigs();
  const matchingLogics = allConfigs.filter((viewLogicConfig) => {
    const isWithinTargetVersion = versionCompare(viewLogicConfig.version, targetVersion) <= 0;

    if (action === "delete") {
      return viewLogicConfig.entity === entity &&
                (destProp ? viewLogicConfig.destProp === destProp : !viewLogicConfig.destProp) &&
                isWithinTargetVersion;
    }

    return viewLogicConfig.actionTypes.includes(action) &&
            (
              viewLogicConfig.modifiedFields === "all" ||
                viewLogicConfig.modifiedFields.some((field) => modifiedFields.includes(field))
            ) &&
            viewLogicConfig.entity === entity && (destProp ? viewLogicConfig.destProp === destProp : !viewLogicConfig.destProp) &&
            isWithinTargetVersion;
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

/**
 * Builds the LogicResultDoc(s) required to create an `@views` document that manually links a
 * view destination (`viewDstPath`) back to its source document (`srcPath`).
 *
 * This is intended for use inside back-fill patches that need to materialize an `@views` link
 * by hand. Rather than trusting caller-supplied values, the `srcProps` and `destEntity` recorded
 * in the produced `@views` document are derived from the registered {@link ViewDefinition}, so the
 * resulting document is guaranteed to be consistent with the framework's view logic. When the view
 * destination is an array-map property, an additional instruction is emitted to register the source
 * id in the destination's `@`-prefixed array, mirroring the automatic view-creation path.
 *
 * The returned docs are meant to be distributed through the normal framework pipeline (e.g. via
 * the same mechanism a patch uses to emit its LogicResultDocs), NOT written directly to Firestore.
 *
 * @param {string} srcPath The source document path (e.g. "users/1234").
 * @param {string} viewDstPath The view destination path (e.g. "users/1/posts/9#followers[1234]" or "servers/123#createdBy").
 * @return {LogicResultDoc[]} The merge (and, for array-map views, instruction) docs to distribute.
 * @throws {Error} If the src/dst entities cannot be resolved or no matching ViewDefinition is registered.
 */
export function createViewDoc(srcPath: string, viewDstPath: string): LogicResultDoc[] {
  const {entity: srcEntity} = findMatchingDocPathRegex(srcPath);
  if (!srcEntity) {
    throw new Error(`Cannot resolve src entity from srcPath: ${srcPath}`);
  }

  const {
    basePath: viewBasePath,
    destProp: viewDestProp,
  } = getDestPropAndDestPropId(viewDstPath);

  const {entity: destEntity} = findMatchingDocPathRegex(viewBasePath);
  if (!destEntity) {
    throw new Error(`Cannot resolve dest entity from viewDstPath: ${viewDstPath}`);
  }

  const matchingViewDefinitions = viewDefinitions.filter((viewDefinition) =>
    viewDefinition.srcEntity === srcEntity &&
    viewDefinition.destEntity === destEntity &&
    (viewDefinition.destProp?.name || undefined) === (viewDestProp || undefined)
  );
  if (matchingViewDefinitions.length === 0) {
    throw new Error(
      `No matching ViewDefinition found for srcEntity="${srcEntity}", destEntity="${destEntity}"` +
      `${viewDestProp ? `, destProp="${viewDestProp}"` : ""}`
    );
  }

  // When multiple versions of a definition match, use the latest one.
  const matchedViewDefinition = matchingViewDefinitions.reduce((latest, current) =>
    versionCompare(current.version, latest.version) > 0 ? current : latest
  );

  return buildViewCreatedLogicDocs(srcPath, viewDstPath, matchedViewDefinition.srcProps, destEntity);
}


