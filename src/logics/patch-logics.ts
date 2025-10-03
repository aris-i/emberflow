import {LogicResult, LogicResultDoc} from "../types";
import {findMatchingDocPathRegex} from "../utils/paths";
import {
  admin,
  db,
  PATCH_LOGICS_TOPIC,
  PATCH_LOGICS_TOPIC_NAME,
} from "../index";
import {CloudEvent} from "firebase-functions/core";
import {MessagePublishedData} from "firebase-functions/pubsub";
import {pubsubUtils} from "../utils/pubsub";
import {
  _mockable, distributeDoc,
  expandConsolidateAndGroupByDstPath,
} from "../index-utils";
import {queueRunViewLogics} from "./view-logics";

export async function queueRunPatchLogics(appVersion: string, ...dstPaths: string[]) {
  try {
    for (const dstPath of dstPaths) {
      const messageId = await PATCH_LOGICS_TOPIC.publishMessage({json: {appVersion, dstPath}});
      console.log(`queueRunPatchLogics: Message ${messageId} published.`);
      console.debug(`queueRunPatchLogics: ${dstPath}`);
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

export async function onMessageRunPatchLogicsQueue(event: CloudEvent<MessagePublishedData>) {
  if (await pubsubUtils.isProcessed(PATCH_LOGICS_TOPIC_NAME, event.id)) {
    console.log("Skipping duplicate message");
    return;
  }

  try {
    const {dstPath, appVersion} = event.data.message.json;
    console.log("Received dstPath:", dstPath);

    console.info("Running Patch Logics");
    const start = performance.now();

    const patchLogicResults = await runPatchLogics(appVersion, dstPath);

    const end = performance.now();
    const execTime = end - start;
    const distributeFnLogicResult: LogicResult = {
      name: "runPatchLogics",
      status: "finished",
      documents: [],
      execTime: execTime,
    };
    await _mockable.createMetricExecution([...patchLogicResults, distributeFnLogicResult]);

    await pubsubUtils.trackProcessedIds(PATCH_LOGICS_TOPIC_NAME, event.id);
    return "Processed patch logics";
  } catch (e) {
    console.error("PubSub message was not JSON", e);
    throw new Error("No json in message");
  }
}

export function versionCompare(version1: string, version2: string): number {
  const parts1 = version1.split(".").map(Number);
  const parts2 = version2.split(".").map(Number);

  const length = Math.max(parts1.length, parts2.length);

  for (let i = 0; i < length; i++) {
    const num1 = parts1[i] ?? 0;
    const num2 = parts2[i] ?? 0;

    if (num1 > num2) return 1; // version1 is higher
    if (num1 < num2) return -1; // version2 is higher
  }

  return 0; // equal
}

export const runPatchLogics = async (appVersion: string, dstPath: string): Promise<LogicResult[]> => {
  const {entity} = findMatchingDocPathRegex(dstPath);
  if (!entity) {
    console.error("Entity should not be blank");
    return [];
  }

  return await db.runTransaction(async (txn) => {
    const logicResults: LogicResult[] = [];
    const snapshot = await txn.get(db.doc(dstPath));
    const data = snapshot.data();
    console.debug("data", data);
    if (!data) return logicResults;
    const dataVersion = data["@dataVersion"] || "0.0.0";

    const matchingPatchLogics = _mockable.getPatchLogicConfigs()
      .filter((patchLogicConfig) => {
        return entity === patchLogicConfig.entity &&
          versionCompare(dataVersion, patchLogicConfig.version) < 0 &&
          versionCompare(patchLogicConfig.version, appVersion) <= 0;
      });

    for (const patchLogic of matchingPatchLogics) {
      console.info(`Running Patch Logic ${patchLogic.name}`);
      const distributedLogicDocs: LogicResultDoc[] = [];
      const start = performance.now();
      const patchLogicResult = await patchLogic.patchLogicFn(dstPath, data);
      const end = performance.now();
      const execTime = end - start;

      const patchLogicResultDocs = patchLogicResult.documents;

      for (const doc of [...patchLogicResultDocs]) {
        if (doc.action === "delete") continue;

        // this will be consolidated later
        patchLogicResultDocs.push({
          action: "merge",
          dstPath: doc.dstPath,
          doc: {
            "@dataVersion": patchLogic.version,
          },
        });
      }

      const dstPathPatchLogicDocsMap: Map<string, LogicResultDoc[]> =
        await expandConsolidateAndGroupByDstPath(patchLogicResultDocs);

      dstPathPatchLogicDocsMap.forEach((value) => {
        value.forEach(async (logicResultDoc) => {
          await distributeDoc(logicResultDoc, undefined, txn);
          distributedLogicDocs.push(logicResultDoc);
        });
      });

      await queueRunViewLogics(appVersion, ...distributedLogicDocs);

      logicResults.push({
        ...patchLogicResult,
        documents: distributedLogicDocs,
        execTime,
        timeFinished: admin.firestore.Timestamp.now(),
      });
    }
    return logicResults;
  });
};
