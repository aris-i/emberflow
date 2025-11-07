import {LogicResult, PatchLogicConfig} from "../types";
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
  _mockable,
  distributeFnTransactional,
} from "../index-utils";
import {queueRunViewLogics} from "./view-logics";

export async function queueRunPatchLogics(appVersion: string, ...dstPaths: string[]) {
  try {
    for (const dstPath of dstPaths) {
      const {patchLogicConfigs, dataVersion} = await findMatchingPatchLogics(appVersion, dstPath);
      if (dataVersion === undefined || patchLogicConfigs === undefined || patchLogicConfigs.length === 0) {
        console.log("No patch logics found for", dstPath);
        continue;
      }
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
    await runPatchLogics(appVersion, dstPath);

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

export const findMatchingPatchLogics = async (appVersion: string, dstPath: string) => {
  const {entity} = findMatchingDocPathRegex(dstPath);
  if (!entity) {
    console.error("Entity should not be blank");
    return {patchLogicConfigs: undefined, dataVersion: undefined};
  }
  const docRef = db.doc(dstPath);
  const docSnap = await docRef.get();
  const document = docSnap.data();

  if (!document) {
    console.error("Document does not exist");
    return {patchLogicConfigs: undefined, dataVersion: undefined};
  }
  const dataVersion= document["@dataVersion"] as string || "0.0.0";

  const patchLogicConfigs = _mockable.getPatchLogicConfigs()
    .filter((patchLogicConfig) => {
      return entity === patchLogicConfig.entity &&
        versionCompare(dataVersion, patchLogicConfig.version) < 0 &&
        versionCompare(patchLogicConfig.version, appVersion) <= 0 &&
        (patchLogicConfig.addtlFilterFn ? patchLogicConfig.addtlFilterFn(document) : true);
    });
  return {patchLogicConfigs, dataVersion};
};

export const runPatchLogics = async (appVersion: string, dstPath: string): Promise<void> => {
  const {patchLogicConfigs, dataVersion} = await findMatchingPatchLogics(appVersion, dstPath);
  if (!patchLogicConfigs || patchLogicConfigs.length === 0) {
    console.info("No matching patch logics found for", dstPath);
    return;
  }

  const matchingPatchLogicsByVersion = patchLogicConfigs
    .sort((a, b) =>versionCompare(a.version, b.version))
    .reduce((map, patchLogicConfig) => {
      if (!map.has(patchLogicConfig.version)) {
        map.set(patchLogicConfig.version, []);
      }
      map.get(patchLogicConfig.version)?.push(patchLogicConfig);
      return map;
    }, new Map<string, PatchLogicConfig[]>());

  for (const [patchVersion, patchLogicConfigs] of matchingPatchLogicsByVersion) {
    const start = performance.now();
    const logicResultsForMetricExecution = await db.runTransaction(async (txn) => {
      const logicResults: LogicResult[] = [];
      const txnDocSnap = await txn.get(db.doc(dstPath));
      const txnDocument = txnDocSnap.data();
      if (!txnDocument) {
        console.error("Document does not exist");
        return logicResults;
      }
      const txnDataVersion = txnDocument["@dataVersion"] || "0.0.0";

      // run only if patch version is higher than the dataVersion
      if (versionCompare(txnDataVersion, patchVersion) >= 0) {
        console.info(`Skipping Patch Logics for version ${patchVersion} as it is not higher than data version ${dataVersion}`);
        return logicResults;
      }

      console.info(`Running Patch Logics for version ${patchVersion}`);
      console.debug("Original Document", txnDocument);
      for (const patchLogicConfig of patchLogicConfigs) {
        console.info("Running Patch Logic:", patchLogicConfig.name,);
        const patchLogicStartTime = performance.now();
        const patchLogicResult = await patchLogicConfig.patchLogicFn(dstPath, txnDocument);
        const patchLogicEndTime = performance.now();
        const execTime = patchLogicEndTime - patchLogicStartTime;
        logicResults.push({...patchLogicResult, execTime});
      }

      logicResults.forEach((logicResult) => {
        logicResult.transactional = true;
        logicResult.documents.forEach((document) => {
          if (["merge", "create"].includes(document.action) && document.doc) {
            document.doc["@dataVersion"] = patchVersion;
          }
        });
      });

      const distributedLogicDocs = await distributeFnTransactional(txn, logicResults);

      console.debug("Distributed Logic Docs", distributedLogicDocs);

      await queueRunViewLogics(patchVersion, distributedLogicDocs);

      console.info(`Finished Patch Logic for version ${patchVersion}`);
      return logicResults.map((result) => ({
        ...result, timeFinished: admin.firestore.Timestamp.now(),
      }));
    });

    const end = performance.now();
    const execTime = end - start;
    const runPatchMetricsLogicResult: LogicResult = {
      name: "runPatchLogics",
      status: "finished",
      documents: [],
      execTime: execTime,
    };

    if ( logicResultsForMetricExecution.length > 0 ) {
      await _mockable.createMetricExecution([...logicResultsForMetricExecution, runPatchMetricsLogicResult]);
    }
  }
};
