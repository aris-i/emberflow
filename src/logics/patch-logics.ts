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
import {firestore} from "firebase-admin";
import Transaction = firestore.Transaction;
import {queueRunViewLogics} from "./view-logics";

export async function queueRunPatchLogics(appVersion: string, ...dstPaths: string[]) {
  try {
    for (const dstPath of dstPaths) {
      const messageId = await PATCH_LOGICS_TOPIC.publishMessage({json: {appVersion, dstPath}});
      console.log(`queueRunPathcLogics: Message ${messageId} published.`);
      console.debug(`queueRunPathcLogics: ${dstPath}`);
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

    let patchLogicResults: LogicResult[] = [];
    const distributeLogicDocs = await db.runTransaction(async (txn) => {
      const distributedLogicDocs: LogicResultDoc[] = [];
      patchLogicResults = await runPatchLogics(appVersion, dstPath, txn);

      const patchLogicResultDocs = patchLogicResults.map((result) => result.documents).flat();
      const dstPathPatchLogicDocsMap: Map<string, LogicResultDoc[]> = await expandConsolidateAndGroupByDstPath(patchLogicResultDocs);

      console.info("Distributing Patch Logic Results");

      dstPathPatchLogicDocsMap.forEach((value) => {
        value.forEach(async (logicResultDoc) => {
          await distributeDoc(logicResultDoc, undefined, txn);
          distributedLogicDocs.push(logicResultDoc);
        });
      });
      return distributedLogicDocs;
    });

    await queueRunViewLogics(...distributeLogicDocs);

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

export function versionCompare(a: string, b: string): number {
  const A = a.split(".").map((s) => parseInt(s, 10) || 0);
  const B = b.split(".").map((s) => parseInt(s, 10) || 0);
  const len = Math.max(A.length, B.length);

  for (let i = 0; i < len; i++) {
    const ai = A[i] ?? 0;
    const bi = B[i] ?? 0;
    if (ai !== bi) return ai - bi;
  }
  return 0;
}

export async function
runPatchLogics(appVersion: string, dstPath: string, txn: Transaction): Promise<LogicResult[]> {
  const {entity} = findMatchingDocPathRegex(dstPath);
  if (!entity) {
    console.error("Entity should not be blank");
    return [];
  }

  const logicResults: LogicResult[] = [];
  const snapshot = await txn.get(db.doc(dstPath));
  const data = snapshot.data();
  if (!data) return logicResults;

  const dataVersion = data["@dataVersion"] || "0.0.0";

  const matchingPatchLogics = _mockable.getPatchLogicConfigs()
    .filter((patchLogicConfig) => {
      return entity === patchLogicConfig.entity &&
            versionCompare(dataVersion, patchLogicConfig.version) < 0 &&
        versionCompare(patchLogicConfig.version, appVersion) <= 0;
    });
  // TODO: Handle errors

  for (const patchLogic of matchingPatchLogics) {
    const start = performance.now();
    const patchLogicResult = await patchLogic.patchLogicFn(dstPath, data);
    patchLogicResult.documents.push({
      action: "merge",
      dstPath,
      doc: {
        "@dataVersion": patchLogic.version,
      },
    });
    const end = performance.now();
    const execTime = end - start;
    logicResults.push({
      ...patchLogicResult,
      execTime,
      timeFinished: admin.firestore.Timestamp.now(),
    });
  }
  return logicResults;
}
