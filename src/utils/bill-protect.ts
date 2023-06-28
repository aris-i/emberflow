import {admin, docPaths, onDocChange} from "../index";

export interface FuncConfigData {
  vCPU: number;
  mem: number;
  costLimit: number;
  pricePer100ms: number;
  pricePer1MInvocation: number;
  enabled: boolean;
}

export interface FuncUsageData {
  totalElapsedTimeInMs: number;
  totalInvocations: number;
}

type onDocChangeType = typeof onDocChange;

async function fetchAndInitFuncConfig(db: FirebaseFirestore.Firestore, funcName: string) {
  const funcConfigRef = db.collection("@server").doc("config")
    .collection("functions").doc(funcName);
  const funcConfig = await funcConfigRef.get();
  let funcConfigData: FuncConfigData;
  if (!funcConfig.exists) {
    funcConfigData = {
      vCPU: 0.167,
      mem: 256,
      costLimit: 10,
      pricePer100ms: 0.000000648,
      pricePer1MInvocation: 0.40,
      enabled: true,
    };
    await funcConfigRef.set(funcConfigData);
  } else {
    funcConfigData = funcConfig.data() as FuncConfigData;
  }
  return {
    funcConfigRef,
    funcConfig: funcConfigData,
  };
}

async function fetchAndInitFuncUsage(db: FirebaseFirestore.Firestore, funcName: string) {
  const funcUsageRef = db.collection("@server").doc("usage")
    .collection("functions").doc(funcName);

  const funcUsage = await funcUsageRef.get();
  let funcUsageData: FuncUsageData;
  if (!funcUsage.exists) {
    funcUsageData = {totalElapsedTimeInMs: 0, totalInvocations: 0};
    await funcUsageRef.set(funcUsageData);
  } else {
    funcUsageData = funcUsage.data() as FuncUsageData;
  }
  return {funcUsageRef, funcUsage: funcUsageData};
}

export function computeTotalCost(totalInvocations: number, pricePer1MInvocation: number, totalElapsedTimeInMs: number, pricePer100ms: number) {
  const invocationCost = Math.max(0, (totalInvocations - 2000000)) * pricePer1MInvocation / 1000000;
  const computeTimeCost = totalElapsedTimeInMs * pricePer100ms / 100;
  return invocationCost + computeTimeCost;
}

async function lockdownCollection(entity: string) {
  const app = admin.app();

  // Get the current Firestore rules.
  const ruleset = await app.securityRules().getFirestoreRuleset();
  let currentRules = ruleset.source[0].content;

  // Define the new match block.
  const docPath = docPaths[entity];
  const newMatchBlock = `
      match /databases/{database}/${docPath} {
        allow write: if false;
      }`;

  // Insert the new match block before the last closing brace of the `match /databases/{database}/documents` block.
  const insertionIndex = currentRules.lastIndexOf("}");
  currentRules = currentRules.substring(0, insertionIndex) + newMatchBlock + currentRules.substring(insertionIndex);

  // Update the Firestore rules with the new rules.
  await app.securityRules().releaseFirestoreRulesetFromSource(currentRules);
}

export function computeElapseTime(startTime: [number, number], endTime: [number, number]) {
  const startTimeInMs = (startTime[0] * 1e9 + startTime[1]) / 1e6; // Convert to milliseconds
  const endTimeInMs = (endTime[0] * 1e9 + endTime[1]) / 1e6; // Convert to milliseconds
  // Round up to nearest 100ms increment
  return Math.ceil((endTimeInMs - startTimeInMs) / 100) * 100;
}

async function incrementTotalInvocations(funcUsageRef: FirebaseFirestore.DocumentReference<FirebaseFirestore.DocumentData>) {
  await funcUsageRef.update({
    totalInvocations: admin.firestore.FieldValue.increment(1),
  });
}

async function incrementTotalElapsedTimeInMs(funcUsageRef: FirebaseFirestore.DocumentReference<FirebaseFirestore.DocumentData>, totalElapsedTimeInMs: number) {
  await funcUsageRef.update({
    totalElapsedTimeIn100Ms: admin.firestore.FieldValue.increment(totalElapsedTimeInMs),
  });
}

async function disableFunc(funcConfigRef: FirebaseFirestore.DocumentReference<FirebaseFirestore.DocumentData>) {
  await funcConfigRef.update({enabled: false});
}

export const _mockable = {
  fetchAndInitFuncConfig,
  fetchAndInitFuncUsage,
  computeTotalCost,
  lockdownCollection,
  computeElapseTime,
  incrementTotalInvocations,
  incrementTotalElapsedTimeInMs,
  disableFunc,
};

export function useBillProtect(onDocChange: onDocChangeType) : onDocChangeType {
  return async (
    entity,
    change,
    context,
    event
  ) => {
    const startTime =process.hrtime();
    const db = admin.firestore();
    const funcName = `on-${event}-${entity}`;

    const {
      funcUsageRef,
      funcUsage: {
        totalElapsedTimeInMs,
        totalInvocations}} = await _mockable.fetchAndInitFuncUsage(db, funcName);

    try {
      const {
        funcConfigRef,
        funcConfig: {
          costLimit,
          pricePer100ms,
          pricePer1MInvocation,
          enabled,
        }} = await _mockable.fetchAndInitFuncConfig(db, funcName);

      await _mockable.incrementTotalInvocations(funcUsageRef);

      if (!enabled) {
        console.warn(`Function ${funcName} is disabled.  Returning`);
        return;
      }

      const totalCost = _mockable.computeTotalCost(
        totalInvocations+1, pricePer1MInvocation, totalElapsedTimeInMs, pricePer100ms);
      if (totalCost >= costLimit) {
        console.warn(`Function ${funcName} has exceeded the cost limit of $${costLimit}`);
        await _mockable.disableFunc(funcConfigRef);

        // Update firestore rules to lockdown collection
        await _mockable.lockdownCollection(entity);

        // TODO: Send email to all devs
        return;
      }

      return onDocChange(entity, change, context, event);
    } finally {
      const endTime = process.hrtime();
      await _mockable.incrementTotalElapsedTimeInMs(funcUsageRef, _mockable.computeElapseTime(startTime, endTime));
    }
  };
}
