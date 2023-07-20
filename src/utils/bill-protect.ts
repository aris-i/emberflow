import {admin, db, docPaths, onDocChange, projectConfig} from "../index";
import {CloudBillingClient} from "@google-cloud/billing";
import {firestore} from "firebase-admin";
import * as batch from "../utils/batch";
import {Message} from "firebase-functions/lib/v1/providers/pubsub";

export const billing = new CloudBillingClient();

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

export interface BillingAlertEvent {
  budgetDisplayName: string;
  alertThresholdExceeded: number;
  costAmount: number;
  costIntervalStart: string;
  budgetAmount: number;
  budgetAmountType: string;
  currencyCode: string;
}

type onDocChangeType = typeof onDocChange;

async function fetchAndInitFuncConfig(db: FirebaseFirestore.Firestore, funcName: string) {
  const funcConfigRef = db.collection("@server").doc("config")
    .collection("functions").doc(funcName);
  const funcConfig = await funcConfigRef.get();
  let funcConfigData: FuncConfigData;
  const {maxCostLimitPerFunction, specialCostLimitPerFunction} = projectConfig;
  if (!funcConfig.exists) {
    funcConfigData = {
      vCPU: 0.167,
      mem: 256,
      costLimit: specialCostLimitPerFunction[funcName] || maxCostLimitPerFunction,
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
    totalElapsedTimeInMs: admin.firestore.FieldValue.increment(totalElapsedTimeInMs),
  });
}

async function disableFunc(funcConfigRef: FirebaseFirestore.DocumentReference<FirebaseFirestore.DocumentData>) {
  await funcConfigRef.update({enabled: false});
}

let hardDisabled = false;
function isHardDisabled() {
  return hardDisabled;
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
  isHardDisabled,
  isBillingEnabled,
  disableBillingForProject,
};


export function useBillProtect(onDocChange: onDocChangeType) : onDocChangeType {
  return async (
    funcName,
    entity,
    change,
    context,
    event
  ) => {
    if (_mockable.isHardDisabled()) {
      console.warn(`Function ${funcName} is hard disabled.  Returning immediately`);
      return;
    }
    const startTime =process.hrtime();

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
        hardDisabled = true;
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

      return onDocChange(funcName, entity, change, context, event);
    } finally {
      const endTime = process.hrtime();
      await _mockable.incrementTotalElapsedTimeInMs(funcUsageRef, _mockable.computeElapseTime(startTime, endTime));
    }
  };
}

export async function stopBillingIfBudgetExceeded(message: Message): Promise<string> {
  const PROJECT_ID = projectConfig.projectId;
  const PROJECT_NAME = `projects/${PROJECT_ID}`;

  const pubsubData: BillingAlertEvent = JSON.parse(
    Buffer.from(message.data, "base64").toString()
  );
  if (pubsubData.costAmount <= pubsubData.budgetAmount) {
    console.log(`No action necessary. (Current cost: ${pubsubData.costAmount})`);
    return `No action necessary. (Current cost: ${pubsubData.costAmount})`;
  }

  const billingEnabled = await _mockable.isBillingEnabled(PROJECT_NAME);
  if (billingEnabled) {
    console.log("Disabling billing");
    return _mockable.disableBillingForProject(PROJECT_NAME);
  } else {
    console.log("Billing already disabled");
    return "Billing already disabled";
  }
}

/**
 * Determine whether billing is enabled for a project
 * @param {string} projectName Name of project to check if billing is enabled
 * @return {Promise<boolean>} Whether project has billing enabled or not
 */
async function isBillingEnabled(projectName: string): Promise<boolean> {
  try {
    const [res] = await billing.getProjectBillingInfo({name: projectName});
    return res.billingEnabled || true;
  } catch (e) {
    console.log(
      "Unable to determine if billing is enabled on specified project, assuming billing is enabled"
    );
    return true;
  }
}

/**
 * Disable billing for a project by removing its billing account
 * @param {string} projectName Name of project to disable billing on
 * @return {Promise<string>} Text containing response from disabling billing
 */
async function disableBillingForProject(projectName: string): Promise<string> {
  const [res] = await billing.updateProjectBillingInfo({
    name: projectName,
    projectBillingInfo: {
      billingAccountName: "",
    }, // Disable billing
  });
  console.log(`Billing disabled: ${JSON.stringify(res)}`);
  return `Billing disabled: ${JSON.stringify(res)}`;
}

export async function resetUsageStats() {
  const usageCollectionPath = "@server/usage/functions";
  const collectionRef = db.collection(usageCollectionPath);
  const querySnapshot = await collectionRef.select(firestore.FieldPath.documentId()).get();
  querySnapshot.forEach((doc) => {
    return batch.set(doc.ref, {
      totalElapsedTimeInMs: 0,
      totalInvocations: 0,
    });
  });
  await batch.commit();
}
