import type {ScheduledEvent} from "firebase-functions/v2/scheduler";
import {firestore} from "firebase-admin";
import {cleanupConfigs, db, rtdb} from "../index";
import {deleteCollection, deleteCollectionRecursive} from "./misc";
import {CleanupConfig, CleanupTimeUnit, LogicResultDoc} from "../types";
import {queueRunViewLogics} from "../logics/view-logics";

export function computeCutoffDate(value: number, unit: CleanupTimeUnit): Date {
  const now = new Date();
  if (unit === "hours") {
    return new Date(now.getTime() - value * 3600_000);
  }
  if (unit === "days") {
    return new Date(now.getTime() - value * 86_400_000);
  }
  // months -> calendar-aware
  const d = new Date(now);
  d.setMonth(d.getMonth() - value);
  return d;
}

export function getInternalCleanupConfigs(): CleanupConfig[] {
  return [
    {
      collectionPath: "processedIds",
      isCollectionGroup: true,
      timestampField: "timestamp",
      olderThan: {value: 7, unit: "days"},
      recursive: false,
    },
    {
      collectionPath: "executions",
      isCollectionGroup: true,
      timestampField: "execDate",
      olderThan: {value: 7, unit: "days"},
      recursive: false,
    },
    {
      collectionPath: "computations",
      isCollectionGroup: true,
      timestampField: "createdAt",
      olderThan: {value: 30, unit: "days"},
      recursive: false,
    },
    {
      collectionPath: "@emberflow/internal/viewLogicExecutions",
      timestampField: "execDate",
      olderThan: {value: 7, unit: "days"},
    },
    {
      collectionPath: "@actions",
      timestampField: "timeCreated",
      olderThan: {value: 7, unit: "days"},
      onBatchDeleted: async (snapshot: firestore.QuerySnapshot) => {
        const updates: {[key: string]: null} = {};
        for (const doc of snapshot.docs) {
          const {formId, uid} = doc.data().eventContext ?? {};
          if (formId && uid) {
            updates[`forms/${uid}/${formId}`] = null;
          }
        }
        if (Object.keys(updates).length) {
          await rtdb.ref().update(updates);
        }
      },
    },
  ];
}

export async function cleanupCollections(_event: ScheduledEvent) {
  console.info("Running cleanupCollections");
  const all = [...getInternalCleanupConfigs(), ...cleanupConfigs];
  for (const cfg of all) {
    try {
      const cutoff = computeCutoffDate(cfg.olderThan.value, cfg.olderThan.unit);
      const base = cfg.isCollectionGroup ?
        db.collectionGroup(cfg.collectionPath) :
        db.collection(cfg.collectionPath);
      let query: firestore.Query = base.where(cfg.timestampField, "<", cutoff);
      for (const condition of cfg.conditions ?? []) {
        query = query.where(condition.fieldName, condition.operator, condition.value);
      }

      let deleted = 0;
      if (cfg.recursive === false) {
        await deleteCollection(query, async (snapshot) => {
          deleted += snapshot.size;
          if (cfg.syncViews) {
            const logicResultDocs: LogicResultDoc[] = snapshot.docs.map((doc) => ({
              action: "delete",
              dstPath: doc.ref.path,
              doc: doc.data(),
            }));
            const targetVersion = cfg.targetVersion || "1.0.0";
            const appVersion = cfg.appVersion || "1.0.0";
            await queueRunViewLogics(targetVersion, appVersion, logicResultDocs);
          }
          if (cfg.onBatchDeleted) {
            await cfg.onBatchDeleted(snapshot);
          }
        }, !cfg.syncViews);
      } else {
        const skipSubcollections = cfg.syncViews ? (id: string) => id === "@views" : undefined;
        await deleteCollectionRecursive(query, async (snapshot) => {
          deleted += snapshot.size;
          if (cfg.syncViews) {
            const logicResultDocs: LogicResultDoc[] = snapshot.docs.map((doc) => ({
              action: "delete",
              dstPath: doc.ref.path,
              doc: doc.data(),
            }));
            const targetVersion = cfg.targetVersion || "1.0.0";
            const appVersion = cfg.appVersion || "1.0.0";
            await queueRunViewLogics(targetVersion, appVersion, logicResultDocs);
          }
          if (cfg.onBatchDeleted) {
            await cfg.onBatchDeleted(snapshot);
          }
        }, skipSubcollections);
      }
      console.info(`cleanupCollections: deleted ${deleted} from ${cfg.collectionPath}`);
    } catch (e) {
      console.error(`cleanupCollections failed for ${cfg.collectionPath}`, e);
    }
  }
}
