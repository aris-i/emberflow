import {CleanupConfig} from "../types";

export const cleanupConfigs: CleanupConfig[] = [
  {
    // Purge stale askJaris documents that never got a topic assigned.
    // "attribute missing" is modeled as an indexed boolean flag (hasTopic).
    collectionPath: "askJaris",
    isCollectionGroup: true,
    timestampField: "createdAt",
    olderThan: {value: 1, unit: "months"},
    conditions: [
      {fieldName: "hasTopic", operator: "==", value: false},
    ],
  },
];
