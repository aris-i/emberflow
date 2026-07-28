import {BackFillPatchConfig} from "../types";

const sampleOneTimePatch: BackFillPatchConfig = {
  name: "sample-one-time-patch",
  patchFn: async (collectionPath, docs) => {
    console.log(`Executing sample one-time patch on collection ${collectionPath} (${docs.length} docs)...`);
    // Add your custom bulk back-fill logic here, e.g. bulk-update docs via a single db.batch().
  },
};

export const backFillPatchConfigs: BackFillPatchConfig[] = [
  sampleOneTimePatch,
];
