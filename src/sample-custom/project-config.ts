import {ProjectConfig} from "../types";

export const projectConfig: ProjectConfig = {
  projectId: "your-project-id",
  region: "us-central1",
  rtdbName: "your-rtdb-name",
  budgetAlertTopicName: "budget-alerts",
  maxCostLimitPerFunction: 10,
  specialCostLimitPerFunction: {},
  // What a business-logic error does to the form status (default "finished", unchanged behaviour).
  // "thrown-only": end the form "error" when a logic threw; "all": when any logic result is "error".
  // logicErrorFormStatus: "thrown-only",
};
