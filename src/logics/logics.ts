import {
  Action,
  LogicConfig,
  LogicResultDoc,
} from "../types";
import {InternalEntity} from "../db-structure";

export const forDistributionLogicConfig: LogicConfig = {
  name: "ForDistribution Logic",
  actionTypes: ["create"],
  modifiedFields: "all",
  entities: [InternalEntity.ForDistribution],
  logicFn: async (action: Action) => {
    const {document} = action;
    const documents: LogicResultDoc[] = document.docsByDstPath;
    return {
      name: "For Distribution Logic Result",
      status: "finished",
      documents,
    };
  },
};
