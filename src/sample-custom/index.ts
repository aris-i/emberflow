import * as admin from "firebase-admin";
import {initializeEmberFlow} from "../index";
import {projectConfig} from "./project-config";
import {dbStructure, Entity} from "./db-structure";
import {securityConfigs} from "./security";
import {validatorConfigs} from "./validators";
import {logics} from "./business-logics";
import {patchLogicConfigs} from "./patch-logics";
import {backFillPatchConfigs} from "./one-time-patches";

admin.initializeApp();

const {functionsConfig} = initializeEmberFlow({
  projectConfig,
  admin,
  dbStructure,
  Entity,
  securityConfigs,
  validatorConfigs,
  logicConfigs: logics,
  patchLogicConfigs,
  backFillPatchConfigs,
});

// Export the generated functions
Object.entries(functionsConfig).forEach(([key, value]) => {
  exports[key] = value;
});
