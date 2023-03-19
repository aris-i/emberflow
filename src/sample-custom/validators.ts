import {Entity} from "./db-structure";
import {ValidationResult, ValidatorConfig} from "../types";
import {firestore} from "firebase-admin";
import DocumentData = firestore.DocumentData;

function userValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  return result;
}

function blankValidator(document: DocumentData): ValidationResult {
  return {};
}

const validatorConfig: ValidatorConfig = {
  [Entity.User]: userValidator,
  [Entity.YourCustomEntity]: blankValidator,
};

export {validatorConfig};
