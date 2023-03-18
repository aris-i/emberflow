import {DocumentData} from "@google-cloud/firestore";
import {Entity} from "./db-structure";
import {ValidationResult, ValidatorFn} from "../types";

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

const validators: Record<Entity, ValidatorFn> = {
  [Entity.User]: userValidator,
  [Entity.YourCustomEntity]: blankValidator,
};

export {validators};
