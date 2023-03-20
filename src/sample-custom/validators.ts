import {Entity} from "./db-structure";
import {firestore} from "firebase-admin";
import DocumentData = firestore.DocumentData;
import {ValidationResult, ValidatorConfig} from "@primeiq/emberflow/lib/types";

/**
 * Validates the user document.
 *
 * @param {DocumentData} document - The document data to validate.
 * @return {ValidationResult} An object containing validation errors, if any.
 */
function userValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  return result;
}

/**
 * A blank validator that always returns an empty ValidationResult object.
 *
 * @param {DocumentData} document - The document data to validate.
 * @return {ValidationResult} An empty ValidationResult object.
 */
function blankValidator(document: DocumentData): ValidationResult {
  return {};
}

/**
 * The validator configuration object, mapping entity names to their respective
 * validator functions.
 *
 * @type {ValidatorConfig}
 */
const validatorConfig: ValidatorConfig = {
  [Entity.User]: userValidator,
  [Entity.YourCustomEntity]: blankValidator,
};

export {validatorConfig};
