import {Entity} from "./db-structure";
import {firestore} from "firebase-admin";
import DocumentData = firestore.DocumentData;
// You should import from the path to the ember-flow package in your project
import {ValidationResult, ValidatorConfig} from "../types";

/**
 * Validates the user document.
 *
 * @param {DocumentData} document - The document data to validate.
 * @return {Promise<ValidationResult>} An object containing validation errors, if any.
 */
async function userValidator(document: DocumentData): Promise<ValidationResult> {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  return Promise.resolve(result);
}

/**
 * A blank validator that always returns an empty ValidationResult object.
 *
 * @param {DocumentData} document - The document data to validate.
 * @return {Promise<ValidationResult>} An empty ValidationResult object.
 */
async function blankValidator(document: DocumentData): Promise<ValidationResult> {
  return Promise.resolve({});
}

/**
 * The validator configuration object, mapping entity names to their respective
 * validator functions.
 *
 * @type {ValidatorConfig}
 */
const validatorConfig: ValidatorConfig = {
  [Entity.User]: userValidator,
  [Entity.Feed]: blankValidator,
  [Entity.Friend]: blankValidator,
};

export {validatorConfig};
