import {Entity} from "./db-structure";
import {firestore} from "firebase-admin";
import DocumentData = firestore.DocumentData;
import {ValidationResult, ValidatorConfig} from "../types";

/**
 * Validates the user document.
 *
 * @param {DocumentData} document - The document data to validate.
 * @param {string} docPath - The path to the document.
 * @return {Promise<ValidationResult>} An object containing validation errors, if any.
 */
async function userValidator(document: DocumentData, docPath: string): Promise<ValidationResult> {
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
 * @param {string} docPath - The path to the document.
 * @return {Promise<ValidationResult>} An empty ValidationResult object.
 */
async function blankValidator(document: DocumentData, docPath: string): Promise<ValidationResult> {
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
