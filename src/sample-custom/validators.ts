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

export const validatorConfigs: ValidatorConfig[] = [
  {
    entity: Entity.User,
    validatorFn: userValidator,
    version: "1.2.3",
  },
  {
    entity: Entity.User,
    validatorFn: userValidator,
    version: "2.4.6",
  },
  {
    entity: Entity.User,
    validatorFn: userValidator,
    version: "3",
  },
  {
    entity: Entity.Feed,
    validatorFn: blankValidator,
    version: "1",
  },
  {
    entity: Entity.Friend,
    validatorFn: blankValidator,
    version: "1",
  },
];
