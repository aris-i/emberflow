import {Entity} from "./db-structure";
import {DocumentData} from "@google-cloud/firestore";

interface ValidationResult {
  [key: string]: string[];
}

interface ValidatorFn {
  (document: DocumentData): ValidationResult;
}

function userValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  return result;
}

function organizationValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  return result;
}

function projectValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  if (!data.description) {
    result["description"] = ["Description is required"];
  }
  return result;
}

function projectAccessListValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.userId) {
    result["userId"] = ["User ID is required"];
  }
  if (!data.projectId) {
    result["projectId"] = ["Project ID is required"];
  }
  return result;
}

function memberValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.userId) {
    result["userId"] = ["User ID is required"];
  }
  if (!data.organizationId) {
    result["organizationId"] = ["Organization ID is required"];
  }
  return result;
}

function formValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  return result;
}

function assetValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  if (!data.description) {
    result["description"] = ["Description is required"];
  }
  return result;
}

function assetAccessListValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.userId) {
    result["userId"] = ["User ID is required"];
  }
  if (!data.assetId) {
    result["assetId"] = ["Asset ID is required"];
  }
  return result;
}

function countryValidator(document: DocumentData): ValidationResult {
  const data = document;
  const result: ValidationResult = {};
  if (!data || !data.name) {
    result["name"] = ["Name is required"];
  }
  if (!data.code) {
    result["code"] = ["Code is required"];
  }
  return result;
}

const validators: Record<Entity, ValidatorFn> = {
  [Entity.User]: userValidator,
  [Entity.Organization]: organizationValidator,
  [Entity.Project]: projectValidator,
  [Entity.ProjectAccessList]: projectAccessListValidator,
  [Entity.Member]: memberValidator,
  [Entity.Form]: formValidator,
  [Entity.Asset]: assetValidator,
  [Entity.AssetAccessList]: assetAccessListValidator,
  [Entity.Country]: countryValidator,
};

export {validators};
