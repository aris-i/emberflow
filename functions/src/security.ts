import {Entity} from "./db-structure";
import {SecurityConfig, SecurityResult, SecurityFn} from "./types";

// A security function that allows all actions
const allAllowed: SecurityFn = async (entity, doc, actionType, modifiedFields) => {
  console.log(`Security check for entity ${entity}, action type ${actionType}, and modified fields:`, modifiedFields);
  return {
    status: "allowed",
  };
};

export const securityConfig: SecurityConfig = {
  // Implement your security functions for each entity here
  [Entity.User]: async (entity, doc, actionType, modifiedFields): Promise<SecurityResult> => {
    switch (actionType) {
    case "create": {
      // Example: allow the user to create a new account only if they are registering from a whitelisted domain
      const email = doc["@form"].email;
      const domain = email.split("@")[1];
      const allowedDomains = ["example.com", "example.org"];
      let result: SecurityResult;
      if (!allowedDomains.includes(domain)) {
        result = {
          status: "rejected",
          message: `Registration is only allowed from these domains: ${allowedDomains.join(", ")}`,
        };
      } else {
        result = {
          status: "allowed",
        };
      }
      return result;
    }

    case "update": {
      // Example: do not allow the user to update their system role
      if (modifiedFields.includes("systemRole")) {
        return {
          status: "rejected",
          message: "User is not allowed to change his system role",
        };
      }
      break;
    }
    case "delete":
      // Example: reject all delete requests
      return {
        status: "rejected",
        message: "You are not allowed to delete this document",
      };
    }

    return {
      status: "allowed",
    };
  },
  [Entity.Organization]: allAllowed,
  [Entity.Project]: allAllowed,
  [Entity.ProjectAccessList]: allAllowed,
  [Entity.Member]: allAllowed,
  [Entity.Form]: allAllowed,
  [Entity.Asset]: allAllowed,
  [Entity.AssetAccessList]: allAllowed,
  [Entity.Country]: allAllowed,
};

