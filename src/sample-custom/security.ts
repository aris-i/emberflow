import {Entity} from "./db-structure";
// You should import from the path to the ember-flow package in your project
import {
  SecurityConfig,
  SecurityFn,
  SecurityResult,
} from "../types";

// A security function that allows all actions
const allAllowed: SecurityFn = async (
  entity,
  doc,
  actionType,
  modifiedFields
) => {
  console.log(`Security check for entity ${entity}, action type ${actionType},
   and modified fields:`, modifiedFields);
  return {
    status: "allowed",
  };
};

const userSecurityFn: SecurityFn =
    async (entity, form, doc, actionType, modifiedFields):
        Promise<SecurityResult> => {
      switch (actionType) {
      case "create": {
        // Example: allow the user to create a new account only
        // if they are registering from a whitelisted domain
        const email = form.email;
        const domain = email.split("@")[1];
        const allowedDomains = ["example.com", "example.org"];
        let result: SecurityResult;
        if (!allowedDomains.includes(domain)) {
          result = {
            status: "rejected",
            message: `Registration is only allowed from these domains:
             ${allowedDomains.join(", ")}`,
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
        if (modifiedFields?.includes("systemRole")) {
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
    };

export const securityConfig: SecurityConfig = {
  // Implement your security functions for each entity here
  [Entity.User]: userSecurityFn,
  [Entity.Feed]: allAllowed,
  [Entity.Friend]: allAllowed,
};

