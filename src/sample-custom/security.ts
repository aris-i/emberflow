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
  modifiedFields,
  user,
) => {
  console.log(`Security check for entity ${entity}, action type ${actionType},
   and modified fields:`, modifiedFields);
  return {
    status: "allowed",
  };
};

const userSecurityFn: SecurityFn =
    async (
      txnGet,
      entity,
      docPath,
      doc,
      actionType,
      modifiedFields,
      user):
        Promise<SecurityResult> => {
      switch (actionType) {
      case "create": {
        // Example: allow the user to create a new account only
        // if they are registering from a whitelisted domain
        const email = modifiedFields.email;
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

export const securityConfigs: SecurityConfig[] = [
  {
    entity: Entity.User,
    securityFn: userSecurityFn,
    version: "1",
  },
  {
    entity: Entity.Feed,
    securityFn: allAllowed,
    version: "1",
  },
  {
    entity: Entity.Friend,
    securityFn: allAllowed,
    version: "1",
  },
];

