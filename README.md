# Emberflow

Emberflow is a library for Firebase Functions that simplifies the process of setting up security, validation, and business logic. It provides a structured way to handle Firestore events and keep your data in sync across different collections.

## Features

- **Centralized Security**: Define security rules for your entities in one place.
- **Validation**: Ensure your data is valid before it's saved to Firestore.
- **Business Logics**: Define complex business rules that are automatically triggered by Firestore changes.
- **View Logics**: Easily create and maintain denormalized data (views) across your database.
- **Patch Logics**: Handle versioning and data migrations seamlessly.
- **Billing Protection**: Built-in budget monitoring to prevent unexpected costs.

## Usage

To use Emberflow in your Firebase Functions project, follow these steps:

### 1. Install Emberflow

Install Emberflow in your `functions` folder:

```sh
npm install emberflow
```

### 2. Initialize Emberflow

Import and initialize Emberflow in your Firebase Functions `index.ts` file. You'll need to provide your database structure, entities, security, validation, and logic configurations.

```typescript
import * as admin from "firebase-admin";
import { initializeEmberFlow } from "emberflow";
import { projectConfig } from "./project-config";
import { dbStructure, Entity } from "./db-structure";
import { securityConfigs } from "./security";
import { validatorConfigs } from "./validators";
import { logics } from "./business-logics";
import { patchLogicConfigs } from "./patch-logics";

admin.initializeApp();

const { functionsConfig } = initializeEmberFlow(
  projectConfig,
  admin,
  dbStructure,
  Entity,
  securityConfigs,
  validatorConfigs,
  logics,
  patchLogicConfigs
);

// Export the generated functions
Object.entries(functionsConfig).forEach(([key, value]) => {
  exports[key] = value;
});
```

## Configuration

Emberflow relies on several configuration objects to define how your project behaves.

### Database Structure (`dbStructure`)

Defines the hierarchy of your Firestore collections and how data is denormalized using views.

```typescript
import { propView, view } from "emberflow/utils/db-structure";

export const dbStructure = {
  users: {
    [Entity.User]: {
      feeds: {
        [Entity.Feed]: {
          createdBy: [propView("map", Entity.User, ["name", "email"])],
        },
      },
    },
  },
};
```

### Security Configs (`securityConfigs`)

Defines access control rules for each entity.

```typescript
export const securityConfigs: SecurityConfig[] = [
  {
    entity: Entity.User,
    securityFn: async (txnGet, entity, docPath, doc, actionType, modifiedFields, user) => {
      if (actionType === "delete") return { status: "rejected", message: "Deletes not allowed" };
      return { status: "allowed" };
    },
    version: "1",
  },
];
```

### Validator Configs (`validatorConfigs`)

Ensures data consistency before any logic is executed.

```typescript
export const validatorConfigs: ValidatorConfig[] = [
  {
    entity: Entity.User,
    validatorFn: async (document) => {
      const result: ValidationResult = {};
      if (!document.name) result["name"] = ["Name is required"];
      return result;
    },
    version: "1",
  },
];
```

### Business Logics (`logics`)

Defines the side effects of data changes.

```typescript
export const logics: LogicConfig[] = [
  {
    name: "EchoLogic",
    actionTypes: ["create", "update"],
    modifiedFields: "all",
    entities: "all",
    logicFn: async (txnGet, action, sharedMap) => {
      // Your business logic here
      return {
        name: "EchoLogic",
        status: "finished",
        documents: [], // documents to create/update/delete
      };
    },
    version: "1",
  },
];
```

## Reference

For more detailed examples on how to set up these configuration files, you can check the `src/sample-custom` folder in the Emberflow repository.

