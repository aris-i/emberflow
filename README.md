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

### Patch Logics (`patchLogicConfigs`)

Patch logics handle data migrations and versioning. They are triggered when a document's `@dataVersion` is lower than the required version defined in the patch logic configurations.

#### 1. Define the `PatchLogicFn`
A patch logic function transforms existing document data to a new version.

```typescript
import { PatchLogicFn, LogicResult } from "emberflow/src/types";

const updateUserData: PatchLogicFn = async (dstPath, data) => {
  const { fullName } = data;
  const [firstName, lastName] = fullName.split(" ");

  return {
    name: "updateUserData",
    status: "finished",
    documents: [
      {
        action: "merge",
        dstPath: dstPath,
        doc: { firstName, lastName },
        instructions: { fullName: "del" },
      },
    ],
  };
};
```

#### 2. Configure the `PatchLogicConfig`
Register the patch logic for a specific entity and version.

```typescript
import { PatchLogicConfig } from "emberflow/src/types";

export const patchLogicConfigs: PatchLogicConfig[] = [
  {
    name: "updateUserData",
    entity: "User",
    patchLogicFn: updateUserData,
    version: "1.1.0", // The version this patch achieves
  },
];
```

#### 3. How it Works
- **Triggering**: Patch logics are automatically queued during form submissions or document distribution if a version mismatch is detected.
- **Asynchronous Execution**: They run asynchronously via Pub/Sub to ensure high performance.
- **Versioning**:
    - **`@dataVersion`**: Incremented automatically after a patch is successfully applied.
    - **`minDataVersion`**: In `LogicConfig`, use this to ensure business logic only runs on compatible data.
    - **`obsoleteStartingFromVersion`**: In `LogicConfig`, use this to retire old logic based on the `appVersion`.
- **Transactions**: Executed within Firestore transactions to ensure data integrity.

## Reference

For more detailed examples on how to set up these configuration files, you can check the `src/sample-custom` folder in the Emberflow repository.

