# Emberflow

Emberflow is a library for Firebase Functions that simplifies the process of setting up security, validation, and business logic. It provides a structured way to handle Firestore events and keep your data in sync across different collections.

## Features

- **Centralized Security**: Define security rules for your entities in one place.
- **Validation**: Ensure your data is valid before it's saved to Firestore.
- **Business Logics**: Define complex business rules that are automatically triggered by Firestore changes.
- **View Logics**: Easily create and maintain denormalized data (views) across your database.
- **Patch Logics**: Handle versioning and data migrations seamlessly.
- **Group Patch Engine**: Run one-time back-fills or bulk patch-logics runs over an entire collection, with progress tracking.
- **Pluggable Cleanup**: Automatically purge stale documents on a schedule using declarative, config-driven cleanup rules.
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
import { cleanupConfigs } from "./cleanup-configs";
import { backFillPatchConfigs } from "./one-time-patches";

admin.initializeApp();

const { functionsConfig } = initializeEmberFlow({
  projectConfig,
  admin,
  dbStructure,
  Entity,
  securityConfigs,
  validatorConfigs,
  logicConfigs: logics,
  patchLogicConfigs,
  cleanupConfigs, // optional, see "Collection Cleanup" below
  backFillPatchConfigs, // optional, see "Group Patch Engine" below
});

// Export the generated functions
Object.entries(functionsConfig).forEach(([key, value]) => {
  exports[key] = value;
});
```

`initializeEmberFlow` takes a single options object (`InitializeEmberFlowOptions`). `projectConfig`, `admin`, `dbStructure`, `Entity`, `securityConfigs`, `validatorConfigs`, `logicConfigs`, and `patchLogicConfigs` are required; `cleanupConfigs`, `backFillPatchConfigs`, and `userRegisterFn` are optional.

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
- **Triggering**: There are two ways patch logics run:
    - **Automatic, per-document (default)**: You only register `patchLogicConfigs`. Emberflow
      automatically queues and runs them for a single document during form submissions or
      document distribution whenever a version mismatch is detected — projects don't call
      anything directly here.
    - **On-demand, collection-wide (bulk migration)**: To re-run patch logics across every
      existing document in a collection, call the public `queueGroupPatch({ path, patchType:
      "patch-logics", appVersion })` (see the [Group Patch Engine](#group-patch-engine-queuegrouppatch-getgrouppatchprogress-backfillpatchconfig)
      below). Progress can be polled with `getGroupPatchProgress`.
- **Asynchronous Execution**: They run asynchronously via Pub/Sub to ensure high performance.
- **Version-gating**: In both cases a `PatchLogicConfig` only fires when `config.version <=`
  the current `appVersion` **and** the document's `@dataVersion` is older than `config.version`.
  After a successful patch, the document's `@dataVersion` is bumped so it won't run again.
- **Versioning**:
    - **`@dataVersion`**: Incremented automatically after a patch is successfully applied.
    - **`minDataVersion`**: In `LogicConfig`, use this to ensure business logic only runs on compatible data.
    - **`obsoleteStartingFromVersion`**: In `LogicConfig`, use this to retire old logic based on the `appVersion`.
- **Transactions**: Executed within Firestore transactions to ensure data integrity.

### Group Patch Engine (`queueGroupPatch`, `getGroupPatchProgress`, `BackFillPatchConfig`)

Emberflow ships with a generic, collection-wide batch engine for running a patch over every
document in a collection (paging 500 docs at a time, tracking progress, and self-rescheduling
over Pub/Sub until done). Each run is identified by a `patchType`:

- **`"back-fill"`**: a version-free, one-time bulk back-fill. The actual work is delegated to a
  `BackFillPatchConfig` resolved by `backFillPatchName`. Emberflow ships with a built-in
  `ancestorIdsPatchConfig` (named `"ancestor-ids"`) that populates the `@entity`/ancestor id
  fields used internally — it is **always registered automatically**, so you can trigger it with
  `queueGroupPatch` at any time without registering it yourself. `appVersion` is **not** used for
  `back-fill` runs (it's only required for `"patch-logics"`).
- **`"patch-logics"`**: runs `runPatchLogics(appVersion, path)` for every document in the
  collection, useful for bulk-applying `patchLogicConfigs` migrations.

#### 1. Register a custom `BackFillPatchConfig`

```typescript
import { BackFillPatchConfig } from "emberflow/src/types";

const myBackFill: BackFillPatchConfig = {
  name: "my-back-fill", // used as backFillPatchName; must be unique
  patchFn: async (collectionPath, docs) => {
    // Bulk-update `docs` here, e.g. via a single db.batch() commit.
  },
};

export const backFillPatchConfigs: BackFillPatchConfig[] = [myBackFill];
```

Pass `backFillPatchConfigs` to `initializeEmberFlow` (see step 2 above). The built-in
`"ancestor-ids"` config is always registered automatically; registering a config with a
duplicate name, or the reserved name `"ancestor-ids"`, throws during initialization.

#### 2. Trigger a group patch

```typescript
import { queueGroupPatch } from "emberflow";

// Kick off the built-in ancestor-ids back-fill for a collection
await queueGroupPatch({
  path: "/users/user123/feeds",
  patchType: "back-fill",
  backFillPatchName: "ancestor-ids",
});

// Kick off your own back-fill
await queueGroupPatch({
  path: "/users/user123/feeds",
  patchType: "back-fill",
  backFillPatchName: "my-back-fill",
});

// Bulk-apply patch logics for a target appVersion
await queueGroupPatch({
  path: "/users/user123/feeds",
  patchType: "patch-logics",
  appVersion: "1.2.0",
});
```

`queueGroupPatch` accepts either a collection path or a document path (in which case the parent
collection is derived). There is no guard/auth on it — it's meant to be called from your own
trusted code (e.g. an admin-only Cloud Function or script). Placeholder paths (e.g.
`/users/{userId}/feeds`) are automatically hydrated into concrete collection paths before patching.

> If `backFillPatchName` doesn't resolve to a registered `BackFillPatchConfig` (or a
> `"patch-logics"` run is missing its `appVersion`), the run is set to status `"error"` — there is
> no silent default.

#### 3. Track progress

```typescript
import { getGroupPatchProgress } from "emberflow";

const progress = await getGroupPatchProgress({
  collectionPath: "/users/user123/feeds",
  patchType: "back-fill",
  backFillPatchName: "ancestor-ids",
});
// progress?.status -> "running" | "completed" | "error" | "reset"
```

Progress is tracked independently per `patchType`/`backFillPatchName`, so different patches
running against the same collection never clobber each other's status. The status doc lives at
`@emberflow/internal/group-patches/<collection>_back-fill_<backFillPatchName>` (or
`..._patch-logics` for `"patch-logics"` runs).

### Collection Cleanup (`cleanupConfigs`, `CleanupConfig`)

Emberflow ships with a single, scheduled `cleanupCollections` Cloud Function that runs **every
hour** and purges stale documents based on declarative rules. Instead of writing a bespoke
scheduled function for each collection you want to prune, you describe *what* to delete with a
`CleanupConfig` and Emberflow handles the *how* (querying, batching, recursive subtree deletion,
and self-paced iteration).

Two layers of rules are merged and executed by the same runner:

1. **Built-in (framework) rules** — always active. They keep Emberflow's own internal
   bookkeeping collections tidy (Pub/Sub `processedIds`, metric `executions`/`computations`,
   view-logic executions, and `@actions` — the latter also nulls out the corresponding
   `forms/{uid}/{formId}` entries in the Realtime Database).
2. **Project-supplied rules** — whatever you pass via the optional `cleanupConfigs` init option.
   These are appended to the built-in rules, so your rules run alongside them.

#### 1. Define a `CleanupConfig`

```typescript
import { CleanupConfig } from "emberflow/src/types";

export const cleanupConfigs: CleanupConfig[] = [
  {
    // Exact collection path, or a collection-group name when isCollectionGroup=true.
    collectionPath: "askJaris",
    // Match this subcollection name anywhere in Firestore (collection-group query).
    isCollectionGroup: true,
    // Timestamp/Date field compared against the computed cutoff.
    timestampField: "createdAt",
    // Delete docs whose timestampField is older than (value · unit).
    // unit is one of "hours" | "days" | "months".
    olderThan: { value: 1, unit: "months" },
    // Optional extra server-side filters, ANDed with the age threshold.
    conditions: [
      { fieldName: "hasTopic", operator: "==", value: false },
    ],
    // recursive defaults to true: each matched doc is deleted with its whole
    // subtree. Set to false to delete only the matched documents.
    recursive: true,
  },
];
```

Then pass `cleanupConfigs` to `initializeEmberFlow` (see step 2 in **Usage** above).

#### 2. How it Works

- **Scheduling**: A single `cleanupCollections` scheduled function runs `every 1 hours`. You can
  override its schedule/region/memory/timeout via `projectConfig.functionsConfig.cleanupCollections`.
- **Cutoff computation**: `olderThan` is converted to a cutoff `Date`; documents whose
  `timestampField` is `< cutoff` are selected. `"months"` is calendar-aware (it subtracts
  calendar months rather than a fixed number of days).
- **Extra filters (`conditions`)**: Optional `QueryCondition` entries (`{ fieldName, operator,
  value }`) are ANDed with the age threshold as additional server-side `where` clauses.
- **Deletion mode (`recursive`)**: Defaults to `true`, deleting each matched document together
  with its entire subtree. Set `recursive: false` to delete only the matched documents (leaving
  any subcollections untouched).
- **Isolation**: Each config is executed independently inside its own `try/catch`, so a failure
  in one rule (e.g. a missing index) won't stop the others; failures are logged and the number of
  deleted documents per collection is reported to the logs.

> **Composite indexes**: Collection-group queries and any `conditions` combined with the
> timestamp filter may require composite Firestore indexes in your project. If a rule fails,
> check the function logs for an index-creation link.

## Reference

For more detailed examples on how to set up these configuration files, you can check the `src/sample-custom` folder in the Emberflow repository.

