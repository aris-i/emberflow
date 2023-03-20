# Emberflow

Emberflow is a library for Firebase Functions that simplifies the process of setting up security, validation, and business logic.

## Usage

To use Emberflow in your Firebase Functions project, follow these steps:

1. Install Emberflow:

```sh
npm install @primeiq/emberflow
```

2. Import and initialize Emberflow in your Firebase Functions `index.ts` file:

```typescript
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "./db-structure";
import {initializeEmberFlow} from "@primeiq/emberflow";
import {securityConfig} from "./security";
import {validatorConfig} from "./validators";
import {logics} from "./business-logics";

admin.initializeApp();
const {functionsConfig} = initializeEmberFlow(
  admin,
  dbStructure,
  Entity,
  securityConfig,
  validatorConfig,
  logics
);

Object.entries(functionsConfig).forEach(([key, value]) => {
  exports[key] = value;
});
```
This example assumes you have the necessary files **(db-structure.ts, security.ts, validators.ts, and business-logics.ts)** in your project. For reference on how to set up these files, you can check the **src.sample-custom** folder in the Emberflow library.

