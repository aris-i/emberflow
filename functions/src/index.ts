import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {docPaths} from "./init-db-structure";
import {Entity} from "./custom/db-structure";
import {onDocChange} from "./index-utils";

admin.initializeApp();

Object.values(docPaths).forEach((path) => {
  const parts = path.split("/");
  const colPath = parts[parts.length - 1].replace(/{(\w+)Id}$/, "$1");
  const entity = colPath as Entity;

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Create`] = functions.firestore
    .document(path)
    .onCreate(async (snapshot, context) => {
      await onDocChange(entity, {before: null, after: snapshot}, context, "create");
    });

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Update`] = functions.firestore
    .document(path)
    .onUpdate(async (change, context) => {
      await onDocChange(entity, change, context, "update");
    });

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Delete`] = functions.firestore
    .document(path)
    .onDelete(async (snapshot, context) => {
      await onDocChange(entity, {before: snapshot, after: null}, context, "delete");
    });
});
