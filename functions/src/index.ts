import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {docPaths} from "./db-structure";

admin.initializeApp();

function onDocCreate(snapshot: functions.firestore.DocumentSnapshot, context: functions.EventContext) {
  const documentId = context.params[Object.keys(context.params)[Object.keys(context.params).length - 1]];
  console.log(`Document created in ${context.resource.service.split("/")[6]} collection with ID ${documentId}`);
}

function onDocUpdate(change: functions.Change<functions.firestore.DocumentSnapshot>, context: functions.EventContext) {
  const documentId = context.params[Object.keys(context.params)[Object.keys(context.params).length - 1]];
  console.log(`Document updated in ${context.resource.service.split("/")[6]} collection with ID ${documentId}`);
}

function onDocDelete(snapshot: functions.firestore.DocumentSnapshot, context: functions.EventContext) {
  const documentId = context.params[Object.keys(context.params)[Object.keys(context.params).length - 1]];
  console.log(`Document deleted in ${context.resource.service.split("/")[6]} collection with ID ${documentId}`);
}

Object.values(docPaths).forEach((path) => {
  const parts = path.split("/");
  const colPath = parts[parts.length - 1].replace(/{(\w+)Id}$/, "$1");


  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Create`] = functions.firestore
    .document(path)
    .onCreate(onDocCreate);

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Update`] = functions.firestore
    .document(path)
    .onUpdate(onDocUpdate);

  exports[`on${colPath.charAt(0).toUpperCase() + colPath.slice(1)}Delete`] = functions.firestore
    .document(path)
    .onDelete(onDocDelete);
});
