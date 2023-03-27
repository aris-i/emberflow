import {admin} from "../index";
import {firestore} from "firebase-admin";
import {QueryCondition} from "../types";

export async function fetchIds(collectionPath: string, condition?: QueryCondition) {
  const ids: string[] = [];
  let query = admin.firestore().collection(collectionPath).select(firestore.FieldPath.documentId());
  if (condition) {
    query = query.where(condition.fieldName, condition.operator, condition.value);
  }
  const querySnapshot = await query.get();
  querySnapshot.forEach((doc) => {
    ids.push(doc.id);
  });
  return ids;
}
