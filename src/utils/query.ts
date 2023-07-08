import {db} from "../index";
import {firestore} from "firebase-admin";
import {QueryCondition} from "../types";
import DocumentData = firestore.DocumentData;
import Query = firestore.Query;

export async function fetchIds(collectionPath: string, condition?: QueryCondition) {
  const ids: string[] = [];
  const baseQuery = db.collection(collectionPath).select(firestore.FieldPath.documentId());

  async function executeQuery(query: Query<DocumentData>) {
    const querySnapshot = await query.get();
    querySnapshot.docs.forEach((doc) => {
      ids.push(doc.id);
    });
  }

  if (
    condition &&
      (
        condition.operator === "in" ||
          condition.operator === "not-in" ||
          condition.operator === "array-contains-any"
      )
  ) {
    const chunkSize = 10;
    const values = condition.value;
    const chunks = [];
    for (let i = 0; i < values.length; i += chunkSize) {
      chunks.push(values.slice(i, i + chunkSize));
    }
    const promises = chunks.map((chunk) => {
      const query = baseQuery.where(condition.fieldName, condition.operator, chunk);
      return executeQuery(query);
    });
    await Promise.all(promises);
  } else {
    let query = baseQuery;
    if (condition) {
      query = query.where(condition.fieldName, condition.operator, condition.value);
    }
    await executeQuery(query);
  }

  return ids;
}
