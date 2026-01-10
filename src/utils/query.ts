import {db} from "../index";
import {firestore} from "firebase-admin";
import {ChunkableWhereFilterOp, QueryCondition} from "../types";
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
        ["in", "not-in", "array-contains-any"].includes(condition.operator)
      )
  ) {
    const chunkSize = 30;
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

  return Array.from(new Set(ids));
}

export async function chunkQuery(
  baseQuery: firestore.Query,
  fieldName: string,
  operator: ChunkableWhereFilterOp,
  values: any[],
  limitPerBatch?: number,
  lastDocSnap?: firestore.DocumentSnapshot
): Promise<firestore.QueryDocumentSnapshot[]> {
  if (values.length <= 30) {
    let q = baseQuery.where(fieldName, operator, values);
    if (limitPerBatch) {
      q = q.limit(limitPerBatch);
    }
    if (lastDocSnap?.exists) {
      q = q.startAfter(lastDocSnap);
    }
    return (await q.get()).docs;
  }

  const chunkSize = 30;
  const chunks = [];
  for (let i = 0; i < values.length; i += chunkSize) {
    chunks.push(values.slice(i, i + chunkSize));
  }

  const queryPromises = chunks.map(async (chunk) => {
    let q = baseQuery.where(fieldName, operator, chunk);
    if (limitPerBatch) {
      q = q.limit(limitPerBatch);
    }
    if (lastDocSnap?.exists) {
      q = q.startAfter(lastDocSnap);
    }
    return (await q.get()).docs;
  });

  const results = await Promise.all(queryPromises);
  const uniqueDocs = new Map<string, firestore.QueryDocumentSnapshot>();
  for (const docs of results) {
    for (const doc of docs) {
      uniqueDocs.set(doc.id, doc);
    }
  }

  return Array.from(uniqueDocs.values()).sort((a, b) => a.id.localeCompare(b.id));
}
