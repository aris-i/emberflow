import {firestore} from "firebase-admin";
import {rtdb} from "../index";
import Timestamp = firestore.Timestamp;
import GeoPoint = firestore.GeoPoint;
import {Request, Response} from "firebase-functions";
import Query = firestore.Query;
import {BatchUtil} from "./batch";

function isObject(item: any): boolean {
  return (typeof item === "object" && !Array.isArray(item) && item !== null);
}

function isTimestamp(item: object): boolean {
  const keys = Object.keys(item);
  return keys.length === 2 && keys.includes("_seconds") && keys.includes("_nanoseconds");
}

function isArray(item: any): boolean {
  return Array.isArray(item);
}

function isStringDate(item: any): boolean {
  return typeof item === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/.test(item);
}

function isFirestoreTimestamp(item: any): boolean {
  return item instanceof Timestamp;
}

function isFirestoreGeoPoint(item: any): boolean {
  return item instanceof GeoPoint;
}

export function deepEqual(a: any, b: any): boolean {
  if (a === b) return true;

  if (isFirestoreTimestamp(a) && isFirestoreTimestamp(b)) {
    return a.isEqual(b);
  }

  if (isFirestoreGeoPoint(a) && isFirestoreGeoPoint(b)) {
    return a.latitude === b.latitude && a.longitude === b.longitude;
  }

  if (isArray(a) && isArray(b)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) return false;
    }
    return true;
  }

  if (isObject(a) && isObject(b)) {
    const keysA = Object.keys(a);
    const keysB = Object.keys(b);

    if (keysA.length !== keysB.length) return false;

    for (const key of keysA) {
      if (!keysB.includes(key)) return false;
      if (!deepEqual(a[key], b[key])) return false;
    }

    return true;
  }

  return false;
}

export function computeHashCode(str: string) {
  let hashCode = 0;
  if (str.length === 0) {
    return hashCode.toString(16).padStart(8, "0"); // Pad with leading zeros to achieve 20 characters
  }
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hashCode = ((hashCode << 5) - hashCode + char) >>> 0; // Convert to unsigned 32-bit integer
  }
  return hashCode.toString(16).padStart(8, "0"); // Pad with leading zeros to achieve 20 characters and truncate to first 20 characters
}

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function deleteForms(request: Request, response: Response) {
  const userId = request.query.userId;

  if (!userId) {
    response.status(400).send("No ID provided");
    return;
  }

  const ref = rtdb.ref(`forms/${userId}`);
  const batchSize = 10;

  try {
    let lastKey = "";
    let lastBatch = false;
    while (!lastBatch) {
      let query = ref.orderByKey().limitToFirst(batchSize);
      if (lastKey) {
        query = query.startAt(lastKey);
      }

      const snapshot = await query.once("value");
      const data = snapshot.val();

      if (!data) {
        if (lastKey) {
          response.send(`Data with ID ${userId} deleted`);
        } else {
          response.status(404).send("No data found for the provided ID");
        }
        return;
      }

      const keys = Object.keys(data);
      lastKey = keys[keys.length - 1];

      for (const key of keys) {
        console.debug("Deleting: ", key);
        await ref.child(key).remove();
      }

      if (keys.length < batchSize) {
        lastBatch = true;
        response.send(`Data with ID ${userId} deleted`);
        return;
      }
    }
  } catch (error) {
    console.error("Error processing data:", error);
    response.status(500).send("Internal Server Error");
  }
}


export class LimitedSet<T> {
  private maxLength: number;
  private _set: Set<T>;
  private _queue: T[];
  constructor(maxLength: number) {
    this.maxLength = maxLength;
    this._set = new Set<T>();
    this._queue = [];
  }

  add(item: T) {
    if (this._set.has(item)) {
      return; // Item already exists, no need to add
    }

    if (this._queue.length === this.maxLength) {
      const oldestItem = this._queue.shift(); // Remove the oldest item from the queue
      if (oldestItem) {
        this._set.delete(oldestItem); // Remove the oldest item from the set
      }
    }

    this._queue.push(item); // Add the new item to the queue
    this._set.add(item); // Add the new item to the set
  }

  has(item: T) {
    return this._set.has(item);
  }

  delete(item: T) {
    if (!this._set.has(item)) {
      return false;
    }

    this._set.delete(item);
    const index = this._queue.indexOf(item);
    this._queue.splice(index, 1);
    return true;
  }

  get set(): Set<T> {
    return this._set;
  }

  get queue(): T[] {
    return this._queue;
  }
}


export async function deleteCollection(query: Query, callback?: (snapshot: firestore.QuerySnapshot) => void): Promise<void> {
  query = query.limit(500);
  return new Promise((resolve, reject) => {
    deleteQueryBatch(query, resolve, callback).catch(reject);
  });
}

async function deleteQueryBatch(query: Query, resolve: () => void, callback?: (snapshot: firestore.QuerySnapshot) => void): Promise<void> {
  const snapshot = await query.get();

  if (snapshot.size === 0) {
    resolve();
    return;
  }

  const batch = BatchUtil.getInstance();
  snapshot.docs.forEach( (doc) => {
    batch.deleteDoc(doc.ref);
  });

  await batch.commit();

  if (callback) {
    await callback(snapshot);
  }

  process.nextTick(() => {
    deleteQueryBatch(query, resolve, callback);
  });
}

export const reviveDateAndTimestamp = (json: { [key: string]: any }) => {
  const stack = [json];
  while (stack.length > 0) {
    const obj = stack.pop();

    for (const key in obj) {
      if (obj && Object.prototype.hasOwnProperty.call(obj, key)) {
        const value = obj[key];

        if (isArray(value)) {
          stack.push(value);
          continue;
        }

        if (isStringDate(value)) {
          obj[key] = new Date(value);
          continue;
        }

        if (isObject(value)) {
          if (isTimestamp(value)) {
            obj[key] = new Timestamp(value._seconds, value._nanoseconds);
            continue;
          }

          stack.push(value);
        }
      }
    }
  }

  return json;
};
