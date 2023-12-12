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

function isArray(item: any): boolean {
  return Array.isArray(item);
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

export function deleteForms(request: Request, response: Response) {
  const userId = request.query.userId;

  if (!userId) {
    response.status(400).send("No ID provided");
    return;
  }

  const ref = rtdb.ref(`forms/${userId}`);
  ref.remove()
    .then(() => {
      response.send(`Data with ID ${userId} deleted`);
    })
    .catch((error) => {
      console.error("Error deleting data:", error);
      response.status(500).send("Internal Server Error");
    });
}


export class LimitedSet<T> {
  private maxLength: number;
  private set: Set<T>;
  private queue: T[];
  constructor(maxLength: number) {
    this.maxLength = maxLength;
    this.set = new Set<T>();
    this.queue = [];
  }

  add(item: T) {
    if (this.set.has(item)) {
      return; // Item already exists, no need to add
    }

    if (this.queue.length === this.maxLength) {
      const oldestItem = this.queue.shift(); // Remove the oldest item from the queue
      if (oldestItem) {
        this.set.delete(oldestItem); // Remove the oldest item from the set
      }
    }

    this.queue.push(item); // Add the new item to the queue
    this.set.add(item); // Add the new item to the set
  }

  has(item: T) {
    return this.set.has(item);
  }

  delete(item: T) {
    if (!this.set.has(item)) {
      return false;
    }

    this.set.delete(item);
    const index = this.queue.indexOf(item);
    this.queue.splice(index, 1);
    return true;
  }
}


export async function deleteCollection(query: Query): Promise<void> {
  query = query.limit(500);
  return new Promise((resolve, reject) => {
    deleteQueryBatch(query, resolve).catch(reject);
  });
}

async function deleteQueryBatch(query: Query, resolve: () => void): Promise<void> {
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

  process.nextTick(() => {
    deleteQueryBatch(query, resolve);
  });
}

