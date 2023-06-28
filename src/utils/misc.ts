import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;
import GeoPoint = firestore.GeoPoint;

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