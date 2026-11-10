import {FieldValue} from "firebase-admin/firestore";

let warnedSplitFirestoreAdmin = false;

/**
 * Logs once when the injected Admin SDK's FieldValue is a different function
 * from the modular `firebase-admin/firestore` export. That happens when the
 * app has two copies of firebase-admin. `instanceof` then fails and sentinels
 * are dropped. A missing static (Functions emulator bind) is not a duplicate.
 * @param {object} adminSdk Admin SDK passed to `initializeEmberFlow`.
 * @return {void}
 */
export function warnIfSplitFirestoreAdmin(adminSdk: {firestore?: {FieldValue?: unknown}}): void {
  if (warnedSplitFirestoreAdmin) {
    return;
  }
  const namespaceFieldValue = adminSdk.firestore?.FieldValue;
  if (namespaceFieldValue !== undefined && namespaceFieldValue !== FieldValue) {
    warnedSplitFirestoreAdmin = true;
    console.warn(
      "Emberflow loaded firebase-admin/firestore from a different copy than admin.firestore. " +
      "Hoist a single firebase-admin that satisfies the peer range (^13.8.0). " +
      "Duplicate copies make instanceof FieldValue fail, and sentinels are dropped."
    );
  }
}
