export type FirestoreIndexOrder = "ASCENDING" | "DESCENDING";
export type FirestoreIndexArrayConfig = "CONTAINS";
export type FirestoreIndexQueryScope =
  | "COLLECTION"
  | "COLLECTION_GROUP"
  | "COLLECTION_RECURSIVE";

/** A single field entry within a composite index (matches firestore.indexes.json). */
export interface FirestoreIndexField {
  fieldPath: string;
  order?: FirestoreIndexOrder;
  arrayConfig?: FirestoreIndexArrayConfig;
}

/** A composite index definition (matches firestore.indexes.json "indexes" entries). */
export interface FirestoreCompositeIndex {
  collectionGroup: string;
  /** Defaults to "COLLECTION" when omitted, mirroring Firestore. */
  queryScope?: FirestoreIndexQueryScope;
  fields: FirestoreIndexField[];
  // Preserve any additional keys Firebase may emit (e.g. "density", "multikey").
  [key: string]: unknown;
}

/** A single scope/order entry for a single-field override. */
export interface FirestoreFieldOverrideIndex {
  queryScope: FirestoreIndexQueryScope;
  order?: FirestoreIndexOrder;
  arrayConfig?: FirestoreIndexArrayConfig;
}

/** A single-field index override (matches firestore.indexes.json "fieldOverrides" entries). */
export interface FirestoreFieldOverride {
  collectionGroup: string;
  fieldPath: string;
  indexes: FirestoreFieldOverrideIndex[];
  // Firebase emits "ttl": false on every fieldOverride; preserve it (and any other
  // keys Firebase may add) so merging does not strip properties from the file.
  ttl?: boolean;
  [key: string]: unknown;
}

/** Firestore index configuration, shaped exactly like a firestore.indexes.json file. */
export interface FirestoreIndexes {
  indexes?: FirestoreCompositeIndex[];
  fieldOverrides?: FirestoreFieldOverride[];
}

// Stable signature for a composite index, used to de-duplicate composite indexes
// when merging. Two composite indexes with the same collection group, query scope
// and (ordered) field list are considered identical.
export function compositeIndexSignature(index: FirestoreCompositeIndex): string {
  const scope = index.queryScope ?? "COLLECTION";
  const fields = (index.fields ?? [])
    .map((field) => `${field.fieldPath}:${field.order ?? ""}:${field.arrayConfig ?? ""}`)
    .join(",");
  return `${index.collectionGroup}|${scope}|${fields}`;
}

// Stable signature for a single scope/order row of a field override, used to union
// the scope rows of two overrides that target the same (collectionGroup, fieldPath).
export function fieldOverrideIndexSignature(index: FirestoreFieldOverrideIndex): string {
  return `${index.queryScope}|${index.order ?? ""}|${index.arrayConfig ?? ""}`;
}

function fieldOverrideKey(override: FirestoreFieldOverride): string {
  return `${override.collectionGroup}|${override.fieldPath}`;
}

// Merge Emberflow's index fragment into a target firestore.indexes.json object.
//
// - Composite `indexes` are unioned and de-duplicated by compositeIndexSignature.
// - `fieldOverrides` are keyed by `(collectionGroup, fieldPath)`. A field override
//   *replaces* Firestore's whole single-field index config for that field, so two
//   overrides for the same field must never coexist. Instead, their scope/order rows
//   are unioned into a single entry (de-duplicated by fieldOverrideIndexSignature),
//   preserving both sides' scopes.
//
// The `target` entries are kept first and in order; fragment-only entries are appended.
// The inputs are never mutated.
export function mergeFirestoreIndexes(
  target: FirestoreIndexes,
  fragment: FirestoreIndexes,
): Required<FirestoreIndexes> {
  const mergedIndexes: FirestoreCompositeIndex[] = [];
  const seenIndexSignatures = new Set<string>();
  for (const index of [...(target.indexes ?? []), ...(fragment.indexes ?? [])]) {
    const signature = compositeIndexSignature(index);
    if (seenIndexSignatures.has(signature)) {
      continue;
    }
    seenIndexSignatures.add(signature);
    mergedIndexes.push(index);
  }

  const mergedOverrides: FirestoreFieldOverride[] = [];
  const overridesByKey = new Map<string, FirestoreFieldOverride>();
  for (const override of [...(target.fieldOverrides ?? []), ...(fragment.fieldOverrides ?? [])]) {
    const key = fieldOverrideKey(override);
    const existing = overridesByKey.get(key);
    if (!existing) {
      // Spread the whole override so unknown keys (e.g. Firebase's "ttl": false)
      // are preserved; only the indexes array is copied to avoid shared mutation.
      const copy: FirestoreFieldOverride = {
        ...override,
        indexes: [...(override.indexes ?? [])],
      };
      overridesByKey.set(key, copy);
      mergedOverrides.push(copy);
      continue;
    }
    const seenScopes = new Set(existing.indexes.map(fieldOverrideIndexSignature));
    for (const scopeRow of override.indexes ?? []) {
      const scopeSignature = fieldOverrideIndexSignature(scopeRow);
      if (seenScopes.has(scopeSignature)) {
        continue;
      }
      seenScopes.add(scopeSignature);
      existing.indexes.push(scopeRow);
    }
  }

  return {
    indexes: mergedIndexes,
    fieldOverrides: mergedOverrides,
  };
}
