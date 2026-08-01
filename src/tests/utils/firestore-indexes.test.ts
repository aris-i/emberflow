import {
  compositeIndexSignature,
  fieldOverrideIndexSignature,
  FirestoreIndexes,
  mergeFirestoreIndexes,
} from "../../utils/firestore-indexes";

describe("compositeIndexSignature", () => {
  it("defaults queryScope to COLLECTION and encodes fields in order", () => {
    expect(
      compositeIndexSignature({
        collectionGroup: "posts",
        fields: [
          {fieldPath: "authorId", order: "ASCENDING"},
          {fieldPath: "createdAt", order: "DESCENDING"},
        ],
      }),
    ).toBe("posts|COLLECTION|authorId:ASCENDING:,createdAt:DESCENDING:");
  });

  it("distinguishes indexes that differ only by query scope", () => {
    const collection = compositeIndexSignature({
      collectionGroup: "posts",
      queryScope: "COLLECTION",
      fields: [{fieldPath: "createdAt", order: "ASCENDING"}],
    });
    const group = compositeIndexSignature({
      collectionGroup: "posts",
      queryScope: "COLLECTION_GROUP",
      fields: [{fieldPath: "createdAt", order: "ASCENDING"}],
    });
    expect(collection).not.toBe(group);
  });
});

describe("fieldOverrideIndexSignature", () => {
  it("encodes scope, order and arrayConfig", () => {
    expect(fieldOverrideIndexSignature({queryScope: "COLLECTION_GROUP", order: "ASCENDING"}))
      .toBe("COLLECTION_GROUP|ASCENDING|");
    expect(fieldOverrideIndexSignature({queryScope: "COLLECTION", arrayConfig: "CONTAINS"}))
      .toBe("COLLECTION||CONTAINS");
  });
});

describe("mergeFirestoreIndexes", () => {
  const fragment: FirestoreIndexes = {
    indexes: [],
    fieldOverrides: [
      {
        collectionGroup: "computations",
        fieldPath: "createdAt",
        indexes: [
          {queryScope: "COLLECTION", order: "ASCENDING"},
          {queryScope: "COLLECTION", order: "DESCENDING"},
          {queryScope: "COLLECTION_GROUP", order: "ASCENDING"},
          {queryScope: "COLLECTION_GROUP", order: "DESCENDING"},
        ],
      },
    ],
  };

  it("adds all fragment overrides when the target is empty", () => {
    const merged = mergeFirestoreIndexes({indexes: [], fieldOverrides: []}, fragment);
    expect(merged.indexes).toEqual([]);
    expect(merged.fieldOverrides).toEqual(fragment.fieldOverrides);
  });

  it("treats missing indexes/fieldOverrides arrays as empty", () => {
    const merged = mergeFirestoreIndexes({}, {});
    expect(merged).toEqual({indexes: [], fieldOverrides: []});
  });

  it("preserves unrelated target overrides and appends the fragment's", () => {
    const target: FirestoreIndexes = {
      fieldOverrides: [
        {
          collectionGroup: "posts",
          fieldPath: "authorId",
          indexes: [{queryScope: "COLLECTION_GROUP", order: "ASCENDING"}],
        },
      ],
    };
    const merged = mergeFirestoreIndexes(target, fragment);
    expect(merged.fieldOverrides).toHaveLength(2);
    expect(merged.fieldOverrides[0].collectionGroup).toBe("posts");
    expect(merged.fieldOverrides[1].collectionGroup).toBe("computations");
  });

  it("unions scope rows for the same (collectionGroup, fieldPath) instead of duplicating", () => {
    const target: FirestoreIndexes = {
      fieldOverrides: [
        {
          collectionGroup: "computations",
          fieldPath: "createdAt",
          indexes: [
            // Already has the COLLECTION rows; missing the COLLECTION_GROUP ones.
            {queryScope: "COLLECTION", order: "ASCENDING"},
            {queryScope: "COLLECTION", order: "DESCENDING"},
          ],
        },
      ],
    };
    const merged = mergeFirestoreIndexes(target, fragment);
    // Still a single override entry for the field.
    expect(merged.fieldOverrides).toHaveLength(1);
    // The COLLECTION_GROUP rows from the fragment are unioned in; no duplicate COLLECTION rows.
    expect(merged.fieldOverrides[0].indexes).toEqual([
      {queryScope: "COLLECTION", order: "ASCENDING"},
      {queryScope: "COLLECTION", order: "DESCENDING"},
      {queryScope: "COLLECTION_GROUP", order: "ASCENDING"},
      {queryScope: "COLLECTION_GROUP", order: "DESCENDING"},
    ]);
  });

  it("preserves extra keys (e.g. Firebase's \"ttl\": false) on target overrides", () => {
    const target: FirestoreIndexes = {
      fieldOverrides: [
        // Firebase emits "ttl": false on every fieldOverride; it must survive the merge.
        {
          collectionGroup: "posts",
          fieldPath: "authorId",
          ttl: false,
          indexes: [{queryScope: "COLLECTION_GROUP", order: "ASCENDING"}],
        },
      ],
    };
    const merged = mergeFirestoreIndexes(target, fragment);
    expect(merged.fieldOverrides[0]).toEqual({
      collectionGroup: "posts",
      fieldPath: "authorId",
      ttl: false,
      indexes: [{queryScope: "COLLECTION_GROUP", order: "ASCENDING"}],
    });
  });

  it("de-duplicates composite indexes by signature", () => {
    const index = {
      collectionGroup: "posts",
      queryScope: "COLLECTION" as const,
      fields: [{fieldPath: "createdAt", order: "ASCENDING" as const}],
    };
    const merged = mergeFirestoreIndexes({indexes: [index]}, {indexes: [{...index}]});
    expect(merged.indexes).toHaveLength(1);
  });

  it("does not mutate the input objects", () => {
    const target: FirestoreIndexes = {
      fieldOverrides: [
        {
          collectionGroup: "computations",
          fieldPath: "createdAt",
          indexes: [{queryScope: "COLLECTION", order: "ASCENDING"}],
        },
      ],
    };
    mergeFirestoreIndexes(target, fragment);
    expect(target.fieldOverrides?.[0].indexes).toHaveLength(1);
  });
});
