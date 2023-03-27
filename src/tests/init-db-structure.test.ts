import {mapColPaths, mapDocPaths, mapViewDefinitions, traverseBFS} from "../init-db-structure";

describe("traverseBFS", () => {
  it("should traverse the object structure and return paths", () => {
    const input = {
      a: {
        b: {
          c: {},
          d: {},
        },
        e: {
          f: "View:b:prop1,prop2",
        },
      },
      h: {
        i: {
          "View:b:prop1,prop2": {},
        },
      },
    };

    const expectedOutput = [
      "a",
      "h",
      "a/b",
      "a/e",
      "h/i",
      "a/b/c",
      "a/b/d",
      "a/e#f=View:b:prop1,prop2",
      "h/i=View:b:prop1,prop2",
    ];

    const result = traverseBFS(input);
    expect(result).toEqual(expectedOutput);
  });
});

describe("mapDocPaths", () => {
  it("should map document paths based on the given entity object", () => {
    const paths = [
      "as",
      "hs",
      "as/a",
      "as/a/bs/b#f=View:Entity:prop1,prop2",
      "as/a/bs/b",
      "as/a/ds/d=View:Entity:prop1,prop2",
    ];

    const Entity = {
      Entity1: "a",
      Entity2: "b",
      Entity3: "d",
    };

    const expectedOutput = {
      "a": "as/{aId}",
      "b": "as/{aId}/bs/{bId}",
      "d": "as/{aId}/ds/{dId}",
    };

    const result = mapDocPaths(paths, Entity);
    expect(result).toEqual(expectedOutput);
  });
});

describe("mapViewPaths", () => {
  it("should map view paths based on the given paths and Entity object", () => {
    const paths = [
      "as",
      "hs",
      "as/a",
      "as/a/bs/b",
      "as/a/bs/b#f=View:a:prop1,prop2",
      "as/a/ds/d=View:a:prop1,prop2",
    ];

    const Entity = {
      Entity1: "a",
      Entity2: "b",
      Entity3: "d",
    };

    const docPaths= {
      "a": "as/{aId}",
      "b": "as/{aId}/bs/{bId}",
      "d": "as/{aId}/ds/{dId}",
    };

    const expectedOutput = [
      {
        destEntity: "b",
        destProp: "f",
        srcProps: ["prop1", "prop2"],
        srcEntity: "a",
      },
      {
        destEntity: "d",
        srcProps: ["prop1", "prop2"],
        srcEntity: "a",
      },
    ];

    const result = mapViewDefinitions(paths, Entity, docPaths);
    expect(result).toEqual(expectedOutput);
  });
});

describe("mapColPaths", () => {
  it("should map collection paths based on the given document paths", () => {
    const docPathsMap = {
      "c": "as/{aId}/bs/{bId}",
      "d": "as/{aId/ds/{dId}",
    };

    const expectedOutput = {
      "c": "as/{aId}/bs",
      "d": "as/{aId/ds",
    };

    const result = mapColPaths(docPathsMap);
    expect(result).toEqual(expectedOutput);
  });
});

