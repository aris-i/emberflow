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
          g: ["View:b:prop1,prop2"],
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
      "a/e#g=[View:b:prop1,prop2]",
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
      "hs/h",
      "as/a/cs",
      "as/a/cs/c",
      "as/a/cs/c#f=View:a:prop1,prop2",
      "hs/h=View:a:prop1,prop2",
    ];

    const Entity = {
      Entity1: "a",
      Entity2: "h",
      Entity3: "c",
    };

    const expectedOutput = {
      "a": "as/{aId}",
      "h": "hs/{hId}",
      "c": "as/{aId}/cs/{cId}",
    };

    const result = mapDocPaths(paths, Entity);
    expect(result).toEqual(expectedOutput);
  });
});

describe("mapViewDefinitions", () => {
  it("should map view paths based on the given paths and Entity object", () => {
    const paths = [
      "as",
      "hs",
      "as/a",
      "as/a/bs/b",
      "as/a/bs/b#f=View:a:prop1,prop2",
      "as/a/ds/d=View:a:prop1,prop2",
      "a/d#g=[View:b:prop1,prop2]",
    ];

    const Entity = {
      Entity1: "a",
      Entity2: "b",
      Entity3: "d",
    };

    const expectedOutput = [
      {
        destEntity: "b",
        destProp: {
          name: "f",
          type: "map",
        },
        srcProps: ["prop1", "prop2"],
        srcEntity: "a",
      },
      {
        destEntity: "d",
        srcProps: ["prop1", "prop2"],
        srcEntity: "a",
      },
      {
        destEntity: "d",
        destProp: {
          name: "g",
          type: "array-map",
        },
        srcProps: ["prop1", "prop2"],
        srcEntity: "b",
      },
    ];

    const result = mapViewDefinitions(paths, Entity);
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

