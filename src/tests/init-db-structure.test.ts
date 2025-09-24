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
          f: ["ViewMap@00.01.20:b:prop1,prop2", "ViewMap@00.01.21:b:prop1,prop2,prop3"],
          g: ["ViewArrayMap@00.01.20:b:prop1,prop2"],
        },
      },
      h: {
        i: ["View@00.01.21:b:prop1,prop2"],
      },
    };

    const expectedOutput = [
      "a",
      "h",
      "a/b",
      "a/e",
      "h/i=View@00.01.21:b:prop1,prop2",
      "a/b/c",
      "a/b/d",
      "a/e#f=ViewMap@00.01.20:b:prop1,prop2",
      "a/e#f=ViewMap@00.01.21:b:prop1,prop2,prop3",
      "a/e#g=ViewArrayMap@00.01.20:b:prop1,prop2",
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
      "as/a/bs/b#f=ViewMap@0.0.0:a:prop1,prop2",
      "as/a/bs/b#f=ViewMap@0.0.2:a:prop1,prop2,prop3",
      "as/a/ds/d=View@0.0.0:a:prop1,prop2",
      "a/d#g=ViewArrayMap@0.0.0:b:prop1,prop2",
      "as/c=View@0.0.0:b:prop1,prop2:syncCreate=true",
      "as/c#f=ViewMap@0.0.2:b:prop1,prop2:syncCreate=true,type=topic",
    ];

    const Entity = {
      Entity1: "a",
      Entity2: "b",
      Entity3: "d",
      Entity4: "c",
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
        version: "0.0.0",
      },
      {
        destEntity: "b",
        destProp: {
          name: "f",
          type: "map",
        },
        srcProps: ["prop1", "prop2", "prop3"],
        srcEntity: "a",
        version: "0.0.2",
      },
      {
        destEntity: "d",
        srcProps: ["prop1", "prop2"],
        srcEntity: "a",
        version: "0.0.0",
      },
      {
        destEntity: "d",
        destProp: {
          name: "g",
          type: "array-map",
        },
        srcProps: ["prop1", "prop2"],
        srcEntity: "b",
        version: "0.0.0",
      },
      {
        destEntity: "c",
        options: {
          "syncCreate": true,
        },
        srcProps: ["prop1", "prop2"],
        srcEntity: "b",
        version: "0.0.0",
      },
      {
        destEntity: "c",
        destProp: {
          name: "f",
          type: "map",
        },
        options: {
          "syncCreate": true,
        },
        srcProps: ["prop1", "prop2"],
        srcEntity: "b",
        version: "0.0.2",
      },
    ];

    const result = mapViewDefinitions(paths, Entity);
    expect(result).toEqual(expectedOutput);
  });

  it("should log an error if SyncCreate option has a non-boolean value", () => {
    const paths = [
      "as/c=View@0.0.0:b:prop1,prop2:syncCreate=string",
    ];

    const Entity = {
      Entity4: "c",
      Entity2: "b",
    };

    const expectedOutput = [
      {
        destEntity: "c",
        srcProps: ["prop1", "prop2"],
        srcEntity: "b",
        version: "0.0.0",
      },
    ];

    const errorSpy = jest.spyOn(console, "error").mockImplementation();
    const result = mapViewDefinitions(paths, Entity);
    expect(errorSpy).toHaveBeenCalledWith("SyncCreate option must be a boolean, got \"string\"");
    expect(result).toEqual(expectedOutput);
  });

  it("should log an error if there are unsupported options", () => {
    const paths = [
      "as/c=View@0.0.0:b:prop1,prop2:syncCreate=true,type=topic",
    ];

    const Entity = {
      Entity4: "c",
      Entity2: "b",
    };

    const expectedOutput = [
      {
        destEntity: "c",
        options: {
          "syncCreate": true,
        },
        srcProps: ["prop1", "prop2"],
        srcEntity: "b",
        version: "0.0.0",
      },
    ];

    const errorSpy = jest.spyOn(console, "error").mockImplementation();
    const result = mapViewDefinitions(paths, Entity);
    expect(errorSpy).toHaveBeenCalledWith("Unsupported view option: type");
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

