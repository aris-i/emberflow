import {transformLogicResultDocIfNeeded} from "../index";
import * as index from "../index";

describe("transformLogicResultDocIfNeeded", () => {
  let originalEntityViewDefinitions: any;
  let originalDocPathsRegex: any;

  beforeAll(() => {
    originalEntityViewDefinitions = {...index.entityViewDefinitions};
    originalDocPathsRegex = {...index.docPathsRegex};
  });

  afterAll(() => {
    Object.keys(index.entityViewDefinitions).forEach((key) => delete (index.entityViewDefinitions as Record<string, any>)[key]);
    Object.assign(index.entityViewDefinitions, originalEntityViewDefinitions);

    Object.keys(index.docPathsRegex).forEach((key) => delete (index.docPathsRegex as Record<string, any>)[key]);
    Object.assign(index.docPathsRegex, originalDocPathsRegex);
  });

  it("should transform LogicResultDoc when destProp is present and no viewDefinition exists", () => {
    // Setup entityViewDefinitions
    (index.entityViewDefinitions as Record<string, any>)["myEntity"] = {};
    (index.docPathsRegex as Record<string, any>)["myEntity"] = /^myCollection\/([^/]+)$/;

    const doc = {
      dstPath: "myCollection/myDoc#myProp",
      doc: {prop1: "hello"},
      action: "create",
      priority: "normal",
    };

    const result = transformLogicResultDocIfNeeded(doc as any);

    expect(result.dstPath).toBe("myCollection/myDoc");
    expect(result.doc).toEqual({myProp: {prop1: "hello"}});
  });

  it("should NOT transform LogicResultDoc when destProp is present and viewDefinition EXISTS", () => {
    (index.entityViewDefinitions as Record<string, any>)["myEntity"] = {
      "myProp": {destEntity: "myEntity", destProp: {name: "myProp", type: "map"}} as any,
    };
    (index.docPathsRegex as Record<string, any>)["myEntity"] = /^myCollection\/([^/]+)$/;

    const doc = {
      dstPath: "myCollection/myDoc#myProp",
      doc: {prop1: "hello"},
      action: "create",
      priority: "normal",
    };

    const result = transformLogicResultDocIfNeeded(doc as any);

    expect(result.dstPath).toBe("myCollection/myDoc#myProp");
    expect(result.doc).toEqual({prop1: "hello"});
  });

  it("should NOT transform LogicResultDoc when NO destProp is present", () => {
    const doc = {
      dstPath: "myCollection/myDoc",
      doc: {prop1: "hello"},
      action: "create",
      priority: "normal",
    };

    const result = transformLogicResultDocIfNeeded(doc as any);

    expect(result.dstPath).toBe("myCollection/myDoc");
    expect(result.doc).toEqual({prop1: "hello"});
  });
});
