import {initDbStructure} from "../../init-db-structure";
import {dbStructure, Entity} from "../../sample-custom/backend-db-structure";
import {propView, view} from "../../utils/db-structure";

describe("view", () => {
  it("uses default version when not provided", () => {
    const result = view("User", ["id", "name"], {syncCreate: true});
    expect(result).toBe("View:User@0.0.0:id,name:syncCreate=true");
  });

  it("uses provided version", () => {
    const result = view("Order", ["id"], {syncCreate: false}, "1.2.3");
    expect(result).toBe("View:Order@1.2.3:id:syncCreate=false");
  });

  it("should handle multiple options", () => {
    const result = view("Order", ["id"], {syncCreate: true, type: "view-array"}, "1.2.3");
    expect(result).toBe("View:Order@1.2.3:id:syncCreate=true,type=view-array");
  });

  it("should handle no options", () => {
    const result = view("Product", ["sku", "price"], undefined, "1.0.0" );
    expect(result).toBe("View:Product@1.0.0:sku,price:");
  });
});

describe("propView", () => {
  it("returns ViewMap when type is map", () => {
    const result = propView("map", "Inventory", ["count"], {syncCreate: true});
    expect(result).toBe("ViewMap@0.0.0:Inventory:count:syncCreate=true");
  });

  it("returns ViewArrayMap when type is array-map", () => {
    const result = propView("array-map", "Inventory", ["count"], {syncCreate: true}, "2.0.0");
    expect(result).toBe("ViewArrayMap@2.0.0:Inventory:count:syncCreate=true");
  });

  it("handles empty options", () => {
    const result = propView("map", "Inventory", ["count"]);
    expect(result).toBe("ViewMap@0.0.0:Inventory:count:");
  });
});

describe("initDbStructure", () => {
  it("should return entity and regex for path", () => {
    function findMatchingDocPathRegex(docPath: string) {
      const {docPathsRegex} = initDbStructure(dbStructure, Entity);

      for (const key in docPathsRegex) {
        if (docPathsRegex[key].test(docPath)) {
          return {entity: key, regex: docPathsRegex[key]};
        }
      }
      return {entity: undefined, regex: undefined};
    }

    const res = findMatchingDocPathRegex("topics/topic123/todos/todo456");
    expect(res).toEqual({entity: Entity.ToDoView, regex: "/^topics\\/([^/]+)\\/todos\\/([^/]+)$/"});
  });
});
