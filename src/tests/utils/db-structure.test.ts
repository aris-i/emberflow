import {propView, view} from "../../utils/db-structure";

describe("view", () => {
  it("uses default version when not provided", () => {
    const result = view("User", ["id", "name"], {syncCreate: true});
    expect(result).toBe("View@0.0.0:User:id,name:syncCreate=true");
  });

  it("uses provided version", () => {
    const result = view("Order", ["id"], {syncCreate: false}, "1.2.3");
    expect(result).toBe("View@1.2.3:Order:id:syncCreate=false");
  });

  it("should handle multiple options", () => {
    const result = view("Order", ["id"], {syncCreate: true, type: "view-array"}, "1.2.3");
    expect(result).toBe("View@1.2.3:Order:id:syncCreate=true,type=view-array");
  });

  it("should handle no options", () => {
    const result = view("Product", ["sku", "price"], undefined, "1.0.0" );
    expect(result).toBe("View@1.0.0:Product:sku,price:");
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
