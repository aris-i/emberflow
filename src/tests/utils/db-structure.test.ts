import {view} from "../../utils/db-structure";

describe("view", () => {
  it("should return correct string without options", () => {
    const result = view("user", ["name", "email"]);
    expect(result).toBe("View:user:name,email:");
  });

  it("should return correct string with a single string option", () => {
    const result = view("product", ["id"], {sortBy: "price"});
    expect(result).toBe("View:product:id:sortBy=price");
  });

  it("should return correct string with a single boolean option", () => {
    const result = view("order", ["id", "status"], {active: true});
    expect(result).toBe("View:order:id,status:active=true");
  });

  it("should return correct string with multiple options", () => {
    const result = view("category", ["name"], {
      visible: true,
      filter: "popular",
    });
    // order of keys is preserved in Object.entries
    expect(result).toBe("View:category:name:visible=true,filter=popular");
  });

  it("should handle empty props array", () => {
    const result = view("dashboard", []);
    expect(result).toBe("View:dashboard::");
  });

  it("should handle empty options object", () => {
    const result = view("settings", ["theme"], {});
    expect(result).toBe("View:settings:theme:");
  });

  it("should handle false boolean option", () => {
    const result = view("settings", ["notifications"], {enabled: false});
    expect(result).toBe("View:settings:notifications:enabled=false");
  });
});
