import {hydrateDocPath} from "../../utils/paths";
import {fetchIds} from "../../utils/query";
import {QueryCondition} from "../../types";

jest.mock("../../utils/query");

describe("hydrateDocPath", () => {
  it("should return all possible document paths", async () => {
    const mockFetchIds = fetchIds as jest.MockedFunction<typeof fetchIds>;
    mockFetchIds
      .mockResolvedValueOnce(["123", "456"])
      .mockResolvedValue(["321", "654"]);

    const destDocPath = "users/{userId}/posts/{postId}";
    const allowedUsers = ["321", "654"];
    const userCondition: QueryCondition = {
      fieldName: "id",
      operator: "in",
      value: allowedUsers,
    };
    const result = await hydrateDocPath(destDocPath, {
      user: userCondition,
    });
    expect(result).toEqual([
      "users/123/posts/321",
      "users/123/posts/654",
      "users/456/posts/321",
      "users/456/posts/654",
    ]);
    expect(mockFetchIds).toHaveBeenCalledTimes(3);
    expect(mockFetchIds).toHaveBeenNthCalledWith(1, "users", userCondition);
    expect(mockFetchIds).toHaveBeenNthCalledWith(2, "users/123/posts", undefined);
    expect(mockFetchIds).toHaveBeenNthCalledWith(3, "users/456/posts", undefined);
  });
});
