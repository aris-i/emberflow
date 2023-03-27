import {hydrateDocPath} from "../../utils/paths";
import {fetchIds} from "../../utils/query";

jest.mock("../../utils/query");

describe("hydrateDocPath", () => {
  it("should return all possible document paths", async () => {
    const mockFetchIds = fetchIds as jest.MockedFunction<typeof fetchIds>;
    mockFetchIds
      .mockResolvedValueOnce(["123", "456"])
      .mockResolvedValue(["321", "654"]);

    const destDocPath = "users/{userId}/posts/{postId}";
    const result = await hydrateDocPath(destDocPath, {});
    expect(result).toEqual([
      "users/123/posts/321",
      "users/123/posts/654",
      "users/456/posts/321",
      "users/456/posts/654",
    ]);
  });
});
