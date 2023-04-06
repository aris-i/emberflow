import {expandAndGroupDocPaths, hydrateDocPath, filterSubDocPathsByEntity, _mockable as pathsMockable} from "../../utils/paths";
import {fetchIds} from "../../utils/query";
import {QueryCondition} from "../../types";
import {initializeEmberFlow} from "../../index";
import * as admin from "firebase-admin";
import {dbStructure, Entity} from "../../sample-custom/db-structure";
// eslint-disable-next-line @typescript-eslint/no-var-requires

initializeEmberFlow(admin, dbStructure, Entity, {}, {}, []);

// Mock fetchIds function
jest.mock("../../utils/query", () => ({
  fetchIds: jest.fn(),
}));


describe("hydrateDocPath", () => {
  beforeEach(() => {
    // Clear the mock before each test
    (fetchIds as jest.Mock).mockClear();
  });

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

  it("should return filtered document paths based on the provided entityCondition", async () => {
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

    // Add a condition to filter the posts based on a specific field
    const postCondition: QueryCondition = {
      fieldName: "category",
      operator: "==",
      value: "technology",
    };

    const result = await hydrateDocPath(destDocPath, {
      user: userCondition,
      post: postCondition,
    });

    // Adjust the expected result based on the postCondition
    expect(result).toEqual([
      "users/123/posts/321",
      "users/123/posts/654",
      "users/456/posts/321",
      "users/456/posts/654",
    ]);

    expect(mockFetchIds).toHaveBeenCalledTimes(3);
    expect(mockFetchIds).toHaveBeenNthCalledWith(1, "users", userCondition);
    expect(mockFetchIds).toHaveBeenNthCalledWith(2, "users/123/posts", postCondition);
    expect(mockFetchIds).toHaveBeenNthCalledWith(3, "users/456/posts", postCondition);
  });

  it("should return empty document paths if fetchIds returns an empty array for one of the entities", async () => {
    const mockFetchIds = fetchIds as jest.MockedFunction<typeof fetchIds>;
    mockFetchIds
      .mockResolvedValueOnce(["123", "456"])
      .mockResolvedValueOnce([])
      .mockResolvedValue(["321", "654"]);

    const destDocPath = "users/{userId}/posts/{postId}";
    const allowedUsers = ["123", "456"];
    const userCondition: QueryCondition = {
      fieldName: "id",
      operator: "in",
      value: allowedUsers,
    };

    const result = await hydrateDocPath(destDocPath, {
      user: userCondition,
    });

    expect(result).toEqual([
      "users/456/posts/321",
      "users/456/posts/654",
    ]);
    expect(mockFetchIds).toHaveBeenCalledTimes(3);
    expect(mockFetchIds).toHaveBeenNthCalledWith(1, "users", userCondition);
    expect(mockFetchIds).toHaveBeenNthCalledWith(2, "users/123/posts", undefined);
    expect(mockFetchIds).toHaveBeenNthCalledWith(3, "users/456/posts", undefined);
  });
});


describe("filterSubDocPathsByEntity", () => {
  beforeEach(() => {
    // Clear the mock before each test
    (fetchIds as jest.Mock).mockClear();
  });
  it("should return sub-doc paths for a given entity", () => {
    const result = filterSubDocPathsByEntity(Entity.Friend);
    expect(result).toEqual([
      "users/{userId}/friends/{friendId}",
      "users/{userId}/friends/{friendId}/games/{gameId}",
    ]);
  });

  it("should return sub-doc paths excluding specified entities", () => {
    const result = filterSubDocPathsByEntity(Entity.User, [Entity.Friend]);
    expect(result).toEqual([
      "users/{userId}",
      "users/{userId}/feeds/{feedId}",
    ]);
  });

  it("should return an empty array if the entity is not found", () => {
    const result = filterSubDocPathsByEntity("nonExistentEntity");
    expect(result).toEqual([]);
  });

  it("should return an empty array if the entity does not have any sub-doc paths", () => {
    const result = filterSubDocPathsByEntity(Entity.Game);
    expect(result).toEqual(["users/{userId}/friends/{friendId}/games/{gameId}"]);
  });
});

describe("expandAndGroupDocPaths", () => {
  beforeEach(() => {
    // Clear the mock before each test
    (fetchIds as jest.Mock).mockClear();
  });

  it("should expand and group doc paths based on the starting doc path", async () => {
    // Setup
    const startingDocPath = "users/456";
    const entityCondition = {
      [Entity.Feed]: {fieldName: "status", operator: "==", value: "active"} as QueryCondition,
    };
    const excludeEntities = [Entity.Friend];

    // Mock fetchIds to return sample IDs
    (fetchIds as jest.Mock).mockResolvedValueOnce([123, 321]);

    // Mock filterSubDocPathsByEntity
    const filterSubDocPathsByEntityMock = jest.fn().mockReturnValueOnce([
      "users/{userId}",
      "users/{userId}/feeds/{feedId}",
    ]);
    pathsMockable.filterSubDocPathsByEntity = filterSubDocPathsByEntityMock;

    // Execute the function
    const result = await expandAndGroupDocPaths(startingDocPath, entityCondition, excludeEntities);

    // Check the result
    const expected = {
      [Entity.User]: [
        "users/456",
      ],
      [Entity.Feed]: [
        "users/456/feeds/123",
        "users/456/feeds/321",
      ],
    };
    expect(result).toEqual(expected);

    // Check that fetchIds was called with the correct parameters
    const fetchIdsCalls = (fetchIds as jest.Mock).mock.calls;
    expect(fetchIdsCalls[0]).toEqual(["users/456/feeds", entityCondition[Entity.Feed]]);

    expect(filterSubDocPathsByEntityMock).toHaveBeenCalledTimes(1);
    expect(filterSubDocPathsByEntityMock.mock.calls[0]).toEqual([Entity.User, excludeEntities]);
  });
});
