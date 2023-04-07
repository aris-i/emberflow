import {createViewLogicFn} from "../../logics";
import {Action, ViewDefinition} from "../../types";
import * as admin from "firebase-admin";
import * as queryutils from "../../utils/query";
import * as pathsutils from "../../utils/paths";

const vd1: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar"],
  destEntity: "friend",
};

const vd2: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar"],
  destEntity: "post",
  destProp: "postedBy",
};


const testAction: Action = {
  actionType: "update",
  path: "users/1234",
  document: {
    name: "John Doe",
    age: 16,
  },
  status: "processing",
  timeCreated: admin.firestore.Timestamp.now(),
};


jest.mock("../../index", () => {
  const originalModule = jest.requireActual("../../index");
  return {
    ...originalModule,
    docPaths: {
      user: "users/{userId}",
      friend: "users/{userId}/friends/{friendId}",
      post: "users/{userId}/posts/{postId}",
    },
    hydrateDocPath: jest.fn()
      .mockReturnValueOnce(Promise.resolve([
        "users/456/friends/1234",
        "users/789/friends/1234",
      ]))
      .mockReturnValueOnce(Promise.resolve([
        "users/123/posts/987",
        "users/890/posts/987",
      ])),

  };
});


describe("createViewLogicFn", () => {
  it("should create a logic function that processes the given action and view definition", async () => {
    const hydrateDocPathSpy = jest.spyOn(pathsutils, "hydrateDocPath");
    const fetchIdsMock = jest.spyOn(queryutils, "fetchIds");
    fetchIdsMock
      .mockResolvedValueOnce(["456", "789"])
      .mockResolvedValue(["1234"]);

    // Create the logic function using the viewDefinition
    const logicFn = createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn(testAction);

    // Add your expectations here, e.g., result.documents should have the correct properties and values
    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/456/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe"});

    document = result.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/789/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe"});

    expect(hydrateDocPathSpy.mock.calls[0][0]).toEqual("users/{userId}/friends/{friendId}");
    expect(hydrateDocPathSpy.mock.calls[0][1]).toEqual({
      "friend": {
        fieldName: "id",
        operator: "==",
        value: "1234",
      },
    });

    fetchIdsMock
      .mockResolvedValueOnce(["123", "890"])
      .mockResolvedValue(["987"]);

    // Create the logic function using the viewDefinition
    const logicFn2 = createViewLogicFn(vd2);

    // Call the logic function with the test action
    const result2 = await logicFn2(testAction);

    // Add your expectations here, e.g., result.documents should have the correct properties and values
    expect(result2).toBeDefined();
    expect(result2.documents).toBeDefined();
    expect(result2.documents.length).toEqual(2);

    document = result2.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/123/posts/987");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    document = result2.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/987");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    expect(hydrateDocPathSpy.mock.calls[1][0]).toEqual("users/{userId}/posts/{postId}");
    expect(hydrateDocPathSpy.mock.calls[1][1]).toEqual({
      "post": {
        fieldName: "postedBy.id",
        operator: "==",
        value: "1234",
      },
    });
  });
});
