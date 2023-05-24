import {createViewLogicFn, syncPeerViews} from "../../logics/view-logics";
import {LogicResultDoc, ViewDefinition} from "../../types";
import * as pathsutils from "../../utils/paths";

const vd1: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar", "age"],
  destEntity: "friend",
};

const vd2: ViewDefinition = {
  srcEntity: "user",
  srcProps: ["name", "avatar"],
  destEntity: "post",
  destProp: "postedBy",
};


const testLogicResultDoc: LogicResultDoc = {
  action: "merge",
  dstPath: "users/1234",
  doc: {
    name: "John Doe",
  },
  instructions: {
    age: "++",
  },
};

const testPeerLogicResultDoc: LogicResultDoc = {
  action: "merge",
  dstPath: "users/1234/posts/5678/comments/9876",
  doc: {
    title: "I like it",
  },
  instructions: {
    likes: "++",
  },
};

const testLogicResultDocDelete: LogicResultDoc = {
  action: "delete",
  dstPath: "users/1234",
};


jest.mock("../../index", () => {
  const originalModule = jest.requireActual("../../index");
  return {
    ...originalModule,
    docPaths: {
      user: "users/{userId}",
      friend: "users/{userId}/friends/{friendId}",
      post: "users/{userId}/posts/{postId}",
      comment: "users/{userId}/posts/{postId}/comments/{commentId}",
    },
    docPathsRegex: {
      user: /^users\/([^/]+)$/,
      friend: /^users\/([^/]+)\/friends\/([^/]+)$/,
      post: /^users\/([^/]+)\/posts\/([^/]+)$/,
      comment: /^users\/([^/]+)\/posts\/([^/]+)\/comments\/([^/]+)$/,
    },
  };
});


describe("createViewLogicFn", () => {
  it("should create a logic function that processes the given logicResultDoc and view definition", async () => {
    const hydrateDocPathSpy = jest.spyOn(pathsutils, "hydrateDocPath");
    hydrateDocPathSpy.mockReset();
    hydrateDocPathSpy
      .mockReturnValueOnce(Promise.resolve([
        "users/456/friends/1234",
        "users/789/friends/1234",
      ]))
      .mockReturnValueOnce(Promise.resolve([
        "users/1234/posts/987",
        "users/1234/posts/654",
        "users/890/posts/987",
        "users/890/posts/654",
      ]))
      .mockReturnValueOnce(Promise.resolve([
        "users/456/friends/1234",
        "users/789/friends/1234",
      ]));

    // Create the logic function using the viewDefinition
    const logicFn = createViewLogicFn(vd1);

    // Call the logic function with the test action
    const result = await logicFn(testLogicResultDoc);

    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(2);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/456/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe"});
    expect(document.instructions).toEqual({"age": "++"});

    document = result.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/789/friends/1234");
    expect(document.doc).toEqual({"name": "John Doe"});
    expect(document.instructions).toEqual({"age": "++"});

    expect(hydrateDocPathSpy.mock.calls[0][0]).toEqual("users/{userId}/friends/1234");
    expect(hydrateDocPathSpy.mock.calls[0][1]).toEqual({});

    // Create the logic function using the viewDefinition
    const logicFn2 = createViewLogicFn(vd2);

    // Call the logic function with the test action
    const result2 = await logicFn2(testLogicResultDoc);

    // Add your expectations here, e.g., result.documents should have the correct properties and values
    expect(result2).toBeDefined();
    expect(result2.documents).toBeDefined();
    expect(result2.documents.length).toEqual(4);

    document = result2.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/1234/posts/987");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    document = result2.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/1234/posts/654");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    document = result2.documents[2];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/987");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    document = result2.documents[3];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/654");
    expect(document.doc).toEqual({"postedBy.name": "John Doe"});

    expect(hydrateDocPathSpy.mock.calls[1][0]).toEqual("users/{userId}/posts/{postId}");
    expect(hydrateDocPathSpy.mock.calls[1][1]).toEqual({
      post: {
        fieldName: "postedBy.id",
        operator: "==",
        value: "1234",
      },
    });

    const resultDelete = await logicFn(testLogicResultDocDelete);

    expect(resultDelete).toBeDefined();
    expect(resultDelete.documents).toBeDefined();
    expect(resultDelete.documents.length).toEqual(2);

    document = resultDelete.documents[0];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/456/friends/1234");

    document = resultDelete.documents[1];
    expect(document).toHaveProperty("action", "delete");
    expect(document).toHaveProperty("dstPath", "users/789/friends/1234");

    expect(hydrateDocPathSpy.mock.calls[2][0]).toEqual("users/{userId}/friends/1234");
    expect(hydrateDocPathSpy.mock.calls[2][1]).toEqual({});
  });
});

describe("syncPeerViews", () => {
  it("should create a logic function that processes the given logicResultDoc and syncs peer views", async () => {
    const hydrateDocPathSpy = jest.spyOn(pathsutils, "hydrateDocPath");
    hydrateDocPathSpy.mockReset();
    hydrateDocPathSpy
      .mockReturnValueOnce(Promise.resolve([
        "users/456/posts/5678/comments/9876",
        "users/789/posts/5678/comments/9876",
        "users/890/posts/5678/comments/9876",
      ]));
    const findMatchingDocPathRegexSpy= jest.spyOn(pathsutils, "findMatchingDocPathRegex");
    findMatchingDocPathRegexSpy
      .mockReturnValue({entity: "comment", regex: /^users\/([^/]+)\/posts\/([^/]+)\/comments\/([^/]+)$/});

    // Call the syncPeerViews function with the test logic result doc
    // users/1234/posts/5678/comments/9876
    const result = await syncPeerViews(testPeerLogicResultDoc);

    // Add your expectations here, e.g., result.documents should have the correct properties and values
    expect(result).toBeDefined();
    expect(result.documents).toBeDefined();
    expect(result.documents.length).toEqual(3);

    let document = result.documents[0];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/456/posts/5678/comments/9876");
    expect(document.doc).toEqual({"title": "I like it"});
    expect(document.instructions).toEqual({"likes": "++"});

    document = result.documents[1];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/789/posts/5678/comments/9876");
    expect(document.doc).toEqual({"title": "I like it"});
    expect(document.instructions).toEqual({"likes": "++"});

    document = result.documents[2];
    expect(document).toHaveProperty("action", "merge");
    expect(document).toHaveProperty("dstPath", "users/890/posts/5678/comments/9876");
    expect(document.doc).toEqual({"title": "I like it"});
    expect(document.instructions).toEqual({"likes": "++"});

    expect(hydrateDocPathSpy.mock.calls[0][0]).toEqual("users/{userId}/posts/5678/comments/{commentId}");
    expect(hydrateDocPathSpy.mock.calls[0][1]).toEqual({
      "user": {
        fieldName: "@id",
        operator: "!=",
        value: "1234",
      },
      "comment": {
        fieldName: "@id",
        operator: "==",
        value: "9876",
      },
    });
  });
});
