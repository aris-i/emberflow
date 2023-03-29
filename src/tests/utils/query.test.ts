import {admin} from "../../index";
import {fetchIds} from "../../utils/query";
import {firestore} from "firebase-admin";
import DocumentSnapshot = firestore.DocumentSnapshot;
import QuerySnapshot = firestore.QuerySnapshot;
import {QueryCondition} from "../../types";

jest.mock("../../index", () => {
  const mockQuery = {
    select: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    get: jest.fn().mockReturnThis(),
    docs: jest.fn().mockReturnThis(),
  };
  return {
    admin: {
      firestore: jest.fn().mockReturnValue({
        collection: jest.fn().mockReturnValue(mockQuery),
      }),
    },
  };
});

describe("fetchIds", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should fetch document IDs from a collection with no condition", async () => {
    const collectionPath = "testCollection";

    const testDocSnapshots: DocumentSnapshot[] = [
            {id: "doc1", exists: true, data: () => null} as any,
            {id: "doc2", exists: true, data: () => null} as any,
    ];

    const testQuerySnapshot: QuerySnapshot = {
      empty: false,
      docs: testDocSnapshots,
      size: testDocSnapshots.length,
    } as any;

    (admin.firestore().collection as jest.Mock).mockReturnValue({
      select: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      get: jest.fn().mockResolvedValue(testQuerySnapshot),
    });

    const ids = await fetchIds(collectionPath);

    expect(ids).toEqual(["doc1", "doc2"]);
    expect(admin.firestore().collection).toHaveBeenCalledWith(collectionPath);
  });

  // You can add more test cases for different conditions and operators, such as "in", "not-in", and "array-contains-any"
  it("should handle 'in' operator with more than 10 values", async () => {
    // Set up the mock data and firestore methods
    const collectionPath = "messages";
    const idsArray = Array.from({length: 15}, (_, i) => `id${i + 1}`);
    const condition: QueryCondition = {
      fieldName: "id",
      operator: "in",
      value: idsArray,
    };

    const querySnapshot1 = {
      docs: idsArray.slice(0, 10).map((id) => ({id})),
    };
    const querySnapshot2 = {
      docs: idsArray.slice(10).map((id) => ({id})),
    };

    (admin.firestore().collection as jest.Mock).mockReturnValue({
      select: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      get: jest.fn().mockResolvedValueOnce(querySnapshot1).mockResolvedValueOnce(querySnapshot2),
    });

    // Call the fetchIds function
    const fetchedIds = await fetchIds(collectionPath, condition);

    // Check the result
    expect(fetchedIds).toEqual(idsArray);
    expect(admin.firestore().collection("").where).toHaveBeenCalledTimes(2); // Two "in" conditions with 10 and 5 ids
  });
});

