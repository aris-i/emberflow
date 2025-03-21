import {extractTransactionGetOnly} from "../../utils/transaction";
import {firestore} from "firebase-admin";

describe("extractTransactionGetOnly", () => {
  it("should expose only get method and bind correctly", async () => {
    const mockDocRef = {} as firestore.DocumentReference;

    const mockGet = jest.fn().mockResolvedValue("mocked-doc");
    const transaction = {
      get: mockGet,
      set: jest.fn(),
      update: jest.fn(),
      delete: jest.fn(),
    } as any;

    const getOnlyTransaction = extractTransactionGetOnly(transaction);

    expect(typeof getOnlyTransaction.get).toBe("function");

    const result = await getOnlyTransaction.get(mockDocRef);
    expect(mockGet).toHaveBeenCalledWith(mockDocRef);
    expect(result).toBe("mocked-doc");

    // @ts-expect-error Ensure no access to set/update/delete
    expect(getOnlyTransaction.set).toBeUndefined();
    // @ts-expect-error Ensure no access to set/update/delete
    expect(getOnlyTransaction.update).toBeUndefined();
  });
});
