import {firestore} from "firebase-admin";
import {TxnGet} from "../types";

export function extractTransactionGetOnly(transaction: firestore.Transaction): TxnGet {
  return {
    get: transaction.get.bind(transaction),
  };
}
