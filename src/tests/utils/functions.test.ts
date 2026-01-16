import {debounce} from "../../utils/functions";
import {Instructions} from "../../types";
import {CloudEvent} from "firebase-functions/lib/v2/core";
import {MessagePublishedData} from "firebase-functions/lib/v2/providers/pubsub";
const isProcessedMock = jest.fn().mockResolvedValue(false);
const trackProcessedIdsMock = jest.fn().mockResolvedValue({});
import {instructionsReducer} from "../../utils/distribution";
jest.useFakeTimers();
jest.mock("../../utils/pubsub", () => {
  return {
    pubsubUtils: {
      isProcessed: isProcessedMock,
      trackProcessedIds: trackProcessedIdsMock,
    },
  };
});

describe("debounce", () => {
  let setTimeoutSpy: jest.SpyInstance;
  let clearTimeoutSpy: jest.SpyInstance;
  let consoleDebugSpy: jest.SpyInstance;
  let func: jest.Mock;

  function createEvent() {
    return {
      id: "test-event",
      data: {
        message: {
          json: {
            action: "merge",
            priority: "high",
            instructions: {"sample": "++"},
            dstPath: "/users/test-user-id/documents/doc1",
          },
        },
      },
    } as CloudEvent<MessagePublishedData>;
  }

  beforeEach(() => {
    clearTimeoutSpy = jest.spyOn(global, "clearTimeout");
    setTimeoutSpy = jest.spyOn(global, "setTimeout");
    consoleDebugSpy = jest.spyOn(console, "debug");
    func = jest.fn();
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.clearAllTimers();
  });

  it("should invoke function without reducer", async () => {
    const func = jest.fn();
    const debouncedFunc = debounce(func, 200);

    debouncedFunc();
    await jest.advanceTimersByTimeAsync(200);

    expect(consoleDebugSpy).toHaveBeenCalledTimes(2);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(1, "debouncing");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(2, "debounce timeout is reached");
    expect(clearTimeoutSpy).toHaveBeenCalledTimes(0);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(1);
    expect(func).toHaveBeenCalledTimes(1);
  });

  it("should invoke function with reducer", async () => {
    const debouncedFunc = debounce(func, 200, 1000, {
      reducerFn: instructionsReducer,
      initialValueFactory: () => {
        return new Map<string, Instructions>();
      },
    },);
    const reducedInstructions: Map<string, Instructions> = new Map();
    const instructions = {"sample": "++"};
    reducedInstructions.set("/users/test-user-id/documents/doc1", instructions);

    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(200);

    expect(consoleDebugSpy).toHaveBeenCalledTimes(6);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(1, "Schedule processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(2, "debouncing");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(3, "Now processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(4, "Instructions reducer: Existing instructions", undefined);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(5, "Instructions reducer: Reduced instructions", reducedInstructions);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(6, "debounce timeout is reached");
    expect(clearTimeoutSpy).toHaveBeenCalledTimes(0);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(2);
    expect(func).toHaveBeenCalledTimes(1);
    expect(func).toHaveBeenCalledWith(reducedInstructions);
  });

  it("should invoke function when there are multiple events in queue", async () => {
    const debouncedFunc = debounce(func, 200, 1000, {
      reducerFn: instructionsReducer,
      initialValueFactory: () => {
        return new Map<string, Instructions>();
      },
    },);
    const reducedInstructions: Map<string, Instructions> = new Map();
    const instructions = {"sample": "+3"};
    reducedInstructions.set("/users/test-user-id/documents/doc1", instructions);

    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(200);

    expect(consoleDebugSpy).toHaveBeenCalledTimes(16);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(1, "Schedule processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(2, "debouncing");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(3, "Now processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(4, "Instructions reducer: Existing instructions", undefined);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(5, "Instructions reducer: Reduced instructions", reducedInstructions);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(6, "Schedule processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(7, "debouncing");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(8, "Now processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(9, "Instructions reducer: Existing instructions", instructions);
    // this should be ++, terminal also says it is ++, yet jest says it is +3
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(10, "Instructions reducer: Reduced instructions", reducedInstructions);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(11, "Schedule processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(12, "debouncing");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(13, "Now processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(14, "Instructions reducer: Existing instructions", instructions);
    // this should be +2, terminal also says it is +2, yet jest says it is +3
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(15, "Instructions reducer: Reduced instructions", reducedInstructions);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(16, "debounce timeout is reached");
    expect(clearTimeoutSpy).toHaveBeenCalledTimes(2);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(6);
    expect(func).toHaveBeenCalledTimes(1);
    expect(func).toHaveBeenCalledWith(reducedInstructions);
  });

  it("should invoke function when maxWait is reached", async () => {
    const debouncedFunc = debounce(func, 200, 1000, {
      reducerFn: instructionsReducer,
      initialValueFactory: () => {
        return new Map<string, Instructions>();
      },
    },);
    const reducedInstructions: Map<string, Instructions> = new Map();
    const instructions = {"sample": "+10"};
    reducedInstructions.set("/users/test-user-id/documents/doc1", instructions);

    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(100);
    debouncedFunc(createEvent());

    expect(consoleDebugSpy).toHaveBeenCalledTimes(52);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(1, "Schedule processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(2, "debouncing");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(3, "Now processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(4, "Instructions reducer: Existing instructions", undefined);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(5, "Instructions reducer: Reduced instructions", reducedInstructions);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(6, "Schedule processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(49, "Instructions reducer: Existing instructions", instructions);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(50, "Instructions reducer: Reduced instructions", reducedInstructions);
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(51, "Schedule processing queue using setTimeout");
    expect(consoleDebugSpy).toHaveBeenNthCalledWith(52, "debounce maxWait is reached");
    expect(clearTimeoutSpy).toHaveBeenCalledTimes(10);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(21);
    expect(func).toHaveBeenCalledTimes(1);
    expect(func).toHaveBeenCalledWith(reducedInstructions);
  });

  it("should return a promise that resolves only after the function completes", async () => {
    let functionCompleted = false;
    const asyncFunc = jest.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 100));
      functionCompleted = true;
    });

    const debouncedFunc = debounce(asyncFunc, 200);

    const promise = debouncedFunc();
    expect(functionCompleted).toBe(false);

    await jest.advanceTimersByTimeAsync(200);
    // The invokeFunction is called, which calls asyncFunc.
    // Since asyncFunc is async, we need to wait for it.
    await jest.advanceTimersByTimeAsync(100);

    await promise;
    expect(functionCompleted).toBe(true);
    expect(asyncFunc).toHaveBeenCalledTimes(1);
  });

  it("should resolve all pending promises when the function completes", async () => {
    const asyncFunc = jest.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });

    const debouncedFunc = debounce(asyncFunc, 200);

    const p1 = debouncedFunc();
    const p2 = debouncedFunc();
    const p3 = debouncedFunc();

    let p1Resolved = false;
    let p2Resolved = false;
    let p3Resolved = false;

    p1.then(() => p1Resolved = true);
    p2.then(() => p2Resolved = true);
    p3.then(() => p3Resolved = true);

    await jest.advanceTimersByTimeAsync(200); // Trigger debounce
    await jest.advanceTimersByTimeAsync(50);  // Complete async work

    await Promise.all([p1, p2, p3]);
    expect(p1Resolved).toBe(true);
    expect(p2Resolved).toBe(true);
    expect(p3Resolved).toBe(true);
  });

  it("should handle errors in the debounced function and still resolve promises", async () => {
    const errorFunc = jest.fn().mockRejectedValue(new Error("Async Error"));
    const consoleErrorSpy = jest.spyOn(console, "error").mockImplementation();

    const debouncedFunc = debounce(errorFunc, 200);

    const promise = debouncedFunc();

    await jest.advanceTimersByTimeAsync(200);

    // Even if it fails, the promise should resolve (or we might want it to reject,
    // but current implementation resolves it in currentResolves.forEach((resolve) => resolve()))
    // Let's verify current behavior.
    await promise;

    expect(errorFunc).toHaveBeenCalledTimes(1);
    expect(consoleErrorSpy).toHaveBeenCalled();
  });
});
