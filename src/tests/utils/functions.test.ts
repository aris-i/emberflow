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

    expect(consoleDebugSpy).toHaveBeenCalledTimes(7);
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

    expect(consoleDebugSpy).toHaveBeenCalledTimes(17);
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

    expect(consoleDebugSpy).toHaveBeenCalledTimes(53);
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
});
