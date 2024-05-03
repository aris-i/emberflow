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

const createEvent = () => {
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
};

describe("debounce", () => {
  let setTimeoutSpy: jest.SpyInstance;
  let clearTimeoutSpy: jest.SpyInstance;
  let initialGeneratedMap: Map<string, Instructions> | undefined;
  let func: jest.Mock;

  beforeEach(() => {
    jest.spyOn(global, "setImmediate").mockImplementation((callback) => {
      callback();
      return undefined as unknown as NodeJS.Immediate;
    });
    clearTimeoutSpy = jest.spyOn(global, "clearTimeout");
    setTimeoutSpy = jest.spyOn(global, "setTimeout");
    func = jest.fn();
    initialGeneratedMap = undefined;
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

    expect(clearTimeoutSpy).toHaveBeenCalledTimes(0);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(1);
    expect(func).toHaveBeenCalledTimes(1);
  });

  it("should invoke function with reducer", async () => {
    const debouncedFunc = debounce(func, 200, 1000, {
      reducerFn: instructionsReducer,
      initialValueFactory: () => {
        if (!initialGeneratedMap) {
          initialGeneratedMap = new Map<string, Instructions>();
          return initialGeneratedMap;
        }
        return new Map<string, Instructions>();
      },
    },);
    const reducedInstructions: Map<string, Instructions> = new Map();
    const instructions = {"sample": "++"};
    reducedInstructions.set("/users/test-user-id/documents/doc1", instructions);

    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(200);

    expect(clearTimeoutSpy).toHaveBeenCalledTimes(0);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(1);
    expect(func).toHaveBeenCalledTimes(1);
    expect(initialGeneratedMap).toStrictEqual(reducedInstructions);
  });

  it("should invoke function when there are multiple events in queue", async () => {
    const debouncedFunc = debounce(func, 200, 1000, {
      reducerFn: instructionsReducer,
      initialValueFactory: () => {
        if (!initialGeneratedMap) {
          initialGeneratedMap = new Map<string, Instructions>();
          return initialGeneratedMap;
        }
        return new Map<string, Instructions>();
      },
    },);
    const reducedInstructions: Map<string, Instructions> = new Map();
    const instructions = {"sample": "+3"};
    reducedInstructions.set("/users/test-user-id/documents/doc1", instructions);

    debouncedFunc(createEvent());
    debouncedFunc(createEvent());
    debouncedFunc(createEvent());
    await jest.advanceTimersByTimeAsync(200);

    expect(clearTimeoutSpy).toHaveBeenCalledTimes(2);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(3);
    expect(func).toHaveBeenCalledTimes(1);
    expect(initialGeneratedMap).toStrictEqual(reducedInstructions);
  });

  it("should invoke function when maxWait is reached", async () => {
    const debouncedFunc = debounce(func, 200, 1000, {
      reducerFn: instructionsReducer,
      initialValueFactory: () => {
        if (!initialGeneratedMap) {
          initialGeneratedMap = new Map<string, Instructions>();
          return initialGeneratedMap;
        }
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

    expect(clearTimeoutSpy).toHaveBeenCalledTimes(10);
    expect(setTimeoutSpy).toHaveBeenCalledTimes(10);
    expect(func).toHaveBeenCalledTimes(1);
    expect(initialGeneratedMap).toStrictEqual(reducedInstructions);
  });
});
