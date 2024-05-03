export function debounce<T extends any[], A extends object|any[]>(
  func: ((...args: T) => void) | ((accumulator: A) => void),
  wait: number,
  maxWait?: number,
  reducer?: {
    reducerFn: (accumulator: A, ...currentArgs: T) => void,
    initialValueFactory: () => A,
  }
): (...args: T) => void {
  let accumulatedResult = reducer?.initialValueFactory(); // Generate a fresh initial value
  let prevAccumulatedResult: A|undefined; // Generate a fresh initial value
  let timeoutId: NodeJS.Timeout | undefined;
  let firstTimeCalled: number | null = null;

  const queue: T[] = [];
  let processing = false;

  function processQueue() {
    if (processing) return;
    processing = true;

    setImmediate(async () => {
      try {
        const args = queue.shift();
        if (reducer && accumulatedResult !== undefined && args !== undefined) {
          reducer.reducerFn(accumulatedResult, ...args);
        }
      } catch (error) {
        console.error("Error processing queue:", error);
      } finally {
        processing = false; // Ensure processing is reset correctly
        if (queue.length > 0) {
          processQueue(); // Continue processing if there are more items
        }
      }
    });
  }

  const invokeFunction = (...args: T) => {
    if (reducer && prevAccumulatedResult !== undefined) {
      (func as ((accumulator: A) => void))(prevAccumulatedResult);
    } else {
      (func as ((...args: T) => void))(...args);
    }
  };

  return function(...args: T) {
    const now = new Date().getTime();
    if (!firstTimeCalled) {
      firstTimeCalled = now;
    }
    const timeSinceFirstCalled = now - firstTimeCalled;

    if (timeoutId) {
      clearTimeout(timeoutId);
    }

    if (reducer) {
      queue.push(args);
      processQueue();
    }

    if (maxWait && timeSinceFirstCalled >= maxWait) {
      // Ensure maxWait is respected
      prevAccumulatedResult = accumulatedResult;
      accumulatedResult = reducer?.initialValueFactory(); // Reset to a new initial value after executing
      invokeFunction(...args);
      firstTimeCalled = null; // Reset timing
    } else {
      // Standard debounce behavior
      timeoutId = setTimeout(() => {
        prevAccumulatedResult = accumulatedResult;
        accumulatedResult = reducer?.initialValueFactory(); // Reset to a new initial value after executing
        invokeFunction(...args);
        firstTimeCalled = null; // Reset timing
      }, wait);
    }
  };
}
