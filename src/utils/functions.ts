export function debounce<T extends any[], A extends object|any[]|Map<string, any>>(
  func: ((...args: T) => void) | ((accumulator: A) => void),
  wait: number,
  maxWait?: number,
  reducer?: {
    reducerFn: (accumulator: A, ...currentArgs: T) => void,
    initialValueFactory: () => A,
  }
): (...args: T) => void {
  const accumulatedResultQueue: A[] = [];
  if (reducer) {
    accumulatedResultQueue.push(reducer.initialValueFactory());
  }

  let timeoutId: NodeJS.Timeout | undefined;
  let firstTimeCalled: number | null = null;
  let lastTimeCalled: number | null = null;

  const queue: T[] = [];
  let processing = false;

  function processQueue() {
    if (processing) return;
    processing = true;

    console.debug("Schedule processing queue using setTimeout");
    setTimeout(() => {
      console.debug("Now processing queue using setTimeout");
      try {
        const args = queue.shift();
        if (!args) {
          console.error("No args in queue. This should not happen");
          return;
        }
        if (!reducer) {
          console.error("Reducer is not defined. This should not happen");
          return;
        }

        const accumulatedResult = accumulatedResultQueue[accumulatedResultQueue.length-1];
        reducer.reducerFn(accumulatedResult, ...args);
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

  return function(...args: T) {
    const now = new Date().getTime();
    lastTimeCalled = now;
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

    const invokeFunction = (...args: T) => {
      console.info("Invoking function");
      if (reducer) {
        console.debug("accumulatedResultQueue.length:", accumulatedResultQueue.length);
        for (let i = 0; i < accumulatedResultQueue.length-1; i++) {
          const accumulatedResult = accumulatedResultQueue[i];
          let isEmpty = false;
          if (Array.isArray(accumulatedResult)) {
            isEmpty = accumulatedResult.length === 0;
          } else if (accumulatedResult instanceof Map) {
            isEmpty = accumulatedResult.size === 0;
          } else {
            isEmpty = Object.keys(accumulatedResult).length === 0;
          }
          if (isEmpty) {
            console.debug("accumulatedResult is empty. Skipping");
            continue;
          }

          (func as ((accumulator: A) => void))(accumulatedResult);
          accumulatedResultQueue.splice(i, 1);
          i--;
        }
      } else {
        (func as ((...args: T) => void))(...args);
      }
    };

    function initiateDebounce() {
      console.debug("debouncing");
      timeoutId = setTimeout(() => {
        if (lastTimeCalled) {
          const timeSinceLastCalled = Date.now() - lastTimeCalled;
          if (timeSinceLastCalled < wait) {
            console.debug("timeSinceLastWait is less than configured wait time. Skipping invoking function");
            return;
          }
        }
        console.debug("debounce timeout is reached");
        if (reducer) {
          accumulatedResultQueue.push(reducer.initialValueFactory());
        }
        firstTimeCalled = null; // Reset timing
        invokeFunction(...args);
      }, wait);
    }

    function initiateMaxWaitReachedSequence() {
      console.debug("debounce maxWait is reached");
      if (reducer) {
        accumulatedResultQueue.push(reducer.initialValueFactory());
      }
      firstTimeCalled = null; // Reset timing
      invokeFunction(...args);
    }

    if (maxWait && timeSinceFirstCalled >= maxWait) {
      initiateMaxWaitReachedSequence();
    } else {
      initiateDebounce();
    }
  };
}
