export function debounce<T extends any[], A extends object|any[]|Map<string, any>>(
  func: ((...args: T) => void | Promise<void>) | ((accumulator: A) => void | Promise<void>),
  wait: number,
  maxWait?: number,
  reducer?: {
    reducerFn: (accumulator: A, ...currentArgs: T) => void | Promise<void>,
    initialValueFactory: () => A,
  }
): (...args: T) => Promise<void> {
  const accumulatedResultQueue: A[] = [];
  const accumulatedResolvesQueue: ((value: void | PromiseLike<void>) => void)[][] = [];

  if (reducer) {
    accumulatedResultQueue.push(reducer.initialValueFactory());
    accumulatedResolvesQueue.push([]);
  }

  let timeoutId: NodeJS.Timeout | undefined;
  let firstTimeCalled: number | null = null;
  let lastTimeCalled: number | null = null;

  const queue: { args: T, resolve: (value: void | PromiseLike<void>) => void }[] = [];
  let processing = false;

  let resolveList: ((value: void | PromiseLike<void>) => void)[] = [];
  let isInvoking = false;
  let pendingRetries = 0;
  let latestInitiateDebounce: (() => void) | undefined;
  let lastArgs: T | undefined;

  function processQueue() {
    if (processing) return;
    processing = true;

    console.debug("Schedule processing queue using setTimeout");
    setTimeout(async () => {
      console.debug("Now processing queue using setTimeout");
      try {
        const item = queue.shift();
        if (!item) {
          console.error("No args in queue. This should not happen");
          return;
        }
        const {args, resolve} = item;
        if (!reducer) {
          console.error("Reducer is not defined. This should not happen");
          return;
        }

        let accumulatedResult = accumulatedResultQueue[accumulatedResultQueue.length - 1];
        let accumulatedResolves = accumulatedResolvesQueue[accumulatedResolvesQueue.length - 1];

        if (!accumulatedResult) {
          accumulatedResult = reducer.initialValueFactory();
          accumulatedResultQueue.push(accumulatedResult);
          accumulatedResolves = [];
          accumulatedResolvesQueue.push(accumulatedResolves);
        }
        accumulatedResolves.push(resolve);
        await reducer.reducerFn(accumulatedResult, ...args);
        if (!timeoutId && latestInitiateDebounce) {
          latestInitiateDebounce();
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

  return async function(...args: T) {
    let resolveFn: ((value: void | PromiseLike<void>) => void) | undefined;
    const promise = new Promise<void>((resolve) => {
      resolveFn = resolve;
    });

    if (!resolveFn) {
      throw new Error("Promise resolver not captured");
    }

    latestInitiateDebounce = initiateDebounce;
    lastArgs = args;

    const now = new Date().getTime();
    lastTimeCalled = now;
    if (!firstTimeCalled) {
      firstTimeCalled = now;
    }
    const timeSinceFirstCalled = now - firstTimeCalled;

    if (timeoutId) {
      clearTimeout(timeoutId);
      timeoutId = undefined;
    }

    if (reducer) {
      queue.push({args, resolve: resolveFn});
      processQueue();
    } else {
      resolveList.push(resolveFn);
    }

    const invokeFunction = async (...args: T) => {
      if (isInvoking) {
        console.debug("Already invoking, skipping new instance");
        pendingRetries++;
        return;
      }
      isInvoking = true;
      console.info("Invoking function");

      try {
        if (reducer) {
          while (processing) {
            await new Promise((resolve) => setTimeout(resolve, 10));
          }

          const accumulatedResult = accumulatedResultQueue.shift();
          const accumulatedResolves = accumulatedResolvesQueue.shift();

          if (accumulatedResult) {
            const currentResolves = accumulatedResolves || [];

            let isEmpty = false;
            if (Array.isArray(accumulatedResult)) {
              isEmpty = accumulatedResult.length === 0;
            } else if (accumulatedResult instanceof Map) {
              isEmpty = accumulatedResult.size === 0;
            } else {
              isEmpty = Object.keys(accumulatedResult).length === 0;
            }

            if (!isEmpty) {
              try {
                await (func as ((accumulator: A) => void | Promise<void>))(accumulatedResult);
              } catch (error) {
                console.error("Error invoking function:", error);
              }
            }
            currentResolves.forEach((resolve) => resolve());
          }
        } else {
          const currentResolves = [...resolveList];
          resolveList = [];
          try {
            await (func as ((...args: T) => void | Promise<void>))(...args);
          } catch (error) {
            console.error("Error invoking function:", error);
          }
          currentResolves.forEach((resolve) => resolve());
        }
      } catch (error) {
        console.error("Error invoking function (outer):", error);
      } finally {
        isInvoking = false;
        if (reducer) {
          if (pendingRetries > 0) {
            pendingRetries--;
            invokeFunction(...args);
          }
        } else if (pendingRetries > 0) {
          pendingRetries = 0;
          if (lastArgs) {
            invokeFunction(...lastArgs);
          }
        }
      }
    };

    function initiateDebounce() {
      console.debug("debouncing");
      timeoutId = setTimeout(() => {
        timeoutId = undefined;
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
          accumulatedResolvesQueue.push([]);
        }
        firstTimeCalled = null; // Reset timing
        invokeFunction(...args);
      }, wait);
    }

    function initiateMaxWaitReachedSequence() {
      console.debug("debounce maxWait is reached");
      firstTimeCalled = null; // Reset timing
      invokeFunction(...args);
    }

    if (maxWait && timeSinceFirstCalled >= maxWait) {
      initiateMaxWaitReachedSequence();
    } else {
      initiateDebounce();
    }

    return promise;
  };
}
