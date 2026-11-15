import {runWithConcurrency} from "../../utils/concurrency";

describe("runWithConcurrency", () => {
  it("caps max in-flight tasks at limit (200 tasks, limit 50)", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    const taskCount = 200;
    const limit = 50;

    const tasks = Array.from({length: taskCount}, (_, i) => async () => {
      inFlight++;
      if (inFlight > maxInFlight) {
        maxInFlight = inFlight;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
      inFlight--;
      return i;
    });

    const results = await runWithConcurrency(tasks, limit);

    expect(maxInFlight).toBeLessThanOrEqual(limit);
    expect(maxInFlight).toBeGreaterThan(1);
    expect(results).toHaveLength(taskCount);
    for (let i = 0; i < taskCount; i++) {
      expect(results[i]).toEqual({status: "fulfilled", value: i});
    }
  });

  it("runs strictly sequentially with limit 1", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    const taskCount = 20;
    const executionOrder: number[] = [];

    const tasks = Array.from({length: taskCount}, (_, i) => async () => {
      inFlight++;
      if (inFlight > maxInFlight) {
        maxInFlight = inFlight;
      }
      await new Promise((resolve) => setTimeout(resolve, 2));
      executionOrder.push(i);
      inFlight--;
      return i;
    });

    const results = await runWithConcurrency(tasks, 1);

    expect(maxInFlight).toBe(1);
    expect(executionOrder).toEqual(Array.from({length: taskCount}, (_, i) => i));
    expect(results).toHaveLength(taskCount);
  });

  it("reports rejections as rejected results without stopping the pool or throwing", async () => {
    const tasks = [
      async () => "success-1",
      async () => {
        throw new Error("failure-2");
      },
      async () => "success-3",
      () => {
        throw new Error("sync-throw-4");
      },
      async () => "success-5",
    ];

    const results = await runWithConcurrency(tasks, 2);

    expect(results).toHaveLength(5);
    expect(results[0]).toEqual({status: "fulfilled", value: "success-1"});
    expect(results[1].status).toBe("rejected");
    expect((results[1] as PromiseRejectedResult).reason.message).toBe("failure-2");
    expect(results[2]).toEqual({status: "fulfilled", value: "success-3"});
    expect(results[3].status).toBe("rejected");
    expect((results[3] as PromiseRejectedResult).reason.message).toBe("sync-throw-4");
    expect(results[4]).toEqual({status: "fulfilled", value: "success-5"});
  });

  it("handles empty tasks array", async () => {
    const results = await runWithConcurrency([], 10);
    expect(results).toEqual([]);
  });
});
