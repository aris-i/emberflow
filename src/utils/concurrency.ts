export async function runWithConcurrency<T>(
  tasks: Array<() => Promise<T>>,
  limit: number,
): Promise<PromiseSettledResult<T>[]> {
  if (tasks.length === 0) {
    return [];
  }
  const results: PromiseSettledResult<T>[] = new Array(tasks.length);
  let next = 0;
  const worker = async () => {
    while (next < tasks.length) {
      const i = next++;
      try {
        results[i] = {status: "fulfilled", value: await tasks[i]()};
      } catch (reason) {
        results[i] = {status: "rejected", reason};
      }
    }
  };
  const effectiveLimit = Math.max(1, limit || 1);
  const workerCount = Math.min(effectiveLimit, tasks.length);
  await Promise.all(Array.from({length: workerCount}, worker));
  return results;
}
