import {
  useBillProtect,
  FuncConfigData,
  FuncUsageData,
  _mockable,
  computeTotalCost,
  computeElapseTime,
} from "../../utils/bill-protect";
import {admin} from "../../index";

jest.mock("../../index", () => ({
  admin: {
    firestore: jest.fn().mockReturnValue({
      collection: jest.fn().mockReturnThis(),
      doc: jest.fn().mockReturnThis(),
      get: jest.fn().mockResolvedValue({}),
      exists: true,
      data: jest.fn().mockReturnValue({}),
      set: jest.fn().mockResolvedValue({}),
      update: jest.fn().mockResolvedValue({}),
    }),
  },
  docPaths: {
    // mock docPaths here
  },
  onDocChange: jest.fn().mockResolvedValue({}),
}));

describe("computeElapseTime", () => {
  it("should calculate the elapsed time correctly", () => {
    const startTime: [number, number] = [10, 0]; // example start time
    const endTime: [number, number] = [12, 120000000]; // example end time

    const result = computeElapseTime(startTime, endTime);

    expect(result).toBe(2200);
  });
});


describe("computeTotalCost", () => {
  it("should calculate the total cost correctly for less than 2M invocation", () => {
    const totalInvocations = 1000000;
    const pricePer1MInvocation = 0.40;
    const totalElapsedTimeInMs = 500;
    const pricePer100ms = 0.000000648;

    const expectedInvocationCost = 0; // The first 2 million invocations are free
    const expectedComputeTimeCost = 500 * 0.000000648 / 100; // totalElapsedTimeInMs * pricePer100ms / 100

    const expectedTotalCost = expectedInvocationCost + expectedComputeTimeCost;

    const totalCost = computeTotalCost(
      totalInvocations,
      pricePer1MInvocation,
      totalElapsedTimeInMs,
      pricePer100ms
    );

    expect(totalCost).toBe(expectedTotalCost);
  });

  it("should calculate the total cost correctly for greater than 2M invocation", () => {
    const totalInvocations = 3000000;
    const pricePer1MInvocation = 0.40;
    const totalElapsedTimeInMs = 500;
    const pricePer100ms = 0.000000648;

    const expectedInvocationCost = pricePer1MInvocation; // The first 2 million invocations are free
    const expectedComputeTimeCost = 500 * 0.000000648 / 100; // totalElapsedTimeInMs * pricePer100ms / 100

    const expectedTotalCost = expectedInvocationCost + expectedComputeTimeCost;

    const totalCost = computeTotalCost(
      totalInvocations,
      pricePer1MInvocation,
      totalElapsedTimeInMs,
      pricePer100ms
    );

    expect(totalCost).toBe(expectedTotalCost);
  });
});
describe("useBillProtect", () => {
  const funcConfigRef = admin.firestore().doc("");
  const mockFuncConfig: FuncConfigData = {
    vCPU: 1,
    mem: 256,
    costLimit: 10,
    pricePer100ms: 1,
    pricePer1MInvocation: 1,
    enabled: true,
  };
  const mockFuncDisabledConfig: FuncConfigData = {
    vCPU: 1,
    mem: 256,
    costLimit: 10,
    pricePer100ms: 1,
    pricePer1MInvocation: 1,
    enabled: false,
  };

  const funcUsageRef = admin.firestore().doc("");
  const mockFuncUsage: FuncUsageData = {
    totalElapsedTimeInMs: 0,
    totalInvocations: 0,
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(_mockable, "fetchAndInitFuncConfig").mockResolvedValue({
      funcConfigRef,
      funcConfig: mockFuncConfig,
    });

    jest.spyOn(_mockable, "fetchAndInitFuncUsage").mockResolvedValue({
      funcUsageRef,
      funcUsage: mockFuncUsage,
    });

    jest.spyOn(_mockable, "lockdownCollection").mockResolvedValue();
    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(0);
    jest.spyOn(_mockable, "incrementTotalInvocations").mockResolvedValue();
    jest.spyOn(_mockable, "incrementTotalElapsedTimeInMs").mockResolvedValue();
    jest.spyOn(_mockable, "disableFunc").mockResolvedValue();
    jest.spyOn(_mockable, "lockdownCollection").mockResolvedValue();
    jest.spyOn(console, "warn").mockImplementation();
  });

  const entity = "example";
  const change = {
    before: null,
    after: null,
  };
  const context = {
    eventId: "12345",
    eventType: "create",
    params: {}, // Add any necessary parameters
    resource: {
      name: "projects/my-project-id/databases/(default)/documents",
      type: "cloud.firestore",
      service: "firestore",
    },
    timestamp: "2023-05-16T00:00:00Z",
  };
  const event = "create";
  const onDocChangeMock = jest.fn().mockResolvedValue({});

  it("should invoke onDocChange and update Firestore", async () => {
    const protectedFunction = useBillProtect(onDocChangeMock);

    // Mock the computeElapseTime function to return a specific value
    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(200);
    jest.spyOn(_mockable, "computeTotalCost").mockReturnValue(9);

    await protectedFunction(entity, change, context, event);

    expect(onDocChangeMock).toHaveBeenCalledWith(entity, change, context, event);
    expect(_mockable.fetchAndInitFuncConfig).toHaveBeenCalledWith(admin.firestore(), "on-create-example");
    expect(_mockable.fetchAndInitFuncUsage).toHaveBeenCalledWith(admin.firestore(), "on-create-example");
    expect(_mockable.incrementTotalInvocations).toHaveBeenCalledWith(funcUsageRef);
    expect(_mockable.computeTotalCost).toHaveBeenCalledWith(
      mockFuncUsage.totalInvocations+1,
      mockFuncConfig.pricePer1MInvocation,
      mockFuncUsage.totalElapsedTimeInMs,
      mockFuncConfig.pricePer100ms);
    expect(_mockable.disableFunc).not.toHaveBeenCalled();
    expect(_mockable.lockdownCollection).not.toHaveBeenCalled();
    expect(_mockable.computeElapseTime).toHaveBeenCalledWith(expect.arrayContaining([expect.any(Number)]), expect.arrayContaining([expect.any(Number)]));
    expect(_mockable.incrementTotalElapsedTimeInMs).toHaveBeenCalledWith(funcUsageRef, 200);
    expect(console.warn).not.toHaveBeenCalled();
  });

  it("should disable function and lockdown collection when budget exceeded", async () => {
    const protectedFunction = useBillProtect(onDocChangeMock);

    // Mock the computeElapseTime function to return a specific value
    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(200);
    jest.spyOn(_mockable, "computeTotalCost").mockReturnValue(10);

    await protectedFunction(entity, change, context, event);

    expect(onDocChangeMock).not.toHaveBeenCalled();
    expect(_mockable.fetchAndInitFuncConfig).toHaveBeenCalledWith(admin.firestore(), "on-create-example");
    expect(_mockable.fetchAndInitFuncUsage).toHaveBeenCalledWith(admin.firestore(), "on-create-example");
    expect(_mockable.incrementTotalInvocations).toHaveBeenCalledWith(funcUsageRef);
    expect(_mockable.computeTotalCost).toHaveBeenCalledWith(
      mockFuncUsage.totalInvocations+1,
      mockFuncConfig.pricePer1MInvocation,
      mockFuncUsage.totalElapsedTimeInMs,
      mockFuncConfig.pricePer100ms);
    expect(_mockable.disableFunc).toHaveBeenCalledWith(funcConfigRef);
    expect(_mockable.lockdownCollection).toHaveBeenCalledWith(entity);
    expect(_mockable.computeElapseTime).toHaveBeenCalledWith(expect.arrayContaining([expect.any(Number)]), expect.arrayContaining([expect.any(Number)]));
    expect(_mockable.incrementTotalElapsedTimeInMs).toHaveBeenCalledWith(funcUsageRef, 200);
    expect(console.warn).toHaveBeenCalledWith("Function on-create-example has exceeded the cost limit of $10");
  });

  it("should exit immediately if function is already disabled", async () => {
    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(200);
    jest.spyOn(_mockable, "computeTotalCost").mockReturnValue(10);
    jest.spyOn(_mockable, "fetchAndInitFuncConfig").mockResolvedValue({
      funcConfigRef,
      funcConfig: mockFuncDisabledConfig,
    });

    const protectedFunction = useBillProtect(onDocChangeMock);
    await protectedFunction(entity, change, context, event);

    expect(onDocChangeMock).not.toHaveBeenCalled();
    expect(_mockable.fetchAndInitFuncConfig).toHaveBeenCalledWith(admin.firestore(), "on-create-example");
    expect(_mockable.fetchAndInitFuncUsage).toHaveBeenCalledWith(admin.firestore(), "on-create-example");
    expect(_mockable.incrementTotalInvocations).toHaveBeenCalledWith(funcUsageRef);
    expect(_mockable.computeTotalCost).not.toHaveBeenCalled();
    expect(_mockable.disableFunc).not.toHaveBeenCalledWith(funcConfigRef);
    expect(_mockable.lockdownCollection).not.toHaveBeenCalled();
    expect(_mockable.computeElapseTime).toHaveBeenCalledWith(expect.arrayContaining([expect.any(Number)]), expect.arrayContaining([expect.any(Number)]));
    expect(_mockable.incrementTotalElapsedTimeInMs).toHaveBeenCalledWith(funcUsageRef, 200);
    expect(console.warn).toHaveBeenCalledWith("Function on-create-example is disabled.  Returning");
  });
});
