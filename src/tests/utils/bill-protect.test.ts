import {
  stopBillingIfBudgetExceeded,
  useBillProtect,
  FuncConfigData,
  FuncUsageData,
  _mockable,
  computeTotalCost,
  computeElapseTime,
} from "../../utils/bill-protect";
import {db} from "../../index";
import {DatabaseEvent, DataSnapshot} from "firebase-functions/lib/v2/providers/database";
import * as paths from "../../utils/paths";

const funcName = "onFormSubmittedForUser";
jest.mock("../../index", () => ({
  db: {
    collection: jest.fn().mockReturnThis(),
    doc: jest.fn().mockReturnThis(),
    get: jest.fn().mockResolvedValue({}),
    exists: true,
    data: jest.fn().mockReturnValue({}),
    set: jest.fn().mockResolvedValue({}),
    update: jest.fn().mockResolvedValue({}),
  },
  projectConfig: {
    projectId: "test-project",
  },
  docPaths: {
    // mock docPaths here
  },
  onFormSubmit: jest.fn().mockResolvedValue({}),
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
  const funcConfigRef = db.doc("");
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

  const funcUsageRef = db.doc("");
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

    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(0);
    jest.spyOn(_mockable, "incrementTotalInvocations").mockResolvedValue();
    jest.spyOn(_mockable, "incrementTotalElapsedTimeInMs").mockResolvedValue();
    jest.spyOn(_mockable, "disableFunc").mockResolvedValue();
    jest.spyOn(console, "warn").mockImplementation();
    jest.spyOn(paths, "findMatchingDocPathRegex")
      .mockReturnValue({entity: "user", regex: /^users\/{userId}$/});
  });

  const event = {
    data: {
      val: jest.fn().mockReturnValue({
        "@docPath": "users/user12345",
        "@actionType": "create",
        "someField": "someValue",
      }),
      ref: {},
    },
    params: {
      formId: "f12345",
      userId: "u12345",
    },
  } as unknown as DatabaseEvent<DataSnapshot>;

  const onFormSubmitMock = jest.fn().mockResolvedValue({});

  it("should invoke onFormSubmit and update Firestore", async () => {
    const protectedFunction = useBillProtect(onFormSubmitMock);
    // Mock the computeElapseTime function to return a specific value
    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(200);
    jest.spyOn(_mockable, "computeTotalCost").mockReturnValue(9);

    await protectedFunction(event);

    expect(onFormSubmitMock).toHaveBeenCalledWith(event);
    expect(_mockable.fetchAndInitFuncConfig).toHaveBeenCalledWith(db, funcName);
    expect(_mockable.fetchAndInitFuncUsage).toHaveBeenCalledWith(db, funcName);
    expect(_mockable.incrementTotalInvocations).toHaveBeenCalledWith(funcUsageRef);
    expect(_mockable.computeTotalCost).toHaveBeenCalledWith(
      mockFuncUsage.totalInvocations+1,
      mockFuncConfig.pricePer1MInvocation,
      mockFuncUsage.totalElapsedTimeInMs,
      mockFuncConfig.pricePer100ms);
    expect(_mockable.disableFunc).not.toHaveBeenCalled();
    expect(_mockable.computeElapseTime).toHaveBeenCalledWith(expect.arrayContaining([expect.any(Number)]), expect.arrayContaining([expect.any(Number)]));
    expect(_mockable.incrementTotalElapsedTimeInMs).toHaveBeenCalledWith(funcUsageRef, 200);
    expect(console.warn).not.toHaveBeenCalled();
  });

  it("should disable function when budget exceeded", async () => {
    const protectedFunction = useBillProtect(onFormSubmitMock);

    // Mock the computeElapseTime function to return a specific value
    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(200);
    jest.spyOn(_mockable, "computeTotalCost").mockReturnValue(10);

    await protectedFunction(event);

    expect(onFormSubmitMock).not.toHaveBeenCalled();
    expect(_mockable.fetchAndInitFuncConfig).toHaveBeenCalledWith(db, funcName);
    expect(_mockable.fetchAndInitFuncUsage).toHaveBeenCalledWith(db, funcName);
    expect(_mockable.incrementTotalInvocations).toHaveBeenCalledWith(funcUsageRef);
    expect(_mockable.computeTotalCost).toHaveBeenCalledWith(
      mockFuncUsage.totalInvocations+1,
      mockFuncConfig.pricePer1MInvocation,
      mockFuncUsage.totalElapsedTimeInMs,
      mockFuncConfig.pricePer100ms);
    expect(_mockable.disableFunc).toHaveBeenCalledWith(funcConfigRef);
    expect(_mockable.computeElapseTime).toHaveBeenCalledWith(expect.arrayContaining([expect.any(Number)]), expect.arrayContaining([expect.any(Number)]));
    expect(_mockable.incrementTotalElapsedTimeInMs).toHaveBeenCalledWith(funcUsageRef, 200);
    expect(console.warn).toHaveBeenCalledWith(`Function ${funcName} has exceeded the cost limit of $10`);
  });

  it("should exit immediately if function is already disabled", async () => {
    jest.spyOn(_mockable, "computeElapseTime").mockReturnValue(200);
    jest.spyOn(_mockable, "computeTotalCost").mockReturnValue(10);
    jest.spyOn(_mockable, "fetchAndInitFuncConfig").mockResolvedValue({
      funcConfigRef,
      funcConfig: mockFuncDisabledConfig,
    });

    const protectedFunction = useBillProtect(onFormSubmitMock);
    await protectedFunction(event);

    expect(onFormSubmitMock).not.toHaveBeenCalled();
    expect(_mockable.fetchAndInitFuncConfig).toHaveBeenCalledWith(db, funcName);
    expect(_mockable.fetchAndInitFuncUsage).toHaveBeenCalledWith(db, funcName);
    expect(_mockable.incrementTotalInvocations).toHaveBeenCalledWith(funcUsageRef);
    expect(_mockable.computeTotalCost).not.toHaveBeenCalled();
    expect(_mockable.disableFunc).not.toHaveBeenCalledWith(funcConfigRef);
    expect(_mockable.computeElapseTime).toHaveBeenCalledWith(expect.arrayContaining([expect.any(Number)]), expect.arrayContaining([expect.any(Number)]));
    expect(_mockable.incrementTotalElapsedTimeInMs).toHaveBeenCalledWith(funcUsageRef, 200);
    expect(console.warn).toHaveBeenCalledWith(`Function ${funcName} is disabled.  Returning`);
  });

  it("should return immediately when hardDisabled is true", async () => {
    jest.spyOn(_mockable, "isHardDisabled").mockReturnValue(true);
    jest.spyOn(_mockable, "computeTotalCost");
    const protectedFunction = useBillProtect(onFormSubmitMock);

    await protectedFunction(event);

    expect(onFormSubmitMock).not.toHaveBeenCalled();
    expect(_mockable.fetchAndInitFuncConfig).not.toHaveBeenCalled();
    expect(_mockable.fetchAndInitFuncUsage).not.toHaveBeenCalled();
    expect(_mockable.incrementTotalInvocations).not.toHaveBeenCalled();
    expect(_mockable.computeTotalCost).not.toHaveBeenCalled();
    expect(_mockable.disableFunc).not.toHaveBeenCalled();
    expect(_mockable.computeElapseTime).not.toHaveBeenCalled();
    expect(_mockable.incrementTotalElapsedTimeInMs).not.toHaveBeenCalled();
    expect(console.warn).toHaveBeenCalledWith(`Function ${funcName} is hard disabled.  Returning immediately`);
  });
});

describe("stopBillingIfBudgetExceeded", () => {
  const pubSubEvent = {
    data: Buffer.from(
      JSON.stringify({
        budgetDisplayName: "Test Budget",
        alertThresholdExceeded: 80,
        costAmount: 100,
        costIntervalStart: "2023-05-16T00:00:00Z",
        budgetAmount: 120,
        budgetAmountType: "SPECIFIED_AMOUNT",
        currencyCode: "USD",
      })
    ).toString("base64"),
    attributes: {},
    _json: "mockJson",
    json: "mockJson",
    toJSON: jest.fn(),
  } as any;

  const pubSubEventOverBudget= {
    data: Buffer.from(
      JSON.stringify({
        budgetDisplayName: "Test Budget",
        alertThresholdExceeded: 80,
        costAmount: 150,
        costIntervalStart: "2023-05-16T00:00:00Z",
        budgetAmount: 120,
        budgetAmountType: "SPECIFIED_AMOUNT",
        currencyCode: "USD",
      } as any)
    ).toString("base64"),
    attributes: {},
    _json: "mockJson",
    json: "mockJson",
    toJSON: jest.fn(),
  } as any;
  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(_mockable, "isBillingEnabled").mockResolvedValue(true);
    jest.spyOn(_mockable, "disableBillingForProject").mockResolvedValue("Billing disabled");
    console.log = jest.fn();
  });

  it("should return 'No action necessary' if costAmount <= budgetAmount", async () => {
    const result = await stopBillingIfBudgetExceeded(pubSubEvent);

    expect(result).toBe(
      "No action necessary. (Current cost: 100)"
    );
    expect(console.log).toHaveBeenCalledWith(
      "No action necessary. (Current cost: 100)"
    );
    expect(_mockable.isBillingEnabled).not.toHaveBeenCalled();
    expect(_mockable.disableBillingForProject).not.toHaveBeenCalled();
  });

  it("should disable billing if costAmount > budgetAmount and billing is enabled", async () => {
    const mockBillingEnabled = true;
    jest.spyOn(_mockable, "isBillingEnabled").mockResolvedValue(mockBillingEnabled);

    const result = await stopBillingIfBudgetExceeded(pubSubEventOverBudget);

    expect(result).toBe("Billing disabled");
    expect(console.log).toHaveBeenCalledWith("Disabling billing");
    expect(_mockable.isBillingEnabled).toHaveBeenCalledWith("projects/test-project");
    expect(_mockable.disableBillingForProject).toHaveBeenCalledWith("projects/test-project");
  });

  it("should return 'Billing already disabled' if billing is already disabled", async () => {
    const mockBillingEnabled = false;
    jest.spyOn(_mockable, "isBillingEnabled").mockResolvedValue(mockBillingEnabled);

    const result = await stopBillingIfBudgetExceeded(pubSubEventOverBudget);

    expect(result).toBe("Billing already disabled");
    expect(console.log).toHaveBeenCalledWith("Billing already disabled");
    expect(_mockable.isBillingEnabled).toHaveBeenCalledWith("projects/test-project");
    expect(_mockable.disableBillingForProject).not.toHaveBeenCalled();
  });
});
