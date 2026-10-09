import {LogicResult, LogicResultDoc, MetricExecution} from "../types";
import {ScheduledEvent} from "firebase-functions/v2/scheduler";
import {hourlyLogicConfigs} from "../index";
import {
  _mockable as _indexUtilsMockable,
  convertLogicResultsToMetricExecutions,
  distributeAndCascadeLogicResultDocs,
  executeLogicWithMetrics,
} from "../index-utils";

export const _mockable = {
  getHourlyLogicConfigs: () => hourlyLogicConfigs,
  saveMetricExecution: (metricExecutions: MetricExecution[]) => _indexUtilsMockable.saveMetricExecution(metricExecutions),
  distributeAndCascadeLogicResultDocs: distributeAndCascadeLogicResultDocs,
  executeLogicWithMetrics: executeLogicWithMetrics,
};

export async function runHourlyBusinessLogics(event: ScheduledEvent): Promise<LogicResult[]> {
  console.info("Running hourly business logics");
  const hourlyConfigs = _mockable.getHourlyLogicConfigs();
  if (!hourlyConfigs || hourlyConfigs.length === 0) {
    console.info("No hourly logic configs found");
    return [];
  }

  const logicResults: LogicResult[] = [];
  for (const config of hourlyConfigs) {
    const result = await _mockable.executeLogicWithMetrics(
      config.name,
      () => config.hourlyLogicFn(event),
    );
    logicResults.push(result);
  }

  const allDocuments: LogicResultDoc[] = logicResults.flatMap((res) => res.documents || []);

  if (allDocuments.length > 0) {
    await _mockable.distributeAndCascadeLogicResultDocs(allDocuments, "no_app_version", "no_app_version");
  }

  const metricExecutions = convertLogicResultsToMetricExecutions(logicResults);
  if (metricExecutions.length > 0) {
    await _mockable.saveMetricExecution(metricExecutions);
  }

  return logicResults;
}

export async function hourlyBusinessLogics(event: ScheduledEvent): Promise<void> {
  await exports.runHourlyBusinessLogics(event);
}
