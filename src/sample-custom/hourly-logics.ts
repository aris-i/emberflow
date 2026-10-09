import * as admin from "firebase-admin";
import {HourlyLogicConfig, HourlyLogicFn} from "../types";

const sampleHourlySyncLogic: HourlyLogicFn = async (event) => {
  console.log(`Executing sampleHourlySyncLogic at ${event.scheduleTime}...`);

  return {
    name: "SampleHourlySyncLogic",
    status: "finished",
    documents: [
      {
        action: "merge",
        dstPath: "system/hourly-status",
        doc: {
          lastRunAt: admin.firestore.Timestamp.now(),
          scheduleTime: event.scheduleTime,
        },
      },
    ],
  };
};

const hourlyLogicConfigs: HourlyLogicConfig[] = [
  {
    name: "SampleHourlySyncLogic",
    hourlyLogicFn: sampleHourlySyncLogic,
  },
];

export {hourlyLogicConfigs};
