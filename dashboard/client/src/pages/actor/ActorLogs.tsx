import React from "react";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { ActorDetail } from "../../type/actor";

export type ActorLogsProps = {
  actor: Pick<ActorDetail, "address" | "jobId" | "pid">;
};

export const ActorLogs = ({
  actor: {
    jobId,
    pid,
    address: { workerId, rayletId },
  },
}: ActorLogsProps) => {
  const tabs: MultiTabLogViewerTabDetails[] = [
    {
      title: "stderr",
      nodeId: rayletId,
      // TODO(aguo): Have API return the log file name.
      filename: `worker-${workerId}-${jobId}-${pid}.err`,
    },
    {
      title: "stdout",
      nodeId: rayletId,
      // TODO(aguo): Have API return the log file name.
      filename: `worker-${workerId}-${jobId}-${pid}.out`,
    },
    {
      title: "system",
      nodeId: rayletId,
      // TODO(aguo): Have API return the log file name.
      filename: `python-core-worker-${workerId}_${pid}.log`,
    },
  ];
  return <MultiTabLogViewer tabs={tabs} />;
};
