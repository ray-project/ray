import React from "react";
import { MultiTabLogViewer } from "../../common/MultiTabLogViewer";
import { UnifiedJob } from "../../type/job";

type JobDriverLogsProps = {
  job: Pick<UnifiedJob, "driver_node_id" | "submission_id">;
};

export const JobDriverLogs = ({ job }: JobDriverLogsProps) => {
  const { driver_node_id, submission_id } = job;
  const filename = submission_id
    ? `job-driver-${submission_id}.log`
    : undefined;

  return (
    <MultiTabLogViewer
      tabs={[
        {
          title: "driver",
          nodeId: driver_node_id,
          filename,
        },
      ]}
    />
  );
};
