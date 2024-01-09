import React, { useContext } from "react";
import { GlobalContext } from "../../App";
import { MultiTabLogViewer } from "../../common/MultiTabLogViewer";
import { UnifiedJob } from "../../type/job";

type JobDriverLogsProps = {
  job: Pick<
    UnifiedJob,
    | "job_id"
    | "driver_node_id"
    | "submission_id"
    | "driver_agent_http_address"
    | "driver_info"
    | "type"
  >;
};

export const JobDriverLogs = ({ job }: JobDriverLogsProps) => {
  const { driver_node_id, submission_id, type } = job;
  const filename = submission_id
    ? `job-driver-${submission_id}.log`
    : undefined;

  const { nodeMapByIp } = useContext(GlobalContext);

  let link: string | undefined;

  if (job.driver_node_id) {
    link = `/logs/?nodeId=${encodeURIComponent(job.driver_node_id)}`;
  } else if (job.driver_info?.node_id) {
    link = `/logs/?nodeId=${encodeURIComponent(job.driver_info.node_id)}`;
  } else if (job.driver_info?.node_ip_address) {
    link = `/logs/?nodeId=${encodeURIComponent(
      nodeMapByIp[job.driver_info.node_ip_address],
    )}`;
  }

  if (link && job.job_id) {
    link += `&fileName=${job.job_id}`;
  } else {
    // Don't show "other logs" link if link is not available
    // or job_id does not exist.
    link = undefined;
  }

  return (
    <MultiTabLogViewer
      tabs={[
        type === "SUBMISSION"
          ? {
              title: "Driver",
              nodeId: driver_node_id,
              filename,
            }
          : {
              title: "Driver",
              contents:
                "Driver logs are only available when submitting jobs via the " +
                "Job Submission API, SDK or the `ray job submit` CLI command.\n" +
                "To learn more, please read the documentation at " +
                "https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html",
            },
      ]}
      otherLogsLink={link}
    />
  );
};
