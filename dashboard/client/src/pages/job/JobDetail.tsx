import { Box, Grid, makeStyles, Typography } from "@material-ui/core";
import { Alert } from "@material-ui/lab";
import dayjs from "dayjs";
import React, { useContext } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../App";
import { DurationText } from "../../common/DurationText";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { UnifiedJob } from "../../type/job";
import ActorList from "../actor/ActorList";
import PlacementGroupList from "../state/PlacementGroup";
import TaskList from "../state/task";

import { useRayStatus } from "./hook/useClusterStatus";
import { useJobDetail } from "./hook/useJobDetail";
import { useJobProgress } from "./hook/useJobProgress";
import { JobTaskNameProgressTable } from "./JobTaskNameProgressTable";
import { TaskProgressBar } from "./TaskProgressBar";
import { TaskTimeline } from "./TaskTimeline";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  taskProgressTable: {
    marginTop: theme.spacing(2),
  },
}));

type JobDetailChartsPageProps = {
  newIA?: boolean;
};

export const JobDetailChartsPage = ({
  newIA = false,
}: JobDetailChartsPageProps) => {
  const classes = useStyle();
  const { job, msg, params } = useJobDetail();
  const jobId = params.id;
  const { progress, error, driverExists } = useJobProgress(jobId);
  const { cluster_status } = useRayStatus();
  console.log(cluster_status?.data.clusterStatus.split("\n"));

  const FormatNodeStatus = (cluster_status: string) => {
    // ==== auto scaling status
    // Node status
    // ....
    // Resources
    // ....
    const sections = cluster_status.split("Resources");
    return FormatClusterStatus(
      "Node Status",
      sections[0].split("Node status")[1],
    );
  };

  const FormatResourcesStatus = (cluster_status: string) => {
    // ==== auto scaling status
    // Node status
    // ....
    // Resources
    // ....
    const sections = cluster_status.split("Resources");
    return FormatClusterStatus("Resource Status", sections[1]);
  };

  const FormatClusterStatus = (title: string, cluster_status: string) => {
    const cluster_status_rows = cluster_status.split("\n");

    return (
      <div>
        <Typography variant="h6">
          <b>{title}</b>
        </Typography>
        {cluster_status_rows.map((i, key) => {
          // Format the output.
          // See format_info_string in util.py
          if (i.startsWith("-----") || i.startsWith("=====")) {
            // Separator
            return <br />;
          } else if (i.endsWith(":")) {
            return (
              <div key={key}>
                <b>{i}</b>
              </div>
            );
          } else if (i === "") {
            return <br />;
          } else {
            return <div key={key}>{i}</div>;
          }
        })}
      </div>
    );
  };

  if (!job) {
    return (
      <div className={classes.root}>
        <Loading loading={msg.startsWith("Loading")} />
        <TitleCard title={`JOB - ${params.id}`}>
          <StatusChip type="job" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </div>
    );
  }

  const tasksSectionContents = (() => {
    if (!driverExists) {
      return <TaskProgressBar />;
    }
    const { status } = job;
    if (!progress || error) {
      return (
        <Alert severity="warning">
          No tasks visualizations because prometheus is not detected. Please
          make sure prometheus is running and refresh this page. See:{" "}
          <a
            href="https://docs.ray.io/en/latest/ray-observability/ray-metrics.html"
            target="_blank"
            rel="noreferrer"
          >
            https://docs.ray.io/en/latest/ray-observability/ray-metrics.html
          </a>
          .
          <br />
          If you are hosting prometheus on a separate machine or using a
          non-default port, please set the RAY_PROMETHEUS_HOST env var to point
          to your prometheus server when launching ray.
        </Alert>
      );
    }
    if (status === "SUCCEEDED" || status === "FAILED") {
      return (
        <React.Fragment>
          <TaskProgressBar {...progress} showAsComplete />
          <JobTaskNameProgressTable
            className={classes.taskProgressTable}
            jobId={jobId}
          />
        </React.Fragment>
      );
    } else {
      return (
        <React.Fragment>
          <TaskProgressBar {...progress} />
          <JobTaskNameProgressTable
            className={classes.taskProgressTable}
            jobId={jobId}
          />
        </React.Fragment>
      );
    }
  })();

  return (
    <div className={classes.root}>
      <TitleCard title={`JOB - ${params.id}`}>
        <MetadataSection
          metadataList={[
            {
              label: "Entrypoint",
              content: job.entrypoint
                ? {
                    value: job.entrypoint,
                    copyableValue: job.entrypoint,
                  }
                : { value: "-" },
            },
            {
              label: "Status",
              content: <StatusChip type="job" status={job.status} />,
            },
            {
              label: "Job ID",
              content: job.job_id
                ? {
                    value: job.job_id,
                    copyableValue: job.job_id,
                  }
                : { value: "-" },
            },
            {
              label: "Submission ID",
              content: job.submission_id
                ? {
                    value: job.submission_id,
                    copyableValue: job.submission_id,
                  }
                : {
                    value: "-",
                  },
            },
            {
              label: "Duration",
              content: job.start_time ? (
                <DurationText
                  startTime={job.start_time}
                  endTime={job.end_time}
                />
              ) : (
                <React.Fragment>-</React.Fragment>
              ),
            },
            {
              label: "Started at",
              content: {
                value: job.start_time
                  ? dayjs(Number(job.start_time)).format("YYYY/MM/DD HH:mm:ss")
                  : "-",
              },
            },
            {
              label: "Ended at",
              content: {
                value: job.end_time
                  ? dayjs(Number(job.end_time)).format("YYYY/MM/DD HH:mm:ss")
                  : "-",
              },
            },
            {
              label: "Logs",
              content: <JobLogsLink job={job} newIA />,
            },
          ]}
        />
      </TitleCard>
      <TitleCard title="Tasks">{tasksSectionContents}</TitleCard>
      <TitleCard title="Task Timeline">
        <TaskTimeline jobId={jobId} />
      </TitleCard>
      <TitleCard title="">
        <Grid container spacing={3}>
          <Grid item xs={6}>
            <Box
              mb={2}
              display="flex"
              flexDirection="column"
              height="300px"
              style={{
                overflow: "hidden",
                overflowY: "scroll",
              }}
              sx={{ borderRadius: "16px" }}
            >
              {cluster_status?.data
                ? FormatNodeStatus(cluster_status?.data.clusterStatus)
                : "No cluster status."}
            </Box>
          </Grid>
          <Grid item xs={6}>
            <Box
              mb={2}
              display="flex"
              flexDirection="column"
              height="300px"
              style={{
                overflow: "hidden",
                overflowY: "scroll",
              }}
              sx={{ border: 1, borderRadius: "1", borderColor: "primary.main" }}
            >
              {cluster_status?.data
                ? FormatResourcesStatus(cluster_status?.data.clusterStatus)
                : "No cluster status."}
            </Box>
          </Grid>
        </Grid>
      </TitleCard>
      <TitleCard title="Task Table">
        <TaskList jobId={jobId} />
      </TitleCard>
      <TitleCard title="Actors">{<ActorList jobId={jobId} />}</TitleCard>
      <TitleCard title="Placement Groups">
        <PlacementGroupList jobId={jobId} />
      </TitleCard>
    </div>
  );
};

type JobLogsLinkProps = {
  job: Pick<
    UnifiedJob,
    | "driver_agent_http_address"
    | "driver_info"
    | "job_id"
    | "submission_id"
    | "type"
  >;
  newIA?: boolean;
};

export const JobLogsLink = ({
  job: { driver_agent_http_address, driver_info, job_id, submission_id, type },
  newIA = false,
}: JobLogsLinkProps) => {
  const { ipLogMap } = useContext(GlobalContext);

  let link: string | undefined;

  const baseLink = newIA ? "/new/logs" : "/log";

  if (driver_agent_http_address) {
    link = `${baseLink}/${encodeURIComponent(
      `${driver_agent_http_address}/logs`,
    )}`;
  } else if (driver_info && ipLogMap[driver_info.node_ip_address]) {
    link = `${baseLink}/${encodeURIComponent(
      ipLogMap[driver_info.node_ip_address],
    )}`;
  }

  if (link) {
    link += `?fileName=${
      type === "DRIVER" ? job_id : `driver-${submission_id}`
    }`;
    return (
      <Link to={link} target="_blank">
        Log
      </Link>
    );
  }

  return <span>-</span>;
};
