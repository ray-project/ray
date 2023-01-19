import { Button, makeStyles } from "@material-ui/core";
import { Alert } from "@material-ui/lab";
import dayjs from "dayjs";
import React from "react";
import { DurationText } from "../../common/DurationText";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { getTaskTimeline } from "../../service/task";
import ActorList from "../actor/ActorList";
import PlacementGroupList from "../state/PlacementGroup";
import TaskList from "../state/task";

import { useJobDetail } from "./hook/useJobDetail";
import { useJobProgress } from "./hook/useJobProgress";
import { JobTaskNameProgressTable } from "./JobTaskNameProgressTable";
import { TaskProgressBar } from "./TaskProgressBar";

const ORIGIN = "https://ui.perfetto.dev";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  taskProgressTable: {
    marginTop: theme.spacing(2),
  },
}));

const onClick = (job_id: string | null) => {
  getTaskTimeline(job_id).then((resp) => {
    console.log(resp);
    openTrace(resp.data);
  });
};

let timer: null | any = undefined;

const openTrace = (arrayBuffer: any) => {
  const win = window.open(ORIGIN);
  window.addEventListener("message", (evt) => onMessage(evt, win, arrayBuffer));
  if (win) {
    timer = setInterval(() => win.postMessage("PING", ORIGIN), 50);
  }
};

const onMessage = (evt: any, win: any, arrayBuffer: any) => {
  console.log("ping");
  if (evt.data !== "PONG") {
    console.log("ping not done");
    return;
  }
  console.log("ping done");
  window.clearInterval(timer);
  console.log(arrayBuffer);
  win.postMessage(arrayBuffer, ORIGIN);
};

export const JobDetailChartsPage = () => {
  const classes = useStyle();
  const { job, msg, params } = useJobDetail();
  const jobId = params.id;
  const { progress, error, driverExists } = useJobProgress(jobId);

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
          ]}
        />
        <Button onClick={() => onClick(job.job_id)} color="primary">
          Timeline
        </Button>
      </TitleCard>
      <TitleCard title="Tasks">{tasksSectionContents}</TitleCard>
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
