import { makeStyles } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useContext } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { UnifiedJob } from "../../type/job";
import ActorList from "../actor/ActorList";
import PlacementGroupList from "../state/PlacementGroup";
import TaskList from "../state/task";

import { useJobDetail } from "./hook/useJobDetail";
import { JobProgressBar } from "./JobProgressBar";
import { TaskTimeline } from "./TaskTimeline";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
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
      <TitleCard title="Tasks">
        <JobProgressBar jobId={jobId} job={job} />
      </TitleCard>
      <TitleCard title="Task Timeline">
        <TaskTimeline jobId={jobId} />
      </TitleCard>
      <TitleCard>
        <CollapsibleSection title="Task Table">
          <TaskList jobId={jobId} />
        </CollapsibleSection>
      </TitleCard>
      <TitleCard>
        <CollapsibleSection title="Actors">
          <ActorList jobId={jobId} />
        </CollapsibleSection>
      </TitleCard>
      <TitleCard>
        <CollapsibleSection title="Placement Groups">
          <PlacementGroupList jobId={jobId} />
        </CollapsibleSection>
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
