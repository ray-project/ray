import { Grid, makeStyles } from "@material-ui/core";
import React, { useContext, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
} from "../../common/ProfilingLink";
import {
  NodeStatusCard,
  ResourceStatusCard,
} from "../../components/JobStatusCards";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { NestedJobProgressLink, UnifiedJob } from "../../type/job";
import ActorList from "../actor/ActorList";
import PlacementGroupList from "../state/PlacementGroup";
import TaskList from "../state/task";

import { useRayStatus } from "./hook/useClusterStatus";
import { useJobDetail } from "./hook/useJobDetail";
import { JobProgressBar } from "./JobProgressBar";
import { TaskTimeline } from "./TaskTimeline";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
}));

export const JobDetailChartsPage = () => {
  const classes = useStyle();
  const { job, msg, params } = useJobDetail();
  const jobId = params.id;

  const [taskListFilter, setTaskListFilter] = useState<string>();
  const [taskTableExpanded, setTaskTableExpanded] = useState(false);
  const taskTableRef = useRef<HTMLDivElement>(null);

  const [actorListFilter, setActorListFilter] = useState<string>();
  const [actorTableExpanded, setActorTableExpanded] = useState(false);
  const actorTableRef = useRef<HTMLDivElement>(null);

  const { cluster_status } = useRayStatus();

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

  const handleClickLink = (link: NestedJobProgressLink) => {
    if (link.type === "task") {
      setTaskListFilter(link.id);
      if (!taskTableExpanded) {
        setTaskTableExpanded(true);
        setTimeout(() => {
          // Wait a few ms to give the collapsible view some time to render.
          taskTableRef.current?.scrollIntoView();
        }, 50);
      } else {
        taskTableRef.current?.scrollIntoView();
      }
    } else if (link.type === "actor") {
      setActorListFilter(link.id);
      if (!actorTableExpanded) {
        setActorTableExpanded(true);
        setTimeout(() => {
          // Wait a few ms to give the collapsible view some time to render.
          actorTableRef.current?.scrollIntoView();
        }, 50);
      } else {
        actorTableRef.current?.scrollIntoView();
      }
    }
  };

  const handleTaskListFilterChange = () => {
    setTaskListFilter(undefined);
  };

  const handleActorListFilterChange = () => {
    setActorListFilter(undefined);
  };

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
                  ? formatDateFromTimeMs(job.start_time)
                  : "-",
              },
            },
            {
              label: "Ended at",
              content: {
                value: job.end_time ? formatDateFromTimeMs(job.end_time) : "-",
              },
            },
            {
              label: "Actions",
              content: (
                <div>
                  <JobLogsLink job={job} />
                  <br />
                  <CpuProfilingLink
                    pid={job.driver_info?.pid}
                    ip={job.driver_info?.node_ip_address}
                    type="Driver"
                  />
                  <br />
                  <CpuStackTraceLink
                    pid={job.driver_info?.pid}
                    ip={job.driver_info?.node_ip_address}
                    type="Driver"
                  />
                </div>
              ),
            },
          ]}
        />
      </TitleCard>
      <TitleCard title="Tasks (beta)">
        <JobProgressBar jobId={jobId} job={job} onClickLink={handleClickLink} />
      </TitleCard>
      <TitleCard title="Task Timeline (beta)">
        <TaskTimeline jobId={jobId} />
      </TitleCard>
      <Grid container>
        <Grid item xs={4}>
          <NodeStatusCard cluster_status={cluster_status} />
        </Grid>
        <Grid item xs={4}>
          <ResourceStatusCard cluster_status={cluster_status} />
        </Grid>
      </Grid>
      <TitleCard>
        <CollapsibleSection
          ref={taskTableRef}
          title="Task Table"
          expanded={taskTableExpanded}
          onExpandButtonClick={() => {
            setTaskTableExpanded(!taskTableExpanded);
          }}
        >
          <TaskList
            jobId={jobId}
            filterToTaskId={taskListFilter}
            onFilterChange={handleTaskListFilterChange}
          />
        </CollapsibleSection>
      </TitleCard>
      <TitleCard>
        <CollapsibleSection
          ref={actorTableRef}
          title="Actors"
          expanded={actorTableExpanded}
          onExpandButtonClick={() => {
            setActorTableExpanded(!actorTableExpanded);
          }}
        >
          <ActorList
            jobId={jobId}
            filterToActorId={actorListFilter}
            onFilterChange={handleActorListFilterChange}
            detailPathPrefix="actors"
          />
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
};

export const JobLogsLink = ({
  job: { driver_agent_http_address, driver_info, job_id, submission_id, type },
}: JobLogsLinkProps) => {
  const { ipLogMap } = useContext(GlobalContext);

  let link: string | undefined;

  if (driver_agent_http_address) {
    link = `/logs/${encodeURIComponent(`${driver_agent_http_address}/logs`)}`;
  } else if (driver_info && ipLogMap[driver_info.node_ip_address]) {
    link = `/logs/${encodeURIComponent(ipLogMap[driver_info.node_ip_address])}`;
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
