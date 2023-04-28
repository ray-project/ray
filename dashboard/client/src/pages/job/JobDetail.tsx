<<<<<<< HEAD
import { Grid, makeStyles } from "@material-ui/core";
=======
import { Box, makeStyles, Typography } from "@material-ui/core";
>>>>>>> upstream/master
import React, { useContext, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
<<<<<<< HEAD
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
=======
import { Section } from "../../common/Section";
>>>>>>> upstream/master
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { NestedJobProgressLink, UnifiedJob } from "../../type/job";
import ActorList from "../actor/ActorList";
import { NodeCountCard } from "../overview/cards/NodeCountCard";
import PlacementGroupList from "../state/PlacementGroup";
import TaskList from "../state/task";

import { useRayStatus } from "./hook/useClusterStatus";
import { useJobDetail } from "./hook/useJobDetail";
import { JobMetadataSection } from "./JobDetailInfoPage";
import { JobDriverLogs } from "./JobDriverLogs";
import { JobProgressBar } from "./JobProgressBar";
import { TaskTimeline } from "./TaskTimeline";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    backgroundColor: "white",
  },
  section: {
    marginBottom: theme.spacing(4),
  },
  autoscalerSection: {
    flexWrap: "wrap",
    [theme.breakpoints.up("md")]: {
      flexWrap: "nowrap",
    },
  },
  nodeCountCard: {
    flex: "1 0 500px",
  },
}));

export const JobDetailChartsPage = () => {
  const classes = useStyle();
  const { job, msg, isLoading, params } = useJobDetail();
  const jobId = params.id;

  const [taskListFilter, setTaskListFilter] = useState<string>();
  const [taskTableExpanded, setTaskTableExpanded] = useState(false);
  const taskTableRef = useRef<HTMLDivElement>(null);

  const [actorListFilter, setActorListFilter] = useState<string>();
  const [actorTableExpanded, setActorTableExpanded] = useState(false);
  const actorTableRef = useRef<HTMLDivElement>(null);

<<<<<<< HEAD
  const { cluster_status } = useRayStatus();
=======
  const formatClusterStatus = (title: string, cluster_status: string) => {
    const cluster_status_rows = cluster_status.split("\n");

    return (
      <div>
        <Box marginBottom={2}>
          <Typography variant="h6">{title}</Typography>
        </Box>
        {cluster_status_rows.map((i, key) => {
          // Format the output.
          // See format_info_string in util.py
          if (i.startsWith("-----") || i.startsWith("=====") || i === "") {
            // Ignore separators
            return null;
          } else if (i.endsWith(":")) {
            return (
              <div key={key}>
                <b>{i}</b>
              </div>
            );
          } else {
            return <div key={key}>{i}</div>;
          }
        })}
      </div>
    );
  };
>>>>>>> upstream/master

  if (!job) {
    return (
      <div className={classes.root}>
        <Loading loading={isLoading} />
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
<<<<<<< HEAD
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
          <TitleCard title="">
            <NodeStatusCard cluster_status={cluster_status} />
          </TitleCard>
        </Grid>
        <Grid item xs={4}>
          <TitleCard title="">
            <ResourceStatusCard cluster_status={cluster_status} />
          </TitleCard>
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
=======
      <JobMetadataSection job={job} />

      <CollapsibleSection
        title="Tasks/actor overview (beta)"
        startExpanded
        className={classes.section}
      >
        <Section>
          <JobProgressBar
            jobId={jobId}
            job={job}
            onClickLink={handleClickLink}
          />
        </Section>
      </CollapsibleSection>

      {job.type === "SUBMISSION" && (
        <CollapsibleSection
          title="Driver logs"
          startExpanded
          className={classes.section}
        >
          <Section>
            <JobDriverLogs job={job} />
          </Section>
        </CollapsibleSection>
      )}

      <CollapsibleSection
        title="Task Timeline (beta)"
        startExpanded
        className={classes.section}
      >
        <Section>
          <TaskTimeline jobId={jobId} />
        </Section>
      </CollapsibleSection>

      <CollapsibleSection
        title="Autoscaler"
        startExpanded
        className={classes.section}
      >
        <Box
          display="flex"
          flexDirection="row"
          gridGap={24}
          alignItems="stretch"
          className={classes.autoscalerSection}
        >
          <NodeCountCard className={classes.nodeCountCard} />
          <Section flex="1 1 500px">
            <Box
              style={{
                overflow: "hidden",
                overflowY: "scroll",
              }}
              sx={{ borderRadius: "16px" }}
              marginLeft={1}
              marginRight={1}
            >
              {cluster_status?.data
                ? formatNodeStatus(cluster_status?.data.clusterStatus)
                : "No cluster status."}
            </Box>
          </Section>
          <Section flex="1 1 500px">
            <Box
              style={{
                overflow: "hidden",
                overflowY: "scroll",
              }}
              sx={{ border: 1, borderRadius: "1", borderColor: "primary.main" }}
              marginLeft={1}
              marginRight={1}
            >
              {cluster_status?.data
                ? formatResourcesStatus(cluster_status?.data.clusterStatus)
                : "No cluster status."}
            </Box>
          </Section>
        </Box>
      </CollapsibleSection>

      <CollapsibleSection
        ref={taskTableRef}
        title="Task Table"
        expanded={taskTableExpanded}
        onExpandButtonClick={() => {
          setTaskTableExpanded(!taskTableExpanded);
        }}
        className={classes.section}
      >
        <Section>
>>>>>>> upstream/master
          <TaskList
            jobId={jobId}
            filterToTaskId={taskListFilter}
            onFilterChange={handleTaskListFilterChange}
          />
        </Section>
      </CollapsibleSection>

      <CollapsibleSection
        ref={actorTableRef}
        title="Actors"
        expanded={actorTableExpanded}
        onExpandButtonClick={() => {
          setActorTableExpanded(!actorTableExpanded);
        }}
        className={classes.section}
      >
        <Section>
          <ActorList
            jobId={jobId}
            filterToActorId={actorListFilter}
            onFilterChange={handleActorListFilterChange}
            detailPathPrefix="actors"
          />
        </Section>
      </CollapsibleSection>

      <CollapsibleSection title="Placement Groups" className={classes.section}>
        <Section>
          <PlacementGroupList jobId={jobId} />
        </Section>
      </CollapsibleSection>
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
