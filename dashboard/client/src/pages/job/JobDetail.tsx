import { Box } from "@mui/material";
import makeStyles from "@mui/styles/makeStyles";
import React, { useRef, useState } from "react";
import useSWR from "swr";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { Section } from "../../common/Section";
import {
  NodeStatusCard,
  ResourceStatusCard,
} from "../../components/AutoscalerStatusCards";
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { getDataDatasets } from "../../service/data";
import { NestedJobProgressLink } from "../../type/job";
import ActorList from "../actor/ActorList";
import DataOverview from "../data/DataOverview";
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

  const [taskListFilter, setTaskListFilter] = useState<string>();
  const [taskTableExpanded, setTaskTableExpanded] = useState(false);
  const taskTableRef = useRef<HTMLDivElement>(null);

  const [actorListFilter, setActorListFilter] = useState<string>();
  const [actorTableExpanded, setActorTableExpanded] = useState(false);
  const actorTableRef = useRef<HTMLDivElement>(null);
  const { cluster_status } = useRayStatus();

  const { data } = useSWR(
    "useDataDatasets",
    async () => {
      const rsp = await getDataDatasets();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: 5000 },
  );

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
      <JobMetadataSection job={job} />

      {data?.datasets && data.datasets.length > 0 && (
        <CollapsibleSection
          title="Ray Data Overview"
          className={classes.section}
        >
          <Section>
            <DataOverview datasets={data.datasets} />
          </Section>
        </CollapsibleSection>
      )}

      <CollapsibleSection
        title="Ray Core Overview"
        startExpanded
        className={classes.section}
      >
        <Section>
          <JobProgressBar
            jobId={job.job_id ? job.job_id : undefined}
            job={job}
            onClickLink={handleClickLink}
          />
        </Section>
      </CollapsibleSection>

      <CollapsibleSection
        title="Logs"
        startExpanded
        className={classes.section}
      >
        <Section noTopPadding>
          <JobDriverLogs job={job} />
        </Section>
      </CollapsibleSection>

      {job.job_id && (
        <CollapsibleSection
          title="Task Timeline (beta)"
          startExpanded
          className={classes.section}
        >
          <Section>
            <TaskTimeline jobId={job.job_id} />
          </Section>
        </CollapsibleSection>
      )}

      <CollapsibleSection
        title="Cluster status and autoscaler"
        startExpanded
        className={classes.section}
      >
        <Box
          display="flex"
          flexDirection="row"
          gap={3}
          alignItems="stretch"
          className={classes.autoscalerSection}
        >
          <NodeCountCard className={classes.nodeCountCard} />
          <Section flex="1 1 500px">
            <NodeStatusCard cluster_status={cluster_status} />
          </Section>
          <Section flex="1 1 500px">
            <ResourceStatusCard cluster_status={cluster_status} />
          </Section>
        </Box>
      </CollapsibleSection>

      {job.job_id && (
        <React.Fragment>
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
              <TaskList
                jobId={job.job_id}
                filterToTaskId={taskListFilter}
                onFilterChange={handleTaskListFilterChange}
              />
            </Section>
          </CollapsibleSection>

          <CollapsibleSection
            ref={actorTableRef}
            title="Actor Table"
            expanded={actorTableExpanded}
            onExpandButtonClick={() => {
              setActorTableExpanded(!actorTableExpanded);
            }}
            className={classes.section}
          >
            <Section>
              <ActorList
                jobId={job.job_id}
                filterToActorId={actorListFilter}
                onFilterChange={handleActorListFilterChange}
                detailPathPrefix="actors"
              />
            </Section>
          </CollapsibleSection>

          <CollapsibleSection
            title="Placement Group Table"
            className={classes.section}
          >
            <Section>
              <PlacementGroupList jobId={job.job_id} />
            </Section>
          </CollapsibleSection>
        </React.Fragment>
      )}
    </div>
  );
};
