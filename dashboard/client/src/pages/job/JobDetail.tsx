import { Box } from "@mui/material";
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

export const JobDetailChartsPage = () => {
  const { job, msg, isLoading, params } = useJobDetail();

  const [taskListFilter, setTaskListFilter] = useState<string>();
  const [taskTableExpanded, setTaskTableExpanded] = useState(false);
  const taskTableRef = useRef<HTMLDivElement>(null);

  const [actorListFilter, setActorListFilter] = useState<string>();
  const [actorTableExpanded, setActorTableExpanded] = useState(false);
  const actorTableRef = useRef<HTMLDivElement>(null);
  const { clusterStatus } = useRayStatus();

  const { data } = useSWR(
    job?.job_id ? ["useDataDatasets", job.job_id] : null,
    async ([_, jobId]) => {
      // Only display details for Ray Datasets that belong to this job.
      const rsp = await getDataDatasets(jobId);

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: 5000 },
  );

  if (!job) {
    return (
      <Box sx={{ padding: 2, backgroundColor: "white" }}>
        <Loading loading={isLoading} />
        <TitleCard title={`JOB - ${params.id}`}>
          <StatusChip type="job" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </Box>
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
    <Box sx={{ padding: 2, backgroundColor: "white" }}>
      <JobMetadataSection job={job} />

      {data?.datasets && data.datasets.length > 0 && (
        <CollapsibleSection title="Ray Data Overview" sx={{ marginBottom: 4 }}>
          <Section>
            <DataOverview datasets={data.datasets} />
          </Section>
        </CollapsibleSection>
      )}

      <CollapsibleSection
        title="Ray Core Overview"
        startExpanded
        sx={{ marginBottom: 4 }}
      >
        <Section>
          <JobProgressBar
            jobId={job.job_id ? job.job_id : undefined}
            job={job}
            onClickLink={handleClickLink}
          />
        </Section>
      </CollapsibleSection>

      <CollapsibleSection title="Logs" startExpanded sx={{ marginBottom: 4 }}>
        <Section noTopPadding>
          <JobDriverLogs job={job} />
        </Section>
      </CollapsibleSection>

      {job.job_id && (
        <CollapsibleSection
          title="Task Timeline (beta)"
          startExpanded
          sx={{ marginBottom: 4 }}
        >
          <Section>
            <TaskTimeline jobId={job.job_id} />
          </Section>
        </CollapsibleSection>
      )}

      <CollapsibleSection
        title="Cluster status and autoscaler"
        startExpanded
        sx={{ marginBottom: 4 }}
      >
        <Box
          display="flex"
          flexDirection="row"
          gap={3}
          alignItems="stretch"
          sx={(theme) => ({
            flexWrap: "wrap",
            [theme.breakpoints.up("md")]: {
              flexWrap: "nowrap",
            },
          })}
        >
          <NodeCountCard sx={{ flex: "1 0 500px" }} />
          <Section flex="1 1 500px">
            <NodeStatusCard clusterStatus={clusterStatus} />
          </Section>
          <Section flex="1 1 500px">
            <ResourceStatusCard clusterStatus={clusterStatus} />
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
            sx={{ marginBottom: 4 }}
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
            sx={{ marginBottom: 4 }}
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
            sx={{ marginBottom: 4 }}
          >
            <Section>
              <PlacementGroupList jobId={job.job_id} />
            </Section>
          </CollapsibleSection>
        </React.Fragment>
      )}
    </Box>
  );
};
