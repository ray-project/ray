import { Box } from "@mui/material";
import React from "react";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { Section } from "../../common/Section";
import {
  NodeStatusCard,
  ResourceStatusCard,
} from "../../components/AutoscalerStatusCards";
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { useJobTable } from "../job";
import { useRayStatus } from "../job/hook/useClusterStatus";
import { NodeCountCard } from "../overview/cards/NodeCountCard";
import { useSubmitDetail } from "./hook/useSubmitDetail";
import { SubmitMetadataSection } from "./SubmitDetailInfoPage";
import { SubmitDriverLogs } from "./SubmitDriverLogs";

export const SubmitDetailChartsPage = () => {
  const { submit, msg, isLoading, params } = useSubmitDetail();
  const { clusterStatus } = useRayStatus();
  const { jobTable } = useJobTable(submit?.submission_id);

  if (!submit) {
    return (
      <Box sx={{ padding: 2, backgroundColor: "white" }}>
        <Loading loading={isLoading} />
        <TitleCard title={`SUBMIT - ${params.submitId}`}>
          <StatusChip type="submit" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </Box>
    );
  }
  return (
    <Box sx={{ padding: 2, backgroundColor: "white" }}>
      <SubmitMetadataSection submit={submit} />

      <CollapsibleSection title="Jobs" startExpanded sx={{ marginBottom: 4 }}>
        <Section>{jobTable}</Section>
      </CollapsibleSection>

      <CollapsibleSection title="Logs" startExpanded sx={{ marginBottom: 4 }}>
        <Section noTopPadding>
          <SubmitDriverLogs submit={submit} />
        </Section>
      </CollapsibleSection>

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
    </Box>
  );
};
