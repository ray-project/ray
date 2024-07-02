import { Box } from "@mui/material";
import React from "react";
import { Outlet } from "react-router-dom";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { generateNodeLink } from "../../common/links";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
  MemoryProfilingButton,
} from "../../common/ProfilingLink";
import { Section } from "../../common/Section";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { MainNavPageInfo } from "../layout/mainNavContext";
import TaskList from "../state/task";
import { ActorLogs } from "./ActorLogs";
import { useActorDetail } from "./hook/useActorDetail";

export const ActorDetailLayout = () => {
  const { params, actorDetail } = useActorDetail();

  return (
    <div>
      <MainNavPageInfo
        pageInfo={
          actorDetail
            ? {
                title: `${params.actorId}`,
                pageTitle: `${params.actorId} | Actor`,
                id: "actor-detail",
                path: `${params.actorId}`,
              }
            : {
                id: "actor-detail",
                title: "Actor",
                path: `${params.actorId}`,
              }
        }
      />
      <Outlet />
    </div>
  );
};

const ActorDetailPage = () => {
  const { params, actorDetail, msg, isLoading } = useActorDetail();

  if (isLoading || actorDetail === undefined) {
    return (
      <Box sx={{ padding: 2, backgroundColor: "white" }}>
        <Loading loading={isLoading} />
        <TitleCard title={`ACTOR - ${params.actorId}`}>
          <StatusChip type="actor" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </Box>
    );
  }

  return (
    <Box sx={{ padding: 2, backgroundColor: "white" }}>
      <MetadataSection
        metadataList={[
          {
            label: "State",
            content: <StatusChip type="actor" status={actorDetail.state} />,
          },
          {
            label: "ID",
            content: actorDetail.actorId
              ? {
                  value: actorDetail.actorId,
                  copyableValue: actorDetail.actorId,
                }
              : { value: "-" },
          },
          {
            label: "Name",
            content: actorDetail.name
              ? {
                  value: actorDetail.name,
                }
              : { value: "-" },
          },
          {
            label: "Class Name",
            content: actorDetail.actorClass
              ? {
                  value: actorDetail.actorClass,
                }
              : { value: "-" },
          },
          {
            label: "Repr",
            content: actorDetail.reprName
              ? {
                  value: actorDetail.reprName,
                }
              : { value: "-" },
          },
          {
            label: "Job ID",
            content: actorDetail.jobId
              ? {
                  value: actorDetail.jobId,
                  copyableValue: actorDetail.jobId,
                }
              : { value: "-" },
          },
          {
            label: "Node ID",
            content: actorDetail.address?.rayletId
              ? {
                  value: actorDetail.address?.rayletId,
                  copyableValue: actorDetail.address?.rayletId,
                  link: actorDetail.address.rayletId
                    ? generateNodeLink(actorDetail.address.rayletId)
                    : undefined,
                }
              : { value: "-" },
          },
          {
            label: "Worker ID",
            content: actorDetail.address?.workerId
              ? {
                  value: actorDetail.address?.workerId,
                  copyableValue: actorDetail.address?.workerId,
                }
              : { value: "-" },
          },
          {
            label: "Started at",
            content: {
              value: actorDetail.startTime
                ? formatDateFromTimeMs(actorDetail.startTime)
                : "-",
            },
          },
          {
            label: "Ended at",
            content: {
              value: actorDetail.endTime
                ? formatDateFromTimeMs(actorDetail.endTime)
                : "-",
            },
          },
          {
            label: "Uptime",
            content: actorDetail.startTime ? (
              <DurationText
                startTime={actorDetail.startTime}
                endTime={actorDetail.endTime}
              />
            ) : (
              <React.Fragment>-</React.Fragment>
            ),
          },
          {
            label: "Restarted",
            content: { value: actorDetail.numRestarts },
          },
          {
            label: "Exit Detail",
            content: actorDetail.exitDetail
              ? {
                  value: actorDetail.exitDetail,
                }
              : { value: "-" },
          },
          {
            label: "Actions",
            content: (
              <div>
                <CpuStackTraceLink
                  pid={actorDetail.pid}
                  ip={actorDetail.address?.ipAddress}
                  type=""
                />
                <br />
                <CpuProfilingLink
                  pid={actorDetail.pid}
                  ip={actorDetail.address?.ipAddress}
                  type=""
                />
                <br />
                <MemoryProfilingButton
                  pid={actorDetail.pid}
                  ip={actorDetail.address?.ipAddress}
                  type=""
                />
              </div>
            ),
          },
        ]}
      />
      <CollapsibleSection title="Logs" startExpanded>
        <Section noTopPadding>
          <ActorLogs actor={actorDetail} />
        </Section>
      </CollapsibleSection>
      <CollapsibleSection title="Tasks History" sx={{ marginTop: 4 }}>
        <Section>
          <TaskList jobId={actorDetail.jobId} actorId={params.actorId} />
        </Section>
      </CollapsibleSection>
    </Box>
  );
};

export default ActorDetailPage;
