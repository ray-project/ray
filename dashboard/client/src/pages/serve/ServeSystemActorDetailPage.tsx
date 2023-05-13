import { Typography } from "@material-ui/core";
import React from "react";
import { useParams } from "react-router-dom";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { generateActorLink, generateNodeLink } from "../../common/links";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { Section } from "../../common/Section";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { ActorDetail } from "../../type/actor";
import { ServeHttpProxy } from "../../type/serve";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeHTTPProxyDetails } from "./hook/useServeApplications";

type ActorInfo = {
  type: "httpProxy";
  detail: ServeHttpProxy;
};

type ServeSystemActorDetailProps = {
  actor: ActorInfo;
};

export const ServeHttpProxyDetailPage = () => {
  const { httpProxyId } = useParams();

  const { httpProxy } = useServeHTTPProxyDetails(httpProxyId);

  if (!httpProxy) {
    return (
      <Typography color="error">
        HTTPProxyActor with id "{httpProxyId}" not found.
      </Typography>
    );
  }

  return (
    <div>
      <MainNavPageInfo
        pageInfo={{
          id: "serveHttpProxy",
          title: `HTTPProxyActor:${httpProxy.node_id}`,
          pageTitle: `${httpProxy.node_id} | Serve HTTPProxyActor`,
          path: `/serve/httpProxies/${encodeURIComponent(httpProxy.node_id)}`,
        }}
      />
      <ServeSystemActorDetail
        actor={{ type: "httpProxy", detail: httpProxy }}
      />
    </div>
  );
};

export const ServeSystemActorDetail = ({
  actor,
}: ServeSystemActorDetailProps) => {
  const name = `HTTPProxyActor:${actor.detail.actor_id}`;

  const { data: fetchedActor } = useFetchActor(actor.detail.actor_id);

  return (
    <div>
      <MetadataSection
        metadataList={[
          {
            label: "Name",
            content: {
              value: name,
            },
          },
          {
            label: "Status",
            content: (
              <StatusChip type="serveReplica" status={actor.detail.status} />
            ),
          },
          {
            label: "Actor ID",
            content: {
              value: actor.detail.actor_id,
              copyableValue: actor.detail.actor_id,
              link: actor.detail.actor_id
                ? generateActorLink(actor.detail.actor_id)
                : undefined,
            },
          },
          {
            label: "Actor name",
            content: {
              value: actor.detail.actor_name,
            },
          },
          {
            label: "Worker ID",
            content: {
              value: actor.detail.worker_id,
            },
          },
          {
            label: "Node ID",
            content: {
              value: actor.detail.node_id,
              copyableValue: actor.detail.node_id,
              link: actor.detail.node_id
                ? generateNodeLink(actor.detail.node_id)
                : undefined,
            },
          },
          {
            label: "Node IP",
            content: {
              value: actor.detail.node_ip,
            },
          },
        ]}
      />
      {fetchedActor && actor.detail.log_file_path && (
        <CollapsibleSection title="Logs" startExpanded>
          <Section noTopPadding>
            <ServeSystemActorLogs
              type="httpProxy"
              actor={fetchedActor}
              systemLogFilePath={actor.detail.log_file_path}
            />
          </Section>
        </CollapsibleSection>
      )}
    </div>
  );
};

type ServeSystemActorLogsProps = {
  type: "controller" | "httpProxy";
  actor: Pick<ActorDetail, "address" | "jobId" | "pid">;
  systemLogFilePath: string;
};

const ServeSystemActorLogs = ({
  type,
  actor: {
    jobId,
    pid,
    address: { workerId, rayletId },
  },
  systemLogFilePath,
}: ServeSystemActorLogsProps) => {
  const tabs: MultiTabLogViewerTabDetails[] = [
    {
      title: type === "controller" ? "Controller" : "HTTP Proxy",
      nodeId: rayletId,
      filename: systemLogFilePath.startsWith("/")
        ? systemLogFilePath.substring(1)
        : systemLogFilePath,
    },
    {
      title: "Actor Logs (stderr)",
      nodeId: rayletId,
      // TODO(aguo): Have API return the log file name.
      filename: `worker-${workerId}-${jobId}-${pid}.err`,
    },
    {
      title: "Actor Logs (stdout)",
      nodeId: rayletId,
      // TODO(aguo): Have API return the log file name.
      filename: `worker-${workerId}-${jobId}-${pid}.out`,
    },
    {
      title: "Actor Logs (system)",
      nodeId: rayletId,
      // TODO(aguo): Have API return the log file name.
      filename: `python-core-worker-${workerId}_${pid}.log`,
    },
  ];
  return <MultiTabLogViewer tabs={tabs} />;
};
