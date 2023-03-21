import { Link, Typography } from "@material-ui/core";
import React, { useContext } from "react";
import { Link as RouterLink, useParams } from "react-router-dom";
import { GlobalContext } from "../../App";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { ServeDeployment, ServeReplica } from "../../type/serve";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeReplicaDetails } from "./hook/useServeApplications";

export const ServeReplicaDetailPage = () => {
  const { applicationName, deploymentName, replicaId } = useParams();

  const { loading, application, deployment, replica, error } =
    useServeReplicaDetails(applicationName, deploymentName, replicaId);

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (loading) {
    return <Loading loading />;
  } else if (!replica || !deployment || !application) {
    return (
      <Typography color="error">
        {applicationName} / {deploymentName} / {replicaId} not found.
      </Typography>
    );
  }

  const appName = application.name ? application.name : "-";
  const {
    replica_id,
    state,
    actor_id,
    actor_name,
    node_id,
    node_ip,
    pid,
    start_time_s,
  } = replica;
  return (
    <div>
      {/* Extra MainNavPageInfo to add an extra layer of nesting in breadcrumbs */}
      <MainNavPageInfo
        pageInfo={{
          id: "serveDeployentDetail",
          title: deployment.name,
        }}
      />
      <MainNavPageInfo
        pageInfo={{
          id: "serveReplicaDetail",
          title: replica_id,
          path: `/serve/applications/${encodeURIComponent(
            appName,
          )}/${encodeURIComponent(deployment.name)}/${encodeURIComponent(
            replica_id,
          )}`,
        }}
      />
      <MetadataSection
        metadataList={[
          {
            label: "ID",
            content: {
              value: replica_id,
              copyableValue: replica_id,
            },
          },
          {
            label: "State",
            content: <StatusChip type="serveReplica" status={state} />,
          },
          {
            label: "Logs",
            content: (
              <ServeReplicaLogsLink replica={replica} deployment={deployment} />
            ),
          },
          {
            label: "Actor ID",
            content: {
              value: actor_id ? actor_id : "-",
              copyableValue: actor_id ? actor_id : undefined,
            },
          },
          {
            label: "Actor name",
            content: {
              value: actor_name,
              copyableValue: actor_name,
            },
          },
          {
            label: "Node ID",
            content: {
              value: node_id ? node_id : "-",
              copyableValue: node_id ? node_id : undefined,
            },
          },
          {
            label: "Node IP",
            content: {
              value: node_ip ? node_ip : "-",
              copyableValue: node_ip ? node_ip : undefined,
            },
          },
          {
            label: "PID",
            content: {
              value: pid ? pid : "-",
              copyableValue: pid ? pid : undefined,
            },
          },
          {
            label: "Started at",
            content: {
              value: formatDateFromTimeMs(start_time_s * 1000),
            },
          },
          {
            label: "Duration",
            content: <DurationText startTime={start_time_s * 1000} />,
          },
        ]}
      />
    </div>
  );
};

export type ServeReplicaLogsLinkProps = {
  replica: ServeReplica;
  deployment: ServeDeployment;
};

export const ServeReplicaLogsLink = ({
  replica: { replica_id, node_ip },
  deployment: { name: deploymentName },
}: ServeReplicaLogsLinkProps) => {
  const { ipLogMap } = useContext(GlobalContext);

  let link: string | undefined;

  if (node_ip && ipLogMap[node_ip]) {
    // TODO(aguo): Have API return the location of the logs.
    const path = `/serve/deployment_${deploymentName}_${replica_id}.log`;
    link = `/logs/${encodeURIComponent(ipLogMap[node_ip])}/${encodeURIComponent(
      path,
    )}`;
  }

  if (link) {
    return (
      <Link component={RouterLink} to={link} target="_blank" rel="noreferrer">
        Log
      </Link>
    );
  }

  return <span>-</span>;
};
