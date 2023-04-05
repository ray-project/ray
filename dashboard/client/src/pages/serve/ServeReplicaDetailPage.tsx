import { createStyles, Link, makeStyles, Typography } from "@material-ui/core";
import React, { useContext } from "react";
import { Link as RouterLink, useParams } from "react-router-dom";
import { GlobalContext } from "../../App";
import { CodeDialogButton } from "../../common/CodeDialogButton";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { generateActorLink, generateNodeLink } from "../../common/links";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { ServeDeployment, ServeReplica } from "../../type/serve";
import { MainNavPageInfo } from "../layout/mainNavContext";
import TaskList from "../state/task";
import { useServeReplicaDetails } from "./hook/useServeApplications";
import { ServeReplicaMetricsSection } from "./ServeDeploymentMetricsSection";

const useStyles = makeStyles((theme) =>
  createStyles({
    section: {
      marginTop: theme.spacing(4),
    },
  }),
);

export const ServeReplicaDetailPage = () => {
  const { applicationName, deploymentName, replicaId } = useParams();
  const classes = useStyles();

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
              link: actor_id ? generateActorLink(actor_id) : undefined,
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
              link: node_id ? generateNodeLink(node_id) : undefined,
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
            label: "Deployment config",
            content: (
              <CodeDialogButton
                title={`Deployment config for ${deployment.name}`}
                code={deployment.deployment_config}
              />
            ),
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
      <ServeReplicaMetricsSection
        className={classes.section}
        deploymentName={deployment.name}
        replicaId={replica.replica_id}
      />
      <CollapsibleSection
        className={classes.section}
        title="Tasks History"
        startExpanded
      >
        <TaskList actorId={replica.actor_id ? replica.actor_id : undefined} />
      </CollapsibleSection>
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
    // TODO(aguo): Clean up this logic after re-writing the log viewer
    const logsRoot = ipLogMap[node_ip].endsWith("/logs")
      ? ipLogMap[node_ip].substring(
          0,
          ipLogMap[node_ip].length - "/logs".length,
        )
      : ipLogMap[node_ip];
    // TODO(aguo): Have API return the location of the logs.
    const path = `/logs/serve/deployment_${deploymentName}_${replica_id}.log`;
    link = `/logs/${encodeURIComponent(logsRoot)}/${encodeURIComponent(path)}`;
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
