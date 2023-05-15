import {
  CircularProgress,
  createStyles,
  makeStyles,
  Typography,
} from "@material-ui/core";
import React from "react";
import { useParams } from "react-router-dom";
import { CodeDialogButton } from "../../common/CodeDialogButton";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { generateActorLink, generateNodeLink } from "../../common/links";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { Section } from "../../common/Section";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { ServeReplica } from "../../type/serve";
import { useFetchActor } from "../actor/hook/useActorDetail";
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
          pageTitle: `${replica_id} | Serve Replica`,
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
      <CollapsibleSection title="Logs" startExpanded>
        <Section noTopPadding>
          <ServeReplicaLogs replica={replica} />
        </Section>
      </CollapsibleSection>
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

type ServeReplicaLogsProps = {
  replica: Pick<ServeReplica, "log_file_path" | "node_id" | "actor_id">;
};

const ServeReplicaLogs = ({
  replica: { log_file_path, node_id, actor_id },
}: ServeReplicaLogsProps) => {
  const { data: actor } = useFetchActor(actor_id);

  if (!actor) {
    return <CircularProgress />;
  }

  const {
    address: { workerId },
    pid,
    jobId,
  } = actor;

  const tabs: MultiTabLogViewerTabDetails[] = [
    {
      title: "stderr",
      nodeId: node_id,
      // TODO(aguo): Have API return the log file name.
      filename: `worker-${workerId}-${jobId}-${pid}.err`,
    },
    {
      title: "stdout",
      nodeId: node_id,
      // TODO(aguo): Have API return the log file name.
      filename: `worker-${workerId}-${jobId}-${pid}.out`,
    },
    {
      title: "system",
      nodeId: node_id,
      // TODO(aguo): Have API return the log file name.
      filename: `python-core-worker-${workerId}_${pid}.log`,
    },
    // TODO(aguo): enable this once state-api logs supports files with # in the name.
    // ...(log_file_path
    //   ? [
    //       {
    //         title: "replica",
    //         nodeId: node_id,
    //         filename: log_file_path.startsWith("/")
    //           ? log_file_path.substring(1)
    //           : log_file_path,
    //       },
    //     ]
    //   : []),
  ];
  return <MultiTabLogViewer tabs={tabs} />;
};
