import { Box, MenuItem, TextField, Typography } from "@mui/material";
import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import _ from "lodash";
import React, { useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { Section } from "../../common/Section";
import { ServeDeployment, ServeSystemActor } from "../../type/serve";
import { LOG_CONTEXT_KEY_SERVE_DEPLOYMENTS } from "./ServeReplicaDetailPage";
import {
  LOG_CONTEXT_KEY_SERVE_CONTROLLER,
  LOG_CONTEXT_KEY_SERVE_PROXY,
} from "./ServeSystemActorDetailPage";

const useStyles = makeStyles((theme) =>
  createStyles({
    captionText: {
      color: theme.palette.grey[600],
    },
  }),
);

type ServeEntityLogsProps = {
  controller?: ServeSystemActor;
  proxies?: ServeSystemActor[];
  deployments: ServeDeployment[];
};

/**
 * A component that displays a log viewer for all types of Serve logs.
 * A user uses dropdown menus to select the entity they wish to view logs of.
 */
export const ServeEntityLogViewer = ({
  controller,
  proxies,
  deployments,
}: ServeEntityLogsProps) => {
  const classes = useStyles();

  const [params, setParams] = useSearchParams();

  const showEntityGroups = controller !== undefined || proxies !== undefined;
  const defaultEntityGroupName = showEntityGroups
    ? controller
      ? "controller"
      : "proxies"
    : "deployments";

  const selectedEntityGroupName =
    params.get("entityGroup") || defaultEntityGroupName;

  const selectedProxyId =
    params.get("proxyId") || proxies?.[0]?.actor_id || undefined;

  const selectedProxy = proxies?.find(
    ({ actor_id }) => actor_id === selectedProxyId,
  );

  const allReplicas = deployments.flatMap(({ replicas }) => replicas);

  const selectedReplicaId =
    params.get("replicaId") || allReplicas[0]?.replica_id || undefined;

  // Effect to update URL params to initial values if they are not set
  useEffect(() => {
    if (!params.get("entityGroup") && showEntityGroups) {
      params.set("entityGroup", defaultEntityGroupName);
    }
    if (!params.get("proxyId") && selectedProxyId) {
      params.set("proxyId", selectedProxyId);
    }
    if (!params.get("replicaId") && selectedReplicaId) {
      params.set("replicaId", selectedReplicaId);
    }
    setParams(params, { replace: true });
  }, [
    params,
    setParams,
    showEntityGroups,
    defaultEntityGroupName,
    selectedProxyId,
    selectedReplicaId,
  ]);

  const selectedReplica = allReplicas.find(
    ({ replica_id }) => replica_id === selectedReplicaId,
  );

  // Detect if replicaId or http proxy does not exist. If not, reset the selected log.
  useEffect(() => {
    if (selectedProxyId && !selectedProxy) {
      params.delete("proxyId");
    }
    if (selectedReplicaId && !selectedReplica) {
      params.delete("replicaId");
    }
    setParams(params, { replace: true });
  }, [
    params,
    setParams,
    selectedProxy,
    selectedProxyId,
    selectedReplica,
    selectedReplicaId,
  ]);

  const tabs: MultiTabLogViewerTabDetails[] =
    selectedEntityGroupName === "controller" && controller
      ? [
          {
            title: "Controller logs",
            nodeId: controller.node_id,
            filename:
              (controller.log_file_path?.startsWith("/")
                ? controller.log_file_path.substring(1)
                : controller.log_file_path) || undefined,
          },
        ]
      : selectedEntityGroupName === "proxies" && selectedProxy
      ? [
          {
            title: "HTTP Proxy logs",
            nodeId: selectedProxy.node_id,
            filename:
              (selectedProxy.log_file_path?.startsWith("/")
                ? selectedProxy.log_file_path.substring(1)
                : selectedProxy.log_file_path) || undefined,
          },
        ]
      : selectedEntityGroupName === "deployments" && selectedReplica
      ? [
          {
            title: "Serve logger",
            nodeId: selectedReplica.node_id,
            filename:
              (selectedReplica.log_file_path?.startsWith("/")
                ? selectedReplica.log_file_path.substring(1)
                : selectedReplica.log_file_path) || undefined,
          },
          {
            title: "stderr",
            actorId: selectedReplica.actor_id,
            suffix: "err",
          },
          {
            title: "stdout",
            actorId: selectedReplica.actor_id,
            suffix: "out",
          },
        ]
      : [];

  const contextKey =
    selectedEntityGroupName === "controller"
      ? LOG_CONTEXT_KEY_SERVE_CONTROLLER
      : selectedEntityGroupName === "proxies"
      ? LOG_CONTEXT_KEY_SERVE_PROXY
      : LOG_CONTEXT_KEY_SERVE_DEPLOYMENTS;

  return (
    <div>
      <Box
        display="flex"
        flexDirection="row"
        alignItems="center"
        gap={2}
        marginTop={4}
      >
        {showEntityGroups && (
          <Box display="flex" flexDirection="column" gap={1}>
            <Typography>View logs from</Typography>
            <TextField
              select
              variant="outlined"
              size="small"
              style={{ minWidth: 120 }}
              value={selectedEntityGroupName}
              data-testid="entity-group-select"
              SelectProps={{
                renderValue: (value) => _.capitalize(value as string),
              }}
              onChange={({ target: { value } }) => {
                setParams(
                  (params) => {
                    params.set("entityGroup", value);
                    return params;
                  },
                  {
                    replace: true,
                  },
                );
              }}
            >
              <MenuItem value="controller">
                <Box display="flex" flexDirection="column" gap={0.5}>
                  <span>Controller</span>
                  <Typography variant="caption" className={classes.captionText}>
                    Logs for app initialization, dependency installation, and
                    autoscaling.
                  </Typography>
                </Box>
              </MenuItem>
              <MenuItem value="proxies">
                <Box display="flex" flexDirection="column" gap={0.5}>
                  <span>Proxies</span>
                  <Typography variant="caption" className={classes.captionText}>
                    Logs for proxy initialization and HTTP handling.
                  </Typography>
                </Box>
              </MenuItem>
              <MenuItem value="deployments">
                <Box display="flex" flexDirection="column" gap={0.5}>
                  <span>Deployments</span>
                  <Typography variant="caption" className={classes.captionText}>
                    Application output and logs.
                  </Typography>
                </Box>
              </MenuItem>
            </TextField>
          </Box>
        )}
        {selectedEntityGroupName === "proxies" && proxies?.length && (
          <Box display="flex" flexDirection="column" gap={1}>
            <Typography>HTTP Proxy</Typography>
            <TextField
              select
              variant="outlined"
              size="small"
              style={{ minWidth: 240 }}
              value={selectedProxyId}
              data-testid="proxies-select"
              onChange={({ target: { value } }) => {
                setParams(
                  (params) => {
                    params.set("proxyId", value);
                    return params;
                  },
                  {
                    replace: true,
                  },
                );
              }}
            >
              {proxies.map(({ actor_id }) => (
                <MenuItem key={actor_id} value={actor_id || undefined}>
                  HTTPProxyActor:{actor_id}
                </MenuItem>
              ))}
            </TextField>
          </Box>
        )}
        {selectedEntityGroupName === "deployments" && deployments.length && (
          <Box display="flex" flexDirection="column" gap={1}>
            <Typography>Deployment replica</Typography>
            <TextField
              select
              variant="outlined"
              size="small"
              style={{ minWidth: 240 }}
              value={selectedReplicaId}
              data-testid="replicas-select"
              onChange={({ target: { value } }) => {
                setParams(
                  (params) => {
                    params.set("replicaId", value);
                    return params;
                  },
                  {
                    replace: true,
                  },
                );
              }}
            >
              {allReplicas.map(({ replica_id }) => (
                <MenuItem key={replica_id} value={replica_id}>
                  {replica_id}
                </MenuItem>
              ))}
            </TextField>
          </Box>
        )}
      </Box>
      <Box marginTop={2}>
        <Section noTopPadding>
          <MultiTabLogViewer tabs={tabs} contextKey={contextKey} />
        </Section>
      </Box>
    </div>
  );
};
