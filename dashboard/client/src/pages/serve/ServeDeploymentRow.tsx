import { Link, TableCell, TableRow } from "@mui/material";
import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "../../common/CodeDialogButton";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { StatusChip } from "../../components/StatusChip";
import {
  ServeApplication,
  ServeDeployment,
  ServeReplica,
} from "../../type/serve";
import { useViewServeDeploymentMetricsButtonUrl } from "./ServeDeploymentMetricsSection";

const useStyles = makeStyles((theme) =>
  createStyles({
    deploymentName: {
      fontWeight: 400,
    },
    deploymentNameAsFirstColumn: {
      fontWeight: 500, // bold style for when name is the first column, e.g. on the Deployment page
    },
    statusMessage: {
      maxWidth: 400,
      display: "inline-flex",
    },
  }),
);

export type ServeDeploymentRowProps = {
  deployment: ServeDeployment;
  application: ServeApplication;
  // Optional prop to control the visibility of the first column.
  // This is used to display an expand/collapse button on the applications page, but not the deployment page.
  showExpandColumn?: boolean;
};

export const ServeDeploymentRow = ({
  deployment,
  application: { last_deployed_time_s, name: applicationName },
  showExpandColumn = false,
}: ServeDeploymentRowProps) => {
  const { name, status, message, deployment_config, replicas } = deployment;

  const classes = useStyles();

  const metricsUrl = useViewServeDeploymentMetricsButtonUrl(name);

  const deploymentNameClass = showExpandColumn
    ? classes.deploymentName
    : `${classes.deploymentName} ${classes.deploymentNameAsFirstColumn}`;

  return (
    <React.Fragment>
      <TableRow>
        {showExpandColumn && (
          <TableCell>
            {/* Empty column for expand/unexpand button in the row of the parent Serve application. */}
          </TableCell>
        )}
        <TableCell align="center" className={deploymentNameClass}>
          <Link
            component={RouterLink}
            to={`/serve/applications/${encodeURIComponent(
              applicationName,
            )}/${encodeURIComponent(name)}`}
          >
            {name}
          </Link>
        </TableCell>
        <TableCell align="center">
          <StatusChip type="serveDeployment" status={status} />
        </TableCell>
        <TableCell align="center">
          {message ? (
            <CodeDialogButtonWithPreview
              className={classes.statusMessage}
              title="Message details"
              code={message}
            />
          ) : (
            "-"
          )}
        </TableCell>
        <TableCell align="center">
          {" "}
          <Link
            component={RouterLink}
            to={`/serve/applications/${encodeURIComponent(
              applicationName,
            )}/${encodeURIComponent(name)}`}
          >
            {replicas.length}
          </Link>
        </TableCell>
        <TableCell align="center">
          <CodeDialogButton
            title={`Deployment config for ${name}`}
            code={deployment_config}
            buttonText="View config"
          />
          <br />
          <Link
            component={RouterLink}
            to={`/serve/applications/${encodeURIComponent(
              applicationName,
            )}/${encodeURIComponent(name)}`}
          >
            Logs
          </Link>
          {metricsUrl && (
            <React.Fragment>
              <br />
              <Link href={metricsUrl} target="_blank" rel="noreferrer">
                Metrics
              </Link>
            </React.Fragment>
          )}
        </TableCell>
        <TableCell align="center">
          {/* placeholder for route_prefix, which does not apply to a deployment */}
          -
        </TableCell>
        <TableCell align="center">
          {formatDateFromTimeMs(last_deployed_time_s * 1000)}
        </TableCell>
        <TableCell align="center">
          <DurationText startTime={last_deployed_time_s * 1000} />
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

export type ServeReplicaRowProps = {
  replica: ServeReplica;
  deployment: ServeDeployment;
};

export const ServeReplicaRow = ({
  replica,
  deployment,
}: ServeReplicaRowProps) => {
  const { replica_id, state, start_time_s } = replica;
  const { name } = deployment;
  const metricsUrl = useViewServeDeploymentMetricsButtonUrl(name, replica_id);

  return (
    <TableRow>
      <TableCell align="center">
        <Link component={RouterLink} to={`${encodeURIComponent(replica_id)}`}>
          {replica_id}
        </Link>
      </TableCell>
      <TableCell align="center">
        <StatusChip type="serveReplica" status={state} />
      </TableCell>
      <TableCell align="center">
        <Link component={RouterLink} to={`${encodeURIComponent(replica_id)}`}>
          Log
        </Link>
        {metricsUrl && (
          <React.Fragment>
            <br />
            <Link href={metricsUrl} target="_blank" rel="noreferrer">
              Metrics
            </Link>
          </React.Fragment>
        )}
      </TableCell>
      <TableCell align="center">
        {formatDateFromTimeMs(start_time_s * 1000)}
      </TableCell>
      <TableCell align="center">
        <DurationText startTime={start_time_s * 1000} />
      </TableCell>
    </TableRow>
  );
};
