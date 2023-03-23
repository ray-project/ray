import {
  createStyles,
  IconButton,
  makeStyles,
  TableCell,
  TableRow,
} from "@material-ui/core";
import React, { useState } from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { Link } from "react-router-dom";
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
import { ServeReplicaLogsLink } from "./ServeReplicaDetailPage";

const useStyles = makeStyles((theme) =>
  createStyles({
    deploymentName: {
      fontWeight: 500,
    },
    expandCollapseIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
  }),
);

export type ServeDeployentRowProps = {
  deployment: ServeDeployment;
  application: ServeApplication;
};

export const ServeDeploymentRow = ({
  deployment,
  application: { last_deployed_time_s },
}: ServeDeployentRowProps) => {
  const { name, status, message, deployment_config, replicas } = deployment;

  const classes = useStyles();

  const [expanded, setExpanded] = useState(false);

  return (
    <React.Fragment>
      <TableRow>
        <TableCell align="center">
          <IconButton
            size="small"
            onClick={() => {
              setExpanded(!expanded);
            }}
          >
            {!expanded ? (
              <RiArrowRightSLine
                className={classes.expandCollapseIcon}
                title="Expand"
              />
            ) : (
              <RiArrowDownSLine
                className={classes.expandCollapseIcon}
                title="Collapse"
              />
            )}
          </IconButton>
        </TableCell>
        <TableCell align="center" className={classes.deploymentName}>
          {name}
        </TableCell>
        <TableCell align="center">{Object.keys(replicas).length}</TableCell>
        <TableCell align="center">
          <StatusChip type="serveDeployment" status={status} />
        </TableCell>
        <TableCell align="center">
          <CodeDialogButton
            title={`Deployment config for ${name}`}
            code={deployment_config}
            buttonText="Deployment config"
          />
        </TableCell>
        <TableCell align="center">
          {message ? (
            <CodeDialogButtonWithPreview
              title="Message details"
              code={message}
            />
          ) : (
            "-"
          )}
        </TableCell>
        <TableCell align="center">
          {formatDateFromTimeMs(last_deployed_time_s * 1000)}
        </TableCell>
        <TableCell align="center">
          <DurationText startTime={last_deployed_time_s * 1000} />
        </TableCell>
      </TableRow>
      {expanded &&
        replicas.map((replica) => (
          <ServeReplicaRow
            key={replica.replica_id}
            replica={replica}
            deployment={deployment}
          />
        ))}
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

  return (
    <TableRow>
      <TableCell align="center"></TableCell>
      <TableCell align="center">
        <Link
          to={`${encodeURIComponent(name)}/${encodeURIComponent(replica_id)}`}
        >
          {replica_id}
        </Link>
      </TableCell>
      <TableCell align="center">-</TableCell>
      <TableCell align="center">
        <StatusChip type="serveReplica" status={state} />
      </TableCell>
      <TableCell align="center">
        <ServeReplicaLogsLink replica={replica} deployment={deployment} />
      </TableCell>
      <TableCell align="center">-</TableCell>
      <TableCell align="center">
        {formatDateFromTimeMs(start_time_s * 1000)}
      </TableCell>
      <TableCell align="center">
        <DurationText startTime={start_time_s * 1000} />
      </TableCell>
    </TableRow>
  );
};
