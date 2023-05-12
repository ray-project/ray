import {
  createStyles,
  Link,
  makeStyles,
  TableCell,
  TableRow,
  Tooltip,
} from "@material-ui/core";
import React, { useContext } from "react";
import { Link as RouterLink } from "react-router-dom";
import { GlobalContext } from "../../App";
import { StatusChip } from "../../components/StatusChip";
import { ServeHttpProxy } from "../../type/serve";

const useStyles = makeStyles((theme) =>
  createStyles({
    idCol: {
      display: "inline-block",
      width: "50px",
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
      verticalAlign: "bottom",
    },
  }),
);

export type ServeHttpProxyRowProps = {
  httpProxy: ServeHttpProxy;
};

export const ServeHttpProxyRow = ({ httpProxy }: ServeHttpProxyRowProps) => {
  const { node_id, status, actor_id } = httpProxy;
  const classes = useStyles();

  return (
    <TableRow>
      <TableCell align="center">HTTPProxyActor:{node_id}</TableCell>
      <TableCell align="center">
        <StatusChip type="serveHttpProxy" status={status} />
      </TableCell>
      <TableCell align="center">
        <ServeHttpProxyLogLink httpProxy={httpProxy} />
      </TableCell>
      <TableCell align="center">
        <Tooltip className={classes.idCol} title={node_id} arrow interactive>
          <Link component={RouterLink} to={`/cluster/nodes/${node_id}`}>
            {node_id}
          </Link>
        </Tooltip>
      </TableCell>
      <TableCell align="center">
        <Tooltip className={classes.idCol} title={actor_id} arrow interactive>
          <Link component={RouterLink} to={`/actors/${actor_id}`}>
            {actor_id}
          </Link>
        </Tooltip>
      </TableCell>
    </TableRow>
  );
};

export type ServeReplicaLogsLinkProps = {
  httpProxy: ServeHttpProxy;
};

export const ServeHttpProxyLogLink = ({
  httpProxy: { log_file_path, node_ip },
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
    const path = `/logs${log_file_path}`;
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
