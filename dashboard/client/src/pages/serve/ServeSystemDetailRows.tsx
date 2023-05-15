import {
  createStyles,
  Link,
  makeStyles,
  TableCell,
  TableRow,
  Tooltip,
} from "@material-ui/core";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
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
      <TableCell align="center">
        <Link component={RouterLink} to={`httpProxies/${node_id}`}>
          HTTPProxyActor:{node_id}
        </Link>
      </TableCell>
      <TableCell align="center">
        <StatusChip type="serveHttpProxy" status={status} />
      </TableCell>
      <TableCell align="center">
        <Link component={RouterLink} to={`httpProxies/${node_id}`}>
          Log
        </Link>
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
