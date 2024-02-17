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
import { ServeProxy, ServeSystemActor } from "../../type/serve";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { convertActorStateForServeController } from "./ServeSystemActorDetailPage";

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

export type ServeProxyRowProps = {
  proxy: ServeProxy;
};

export const ServeProxyRow = ({ proxy }: ServeProxyRowProps) => {
  const { status } = proxy;

  return (
    <ServeSystemActorRow
      actor={proxy}
      type="proxy"
      status={<StatusChip type="serveProxy" status={status} />}
    />
  );
};

export type ServeControllerRowProps = {
  controller: ServeSystemActor;
};

export const ServeControllerRow = ({ controller }: ServeControllerRowProps) => {
  const { data: actor } = useFetchActor(controller.actor_id);

  const status = actor?.state;

  return (
    <ServeSystemActorRow
      actor={controller}
      type="controller"
      status={
        status ? (
          <StatusChip
            type="serveController"
            status={convertActorStateForServeController(status)}
          />
        ) : (
          "-"
        )
      }
    />
  );
};

type ServeSystemActorRowProps = {
  actor: ServeSystemActor;
  type: "controller" | "proxy";
  status: React.ReactNode;
};

const ServeSystemActorRow = ({
  actor,
  type,
  status,
}: ServeSystemActorRowProps) => {
  const { node_id, actor_id } = actor;
  const classes = useStyles();

  return (
    <TableRow>
      <TableCell align="center">
        {type === "proxy" ? (
          <Link component={RouterLink} to={`proxies/${node_id}`}>
            HTTPProxyActor:{node_id}
          </Link>
        ) : (
          <Link component={RouterLink} to="controller">
            Serve Controller
          </Link>
        )}
      </TableCell>
      <TableCell align="center">{status}</TableCell>
      <TableCell align="center">
        {type === "proxy" ? (
          <Link component={RouterLink} to={`proxies/${node_id}`}>
            Log
          </Link>
        ) : (
          <Link component={RouterLink} to="controller">
            Log
          </Link>
        )}
      </TableCell>
      <TableCell align="center">
        {node_id ? (
          <Tooltip className={classes.idCol} title={node_id} arrow interactive>
            <Link component={RouterLink} to={`/cluster/nodes/${node_id}`}>
              {node_id}
            </Link>
          </Tooltip>
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">
        {actor_id ? (
          <Tooltip className={classes.idCol} title={actor_id} arrow interactive>
            <Link component={RouterLink} to={`/actors/${actor_id}`}>
              {actor_id}
            </Link>
          </Tooltip>
        ) : (
          "-"
        )}
      </TableCell>
    </TableRow>
  );
};
