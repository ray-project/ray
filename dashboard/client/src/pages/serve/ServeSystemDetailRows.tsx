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
import { ServeHttpProxy, ServeSystemActor } from "../../type/serve";
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

export type ServeHttpProxyRowProps = {
  httpProxy: ServeHttpProxy;
};

export const ServeHttpProxyRow = ({ httpProxy }: ServeHttpProxyRowProps) => {
  const { status } = httpProxy;

  return (
    <ServeSystemActorRow
      actor={httpProxy}
      type="httpProxy"
      status={<StatusChip type="serveHttpProxy" status={status} />}
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
  type: "controller" | "httpProxy";
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
        {type === "httpProxy" ? (
          <Link component={RouterLink} to={`httpProxies/${node_id}`}>
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
        {type === "httpProxy" ? (
          <Link component={RouterLink} to={`httpProxies/${node_id}`}>
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
