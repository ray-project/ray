import {
  createStyles,
  makeStyles,
  Theme,
  Tooltip,
  Typography,
} from "@material-ui/core";
import React from "react";
import { ActorState } from "../../../api";

type ActorStateReprProps = {
  state: ActorState;
  showTooltip?: boolean;
  variant?: any;
};

const {
  Alive,
  Dead,
  PendingCreation,
  Restarting,
  DependenciesUnready,
  Infeasible,
  PendingResources,
} = ActorState;

const useActorStateReprStyles = makeStyles((theme: Theme) =>
  createStyles({
    infeasible: {
      color: theme.palette.error.light,
    },
    pendingResources: {
      color: theme.palette.warning.light,
    },
    unknown: {
      color: theme.palette.warning.light,
    },
    creating: {
      color: theme.palette.success.light,
    },
    alive: {
      color: theme.palette.success.dark,
    },
    restarting: {
      color: theme.palette.warning.light,
    },
    dead: {
      color: "#cccccc",
    },
    tooltip: {
      cursor: "help",
    },
  }),
);
const infeasibleTooltip =
  "The actor cannot be created because of insufficient resources in the cluster. Please examine its resource constraints to make sure they are correct or add additional compute to your cluster.";
const pendingResourcesTooltip =
  "The actor is pending resources, such as GPU, Memory, or CPU. It will be created when they become available.";
const aliveTooltip = "The actor is alive and handling remote calls.";
const deadTooltip = "The actor is dead and will not be restarted anymore.";
const restartingTooltip = "The actor died and is restarting.";
const pendingCreationTooltip =
  "The actor's resources and other dependencies are ready, and the Ray backend is processing its creation.";
const dependenciesUnreadyTooltip =
  "The actor is pending creation because it is waiting for one or more of its initialization arguments to be ready.";

const stateToTooltip = {
  [Alive]: aliveTooltip,
  [Dead]: deadTooltip,
  [Infeasible]: infeasibleTooltip,
  [Restarting]: restartingTooltip,
  [PendingCreation]: pendingCreationTooltip,
  [DependenciesUnready]: dependenciesUnreadyTooltip,
  [PendingResources]: pendingResourcesTooltip,
};

const ActorStateRepr: React.FC<ActorStateReprProps> = ({
  state,
  variant,
  showTooltip,
}) => {
  const classes = useActorStateReprStyles();
  const variantOrDefault = variant ?? "body1";
  let body;
  switch (state) {
    case Infeasible:
      body = (
        <Typography variant={variantOrDefault} className={classes.infeasible}>
          Infeasible
        </Typography>
      );
      break;
    case PendingResources:
      body = (
        <Typography
          variant={variantOrDefault}
          className={classes.pendingResources}
        >
          Pending Resources
        </Typography>
      );
      break;
    case PendingCreation:
      body = (
        <Typography variant={variantOrDefault} className={classes.creating}>
          Creating
        </Typography>
      );
      break;
    case DependenciesUnready:
      body = (
        <Typography variant={variantOrDefault} className={classes.creating}>
          Dependencies Unready
        </Typography>
      );
      break;
    case Alive:
      body = (
        <Typography variant={variantOrDefault} className={classes.alive}>
          Alive
        </Typography>
      );
      break;
    case Restarting:
      body = (
        <Typography variant={variantOrDefault} className={classes.restarting}>
          Restarting
        </Typography>
      );
      break;
    case Dead:
      body = (
        <Typography variant={variantOrDefault} className={classes.dead}>
          Dead
        </Typography>
      );
      break;
  }
  return showTooltip ? (
    <Tooltip className={classes.tooltip} title={stateToTooltip[state]}>
      {body}
    </Tooltip>
  ) : (
    body
  );
};

export default ActorStateRepr;
