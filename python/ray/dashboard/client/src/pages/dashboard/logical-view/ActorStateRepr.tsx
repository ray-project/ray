import { createStyles, makeStyles, Theme, Typography } from "@material-ui/core";
import React from "react";
import { ActorState } from "../../../api";

type ActorStateReprProps = {
  state: ActorState;
  variant?: any;
};

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
  }),
);

const ActorStateRepr: React.FC<ActorStateReprProps> = ({ state, variant }) => {
  const classes = useActorStateReprStyles();
  const {
    Alive,
    Dead,
    PendingCreation,
    Restarting,
    DependenciesUnready,
    Infeasible,
    PendingResources,
  } = ActorState;
  const variantOrDefault = variant ?? "body1";
  switch (state) {
    case Infeasible:
      return (
        <Typography variant={variantOrDefault} className={classes.infeasible}>
          Infeasible
        </Typography>
      );
    case PendingResources:
      return (
        <Typography
          variant={variantOrDefault}
          className={classes.pendingResources}
        >
          Pending
        </Typography>
      );
    case PendingCreation:
      return (
        <Typography variant={variantOrDefault} className={classes.creating}>
          Creating
        </Typography>
      );
    case DependenciesUnready:
      return (
        <Typography variant={variantOrDefault} className={classes.creating}>
          Dependencies Unready
        </Typography>
      );
    case Alive:
      return (
        <Typography variant={variantOrDefault} className={classes.alive}>
          Alive
        </Typography>
      );
    case Restarting:
      return (
        <Typography variant={variantOrDefault} className={classes.restarting}>
          Restarting
        </Typography>
      );
    case Dead:
      return (
        <Typography variant={variantOrDefault} className={classes.dead}>
          Dead
        </Typography>
      );
  }
};

export default ActorStateRepr;
