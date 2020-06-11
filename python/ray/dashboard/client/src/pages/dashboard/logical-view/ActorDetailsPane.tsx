import {
  Grid,
  makeStyles,
  Theme,
  Tooltip,
  Typography,
  Divider,
  createStyles
} from "@material-ui/core";
import React from "react";
import { InvalidStateType } from '../../../api';

type ActorStateReprProps = {
    state: -1 | 0 | 1 | 2,
    ist?: InvalidStateType
};

const actorStateReprStyles = makeStyles(
  (theme: Theme) => createStyles({
    infeasible: {
      color: theme.palette.error.light,
    },
    pending: {
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
      color: theme.palette.warning.light
    },
  }));

const ActorStateRepr = ({ state, ist }: ActorStateReprProps) => {
    const classes = actorStateReprStyles();
    switch (state) {
        case -1:
            if (ist === "infeasibleActor")
               return <span className={classes.infeasible}>Infeasible</span>;
            if (ist === "pendingActor") 
               return <span className={classes.pending}>Pending</span>;
            if (!ist)
               return <span className={classes.unknown}>Unknown</span>;
        case 0:
            return <span className={classes.creating}>Creating</span>;
        case 1:
            return <span className={classes.alive}>Alive</span>;
        case 2:
            return <span className={classes.restarting}>Restarting</span>;
    }
};

type ActorDetailsPaneProps = {
  actorTitle: string;
  invalidStateType?: InvalidStateType;
  actorState: -1 | 0 | 1 | 2;
  actorDetails: {
    label: string;
    value: any;
    tooltip?: string;
  }[];
};

const useStyles = makeStyles((theme: Theme) => ({
    divider: {
        width: "100%",
        margin: "0 auto",
    },
    actorTitleWrapper: {
        marginTop: ".50em",
        marginBottom: ".50em",
        fontWeight: "bold",
        fontSize: "130%",
    },
    actorTitle: {
        marginRight: "1em",
    },
    detailsPane: {
        margin: ".5em",
    }
}));

type LabeledDatumProps = {
  label: string;
  datum: any;
  tooltip?: string;
};

const labeledDatumStyles = makeStyles({
  label: {
    textDecorationLine: "underline",
    textDecorationColor: "#a6c3e3",
    textDecorationThickness: "1px",
    textDecorationStyle: "dotted",
    cursor: "help",
  },
});

const LabeledDatum = ({ label, datum, tooltip }: LabeledDatumProps) => {
  const classes = labeledDatumStyles();
  const innerHtml = (
    <Grid container item xs={6}>
      <Grid item xs={6}>
        <span className={classes.label}>{label}</span>
      </Grid>
      <Grid item xs={6}>
        <span>{datum}</span>
      </Grid>
    </Grid>
  );
  return tooltip ? <Tooltip title={tooltip}>{innerHtml}</Tooltip> : innerHtml;
};

const ActorDetailsPane = ({
  actorTitle,
  actorDetails,
  actorState,
  invalidStateType
}: ActorDetailsPaneProps) => {
  const classes = useStyles();
  return (
    <React.Fragment>
      <span className={classes.actorTitleWrapper}>
        <span className={classes.actorTitle}>{actorTitle}</span>
        <ActorStateRepr ist={invalidStateType} state={actorState}/>
      </span>
      <Divider className={classes.divider} />
      <Grid container className={classes.detailsPane}>
        {actorDetails.map(
          ({ label, value, tooltip }) =>
            value &&
            value.length > 0 && (
              <LabeledDatum label={label} datum={value} tooltip={tooltip} />
            ),
        )}
      </Grid>
    </React.Fragment>
  );
};

export default ActorDetailsPane;
