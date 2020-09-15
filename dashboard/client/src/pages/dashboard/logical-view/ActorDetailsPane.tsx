import { Divider, Grid, makeStyles, Theme } from "@material-ui/core";
import React from "react";
import { ActorState } from "../../../api";
import LabeledDatum from "../../../common/LabeledDatum";
import ActorStateRepr from "./ActorStateRepr";

type ActorDetailsPaneProps = {
  actorTitle: string;
  actorState: ActorState;
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
    marginTop: theme.spacing(1),
    marginBottom: theme.spacing(1),
    fontWeight: "bold",
    fontSize: "130%",
  },
  detailsPane: {
    margin: theme.spacing(1),
  },
}));

const ActorDetailsPane: React.FC<ActorDetailsPaneProps> = ({
  actorTitle,
  actorDetails,
  actorState,
}) => {
  const classes = useStyles();
  return (
    <React.Fragment>
      <div className={classes.actorTitleWrapper}>
        <div>{actorTitle}</div>
        <ActorStateRepr state={actorState} />
      </div>
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
