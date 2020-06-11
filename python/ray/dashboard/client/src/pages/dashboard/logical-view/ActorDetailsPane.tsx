import {
  Grid,
  makeStyles,
  Theme,
  Tooltip,
  Typography,
} from "@material-ui/core";
import React from "react";

type ActorDetailsPaneProps = {
  actorTitle: string;
  actorDetails: {
    label: string;
    value: any;
    tooltip?: string;
  }[];
};

const useStyles = makeStyles((theme: Theme) => ({
  root: {
    fontSize: "0.875rem",
  },
  datum: {
    "&:not(:first-child)": {
      marginLeft: theme.spacing(2),
    },
  },
}));

type LabeledDatumProps = {
  label: string;
  datum: any;
  tooltip?: string;
};

const labeledDatumStyles = makeStyles({
  label: {
    textDecorationLine: "underline",
    textDecorationColor: "light-blue",
    textDecorationThickness: "1px",
    textDecorationStyle: "dotted",
  },
});

const LabeledDatum = ({ label, datum, tooltip }: LabeledDatumProps) => {
  const classes = labeledDatumStyles();
  const innerHtml = (
    <Grid container xs={6}>
      <Grid item xs={8}>
        <span className={classes.label}>{label}</span>
      </Grid>
      <Grid item xs={4}>
        <span>{datum}</span>
      </Grid>
    </Grid>
  );
  return tooltip ? <Tooltip title={tooltip}>{innerHtml}</Tooltip> : innerHtml;
};

const ActorDetailsPane = ({
  actorTitle,
  actorDetails,
}: ActorDetailsPaneProps) => {
  return (
    <React.Fragment>
      <Typography variant="subtitle2">{actorTitle}</Typography>
      <Grid container>
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
