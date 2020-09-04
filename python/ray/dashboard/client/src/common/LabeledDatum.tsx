import { Grid, makeStyles, Tooltip } from "@material-ui/core";
import React from "react";

type LabeledDatumProps = {
  label: string;
  datum: any;
  tooltip?: string;
};

const useLabeledDatumStyles = makeStyles({
  tooltipLabel: {
    textDecorationLine: "underline",
    textDecorationColor: "#a6c3e3",
    textDecorationThickness: "1px",
    textDecorationStyle: "dotted",
    cursor: "help",
  },
});

const LabeledDatum: React.FC<LabeledDatumProps> = ({
  label,
  datum,
  tooltip,
}) => {
  const classes = useLabeledDatumStyles();
  const innerHtml = (
    <Grid container item xs={6}>
      <Grid item xs={6}>
        <span className={tooltip && classes.tooltipLabel}>{label}</span>
      </Grid>
      <Grid item xs={6}>
        <span>{datum}</span>
      </Grid>
    </Grid>
  );
  return tooltip ? <Tooltip title={tooltip}>{innerHtml}</Tooltip> : innerHtml;
};

export default LabeledDatum;
