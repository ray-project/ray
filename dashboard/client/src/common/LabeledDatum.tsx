import { Box, Grid, Theme, Tooltip, useTheme } from "@mui/material";
import React, { ReactChild } from "react";

type LabeledDatumProps = {
  label: ReactChild;
  datum: any;
  tooltip?: string;
};

const useLabeledDatumStyles = (theme: Theme) => ({
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
  const styles = useLabeledDatumStyles(useTheme());
  const innerHtml = (
    <Grid container item xs={12}>
      <Grid item xs={6}>
        <Box sx={tooltip ? styles.tooltipLabel : {}}>{label}</Box>
      </Grid>
      <Grid item xs={6}>
        <span>{datum}</span>
      </Grid>
    </Grid>
  );
  return tooltip ? <Tooltip title={tooltip}>{innerHtml}</Tooltip> : innerHtml;
};

export default LabeledDatum;
