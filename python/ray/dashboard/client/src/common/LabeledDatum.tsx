import { Box, Grid, Tooltip } from "@mui/material";
import React, { ReactChild } from "react";

type LabeledDatumProps = {
  label: ReactChild;
  datum: any;
  tooltip?: string;
};

const LabeledDatum: React.FC<LabeledDatumProps> = ({
  label,
  datum,
  tooltip,
}) => {
  const innerHtml = (
    <Grid container item xs={12}>
      <Grid item xs={6}>
        <Box
          sx={
            tooltip
              ? {
                  textDecorationLine: "underline",
                  textDecorationColor: "#a6c3e3",
                  textDecorationThickness: "1px",
                  textDecorationStyle: "dotted",
                  cursor: "help",
                }
              : {}
          }
        >
          {label}
        </Box>
      </Grid>
      <Grid item xs={6}>
        <span>{datum}</span>
      </Grid>
    </Grid>
  );
  return tooltip ? <Tooltip title={tooltip}>{innerHtml}</Tooltip> : innerHtml;
};

export default LabeledDatum;
