import { Grid } from "@material-ui/core";
import React from "react";
import { StatusChip } from "./StatusChip";

const StateCounter = ({
  type,
  list,
}: {
  type: string;
  list: { state: string }[];
}) => {
  const stateMap = {} as { [state: string]: number };
  list.forEach(({ state }) => {
    stateMap[state] = stateMap[state] + 1 || 1;
  });

  return (
    <Grid container spacing={2} alignItems="center">
      <Grid item>
        <StatusChip status="TOTAL" type={type} suffix={`x ${list.length}`} />
      </Grid>
      {Object.entries(stateMap).map(([s, num]) => (
        <Grid item>
          <StatusChip status={s} type={type} suffix={` x ${num}`} />
        </Grid>
      ))}
    </Grid>
  );
};

export default StateCounter;
