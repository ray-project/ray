import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import DataOverviewTable from "../../components/DataOverviewTable";
import { DatasetMetrics } from "../../type/data";

const DataOverview = ({ datasets = [] }: { datasets: DatasetMetrics[] }) => {
  const [timeStamp] = useState(dayjs());

  return (
    <div>
      <Grid container alignItems="center">
        <Grid item>
          Last updated: {timeStamp.format("YYYY-MM-DD HH:mm:ss")}
        </Grid>
      </Grid>
      <DataOverviewTable datasets={datasets || []} />
    </div>
  );
};

export default DataOverview;
