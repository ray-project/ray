import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import useSWR from "swr";
import DataOverviewTable from "../../components/DataOverviewTable";
import { getDataDatasets } from "../../service/data";

const DataOverview = () => {
  const [timeStamp] = useState(dayjs());
  const { data } = useSWR(
    "useDataDatasets",
    async () => {
      const rsp = await getDataDatasets();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: 1000 },
  );

  return (
    <div>
      <Grid container alignItems="center">
        <Grid item>
          Last updated: {timeStamp.format("YYYY-MM-DD HH:mm:ss")}
        </Grid>
      </Grid>
      <DataOverviewTable datasets={data?.datasets || []} />
    </div>
  );
};

export default DataOverview;
