import { Grid, Switch } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useCallback, useEffect, useState } from "react";
import { API_REFRESH_INTERVAL_MS } from "../../common/constants";
import PlacementGroupTable from "../../components/PlacementGroupTable";
import { getPlacementGroup } from "../../service/placementGroup";
import { PlacementGroup } from "../../type/placementGroup";

/**
 * Represent the embedable actors page.
 */
const PlacementGroupList = ({ jobId = null }: { jobId?: string | null }) => {
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [placementGroups, setPlacementGroups] = useState<Array<PlacementGroup>>(
    [],
  );
  const [timeStamp, setTimeStamp] = useState(dayjs());
  const queryPlacementGroup = useCallback(
    () =>
      getPlacementGroup().then((res) => {
        if (res?.data?.data?.result?.result) {
          setPlacementGroups(res.data.data.result.result);
        }
      }),
    [],
  );

  useEffect(() => {
    let tmo: NodeJS.Timeout;
    const refreshPlacementGroup = () => {
      const nowTime = dayjs();
      queryPlacementGroup().then(() => {
        setTimeStamp(nowTime);
        if (autoRefresh) {
          tmo = setTimeout(refreshPlacementGroup, API_REFRESH_INTERVAL_MS);
        }
      });
    };

    refreshPlacementGroup();

    return () => {
      clearTimeout(tmo);
    };
  }, [autoRefresh, queryPlacementGroup]);

  return (
    <div>
      <Grid container alignItems="center">
        <Grid item>
          Auto Refresh:{" "}
          <Switch
            checked={autoRefresh}
            onChange={({ target: { checked } }) => setAutoRefresh(checked)}
          />
        </Grid>
        <Grid item>{timeStamp.format("YYYY-MM-DD HH:mm:ss")}</Grid>
      </Grid>
      <PlacementGroupTable placementGroups={placementGroups} jobId={jobId} />
    </div>
  );
};

export default PlacementGroupList;
