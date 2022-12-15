import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import PlacementGroupTable from "../../components/PlacementGroupTable";
import { getPlacementGroup } from "../../service/placementGroup";
import { PlacementGroup } from "../../type/placementGroup";
import { useStateApiList } from "./hook/useStateApi";

/**
 * Represent the embedable actors page.
 */
const PlacementGroupList = ({ jobId = null }: { jobId?: string | null }) => {
  const [timeStamp] = useState(dayjs());
  const data: Array<PlacementGroup> | undefined = useStateApiList(
    "usePlacementGroup",
    getPlacementGroup,
  );
  const placementGroups = data ? data : [];

  return (
    <div>
      <Grid container alignItems="center">
        <Grid item>{timeStamp.format("YYYY-MM-DD HH:mm:ss")}</Grid>
      </Grid>
      <PlacementGroupTable placementGroups={placementGroups} jobId={jobId} />
    </div>
  );
};

export default PlacementGroupList;
