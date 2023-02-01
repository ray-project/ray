import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import ActorTable from "../../components/ActorTable";
import { Actor } from "../../type/actor";
import { useActorList } from "./hook/useActorList";

/**
 * Represent the embedable actors page.
 */
const ActorList = ({
  jobId = null,
  detailPathPrefix = null,
}: {
  jobId?: string | null;
  detailPathPrefix: string | null;
}) => {
  const [timeStamp] = useState(dayjs());
  const data: { [actorId: string]: Actor } | undefined = useActorList();
  const actors: { [actorId: string]: Actor } = data ? data : {};
  if (detailPathPrefix === null) {
    detailPathPrefix = "";
  }

  return (
    <div>
      <Grid container alignItems="center">
        <Grid item>
          Last updated: {timeStamp.format("YYYY-MM-DD HH:mm:ss")}
        </Grid>
      </Grid>
      <ActorTable
        actors={actors}
        jobId={jobId}
        detailPathPrefix={detailPathPrefix}
      />
    </div>
  );
};

export default ActorList;
