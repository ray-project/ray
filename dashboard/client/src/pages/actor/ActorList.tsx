import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import ActorTable, { ActorTableProps } from "../../components/ActorTable";
import { Actor } from "../../type/actor";
import { useActorList } from "./hook/useActorList";

/**
 * Represent the embedable actors page.
 */
const ActorList = ({
  jobId = null,
  newIA = false,
  detailPathPrefix = "",
  ...actorTableProps
}: {
  jobId?: string | null;
  newIA?: boolean;
  detailPathPrefix?: string;
} & Pick<ActorTableProps, "filterToActorId" | "onFilterChange">) => {
  const [timeStamp] = useState(dayjs());
  const data: { [actorId: string]: Actor } | undefined = useActorList();
  const actors: { [actorId: string]: Actor } = data ? data : {};

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
        newIA={newIA}
        detailPathPrefix={detailPathPrefix}
        {...actorTableProps}
      />
    </div>
  );
};

export default ActorList;
