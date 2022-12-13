import { Grid, Switch } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useEffect, useState } from "react";
import { API_REFRESH_INTERVAL_MS } from "../../common/constants";
import ActorTable from "../../components/ActorTable";
import { getActors } from "../../service/actor";
import { Actor } from "../../type/actor";

/**
 * Represent the embedable actors page.
 */
const ActorList = ({ jobId = null }: { jobId?: string | null }) => {
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [actors, setActors] = useState<{ [actorId: string]: Actor }>({});
  const [timeStamp, setTimeStamp] = useState(dayjs());
  const queryActor = () =>
    getActors().then((res) => {
      if (res?.data?.data?.actors) {
        setActors(res.data.data.actors);
      }
    });

  useEffect(() => {
    let tmo: NodeJS.Timeout;
    const refreshActor = () => {
      const nowTime = dayjs();
      queryActor().then(() => {
        setTimeStamp(nowTime);
        if (autoRefresh) {
          tmo = setTimeout(refreshActor, API_REFRESH_INTERVAL_MS);
        }
      });
    };

    refreshActor();

    return () => {
      clearTimeout(tmo);
    };
  }, [autoRefresh]);

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
      <ActorTable actors={actors} jobId={jobId} />
    </div>
  );
};

export default ActorList;
