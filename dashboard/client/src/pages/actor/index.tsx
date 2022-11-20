import { Grid, makeStyles, Switch } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useEffect, useState } from "react";
import { API_REFRESH_INTERVAL_MS } from "../../common/constants";
import ActorTable from "../../components/ActorTable";
import TitleCard from "../../components/TitleCard";
import { getActors } from "../../service/actor";
import { Actor } from "../../type/actor";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
  },
}));

const Actors = () => {
  const classes = useStyles();
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
    <div className={classes.root}>
      <TitleCard title="ACTORS">
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
        <ActorTable actors={actors} />
      </TitleCard>
    </div>
  );
};

export default Actors;
