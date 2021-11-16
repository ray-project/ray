import { makeStyles } from "@material-ui/core";
import React, { useEffect, useState } from "react";
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
  const [actors, setActors] = useState<{ [actorId: string]: Actor }>({});

  useEffect(() => {
    getActors().then((res) => {
      if (res?.data?.data?.actors) {
        setActors(res.data.data.actors);
      }
    });
  }, []);

  return (
    <div className={classes.root}>
      <TitleCard title="ACTORS">
        <ActorTable actors={actors} />
      </TitleCard>
    </div>
  );
};

export default Actors;
