import { makeStyles } from "@material-ui/core";
import React from "react";
import TitleCard from "../../components/TitleCard";
import ActorList from "./ActorList";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
  },
}));

/**
 * Represent the standalone actors page.
 */
const Actors = () => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <TitleCard title="ACTORS">{<ActorList />}</TitleCard>
    </div>
  );
};

export default Actors;
