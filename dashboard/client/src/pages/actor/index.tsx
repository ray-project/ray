import { makeStyles } from "@material-ui/core";
import React from "react";
import ActorList from "./ActorList";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
    backgroundColor: "white",
  },
}));

/**
 * Represent the standalone actors page.
 */
const Actors = () => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <ActorList />
    </div>
  );
};

export default Actors;
