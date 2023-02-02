import { makeStyles } from "@material-ui/core";
import React from "react";
import TitleCard from "../../components/TitleCard";
import { MainNavPageInfo } from "../layout/mainNavContext";
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
const Actors = ({ newIA = false }: { newIA?: boolean }) => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          id: "actors",
          title: "Actors",
          path: "/new/actors",
        }}
      />
      <TitleCard title="ACTORS">
        <ActorList newIA={newIA} />
      </TitleCard>
    </div>
  );
};

export default Actors;
