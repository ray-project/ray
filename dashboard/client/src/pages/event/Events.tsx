import { makeStyles } from "@material-ui/core";
import React from "react";
import EventTable from "../../components/EventTable";
import TitleCard from "../../components/TitleCard";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
}));

const Events = () => {
  const classes = useStyle();

  return (
    <div className={classes.root}>
      <TitleCard title="Event">
        <EventTable />
      </TitleCard>
    </div>
  );
};

export default Events;
