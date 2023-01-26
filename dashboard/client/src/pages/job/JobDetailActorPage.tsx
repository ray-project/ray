import { makeStyles } from "@material-ui/core";
import React from "react";

import TitleCard from "../../components/TitleCard";
import ActorList from "../actor/ActorList";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useJobDetail } from "./hook/useJobDetail";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
}));

export const JobDetailActorsPage = () => {
  const classes = useStyle();
  const { job, params } = useJobDetail();

  const pageInfo = job
    ? {
        title: "Actors",
        id: "actors",
        path: job.job_id ? `/new/jobs/${job.job_id}/actors` : undefined,
      }
    : {
        title: "Actors",
        id: "actors",
        path: undefined,
      };

  return (
    <div className={classes.root}>
      <MainNavPageInfo pageInfo={pageInfo} />
      <TitleCard title="Actors">{<ActorList jobId={params.id} />}</TitleCard>
    </div>
  );
};
