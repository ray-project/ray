import { makeStyles } from "@material-ui/core";
import React from "react";

import { Outlet } from "react-router-dom";
import { Section } from "../../common/Section";
import ActorList from "../actor/ActorList";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useJobDetail } from "./hook/useJobDetail";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    backgroundColor: "white",
  },
}));

export const JobDetailActorsPage = () => {
  const classes = useStyle();
  const { params } = useJobDetail();

  return (
    <div className={classes.root}>
      <Section title="Actors">
        <ActorList jobId={params.id} />
      </Section>
    </div>
  );
};

export const JobDetailActorLayout = () => {
  const { job } = useJobDetail();

  const pageInfo = job
    ? {
        title: "Actors",
        id: "actors",
        path: job.job_id ? `/jobs/${job.job_id}/actors` : undefined,
      }
    : {
        title: "Actors",
        id: "actors",
        path: undefined,
      };

  return (
    <div>
      <MainNavPageInfo pageInfo={pageInfo} />
      <Outlet />
    </div>
  );
};
