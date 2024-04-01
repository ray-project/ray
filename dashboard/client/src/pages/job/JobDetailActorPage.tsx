import makeStyles from "@mui/styles/makeStyles";
import React, { PropsWithChildren } from "react";

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
      <MainNavPageInfo
        pageInfo={{
          title: "Actors",
          id: "actors",
          path: "actors",
        }}
      />
      <Section title="Actors">
        <ActorList jobId={params.id} />
      </Section>
    </div>
  );
};

export const JobDetailActorDetailWrapper = ({
  children,
}: PropsWithChildren<{}>) => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{
          title: "Actors",
          id: "actors",
          path: "actors",
        }}
      />
      {children}
    </div>
  );
};
