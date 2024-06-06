import { styled } from "@mui/material/styles";
import React, { PropsWithChildren } from "react";

import { Section } from "../../common/Section";
import ActorList from "../actor/ActorList";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useJobDetail } from "./hook/useJobDetail";

const JobDetailActorsPageRoot = styled("div")(({theme}) => ({
  padding: theme.spacing(2),
  backgroundColor: "white",
}));
export const JobDetailActorsPage = () => {
  const { params } = useJobDetail();

  return (
    <JobDetailActorsPageRoot>
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
    </JobDetailActorsPageRoot>
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
