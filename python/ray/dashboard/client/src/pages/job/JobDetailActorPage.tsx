import { Box } from "@mui/material";
import React, { PropsWithChildren } from "react";

import { Section } from "../../common/Section";
import ActorList from "../actor/ActorList";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useJobDetail } from "./hook/useJobDetail";

export const JobDetailActorsPage = () => {
  const { params } = useJobDetail();

  return (
    <Box sx={{ padding: 2, backgroundColor: "white" }}>
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
    </Box>
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
