import { Box, Theme, useTheme } from "@mui/material";
import React, { PropsWithChildren } from "react";

import { Section } from "../../common/Section";
import ActorList from "../actor/ActorList";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useJobDetail } from "./hook/useJobDetail";

const useStyle = (theme: Theme) => ({
  root: {
    padding: theme.spacing(2),
    backgroundColor: "white",
  },
});

export const JobDetailActorsPage = () => {
  const styles = useStyle(useTheme());
  const { params } = useJobDetail();

  return (
    <Box sx={styles.root}>
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
