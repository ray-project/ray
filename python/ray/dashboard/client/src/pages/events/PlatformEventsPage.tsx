import { Box } from "@mui/material";
import React, { useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../common/constants";
import TitleCard from "../../components/TitleCard";
import { getPlatformEvents } from "../../service/node";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { PlatformEvents } from "../node/PlatformEvents";

const PlatformEventsPage = () => {
  const [isRefreshing] = useState(true);

  const { data: platformEvents, isLoading } = useSWR(
    "useClusterPlatformEvents",
    async () => {
      const { data } = await getPlatformEvents();
      return data?.data?.events || [];
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  return (
    <Box sx={{ padding: 2 }}>
      <MainNavPageInfo
        pageInfo={{
          title: "Events",
          pageTitle: "Events | Ray",
          id: "events",
          path: "/events",
        }}
      />
      <TitleCard title="Platform Events">
        <PlatformEvents events={platformEvents || []} isLoading={isLoading} />
      </TitleCard>
    </Box>
  );
};

export default PlatformEventsPage;
