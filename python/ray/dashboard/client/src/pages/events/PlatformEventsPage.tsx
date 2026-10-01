import { Alert, Box } from "@mui/material";
import React from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../common/constants";
import TitleCard from "../../components/TitleCard";
import { getPlatformEvents } from "../../service/platform";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { PlatformEvents } from "./PlatformEvents";

const errorMessage = (error: any): string => {
  const status = error?.response?.status;
  if (status === 404) {
    return "Platform events are not enabled on this cluster.";
  }
  if (status === 401 || status === 403) {
    return "You are not authorized to view platform events.";
  }
  if (status) {
    return `Failed to load platform events (HTTP ${status}).`;
  }
  return `Failed to load platform events: ${
    error?.message || "unknown error"
  }.`;
};

const PlatformEventsPage = () => {
  const isRefreshing = true;

  const {
    data: platformEvents,
    isLoading,
    error,
  } = useSWR(
    "useClusterPlatformEvents",
    async () => {
      const { data } = await getPlatformEvents();
      return data?.data?.events || [];
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const status = error?.response?.status;
  const isCriticalError = status === 404 || status === 401 || status === 403;

  return (
    <Box sx={{ padding: 2 }}>
      <MainNavPageInfo
        pageInfo={{
          title: "Platform Events",
          pageTitle: "Platform Events",
          id: "platform-events",
          path: "/platform-events",
        }}
      />
      <TitleCard title="Platform Events">
        {error && (
          <Alert severity="error" sx={{ marginBottom: 2 }}>
            {errorMessage(error)}
          </Alert>
        )}
        {isCriticalError ||
        (error && (!platformEvents || platformEvents.length === 0)) ? null : (
          <PlatformEvents events={platformEvents || []} isLoading={isLoading} />
        )}
      </TitleCard>
    </Box>
  );
};

export default PlatformEventsPage;
