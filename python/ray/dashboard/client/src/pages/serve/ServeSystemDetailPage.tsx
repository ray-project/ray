import { Alert, Box, Typography } from "@mui/material";
import React from "react";
import { Outlet } from "react-router-dom";
import Loading from "../../components/Loading";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeDeployments } from "./hook/useServeApplications";
import {
  SERVE_SYSTEM_METRICS_CONFIG,
  ServeMetricsSection,
} from "./ServeMetricsSection";
import { ServeSystemDetails } from "./ServeSystemDetails";

export const ServeSystemDetailPage = () => {
  const { serveDetails, proxies, proxiesPage, setProxiesPage, error } =
    useServeDeployments();

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (serveDetails === undefined) {
    return <Loading loading={true} />;
  }

  return (
    <Box sx={{ padding: 3 }}>
      <MainNavPageInfo
        pageInfo={{
          title: "System",
          id: "serve-system",
          path: "system",
        }}
      />
      {serveDetails.http_options === undefined ? (
        <Alert sx={{ marginBottom: 2 }} severity="warning">
          Serve not started. Please deploy a serve application first.
        </Alert>
      ) : (
        <ServeSystemDetails
          serveDetails={serveDetails}
          proxies={proxies}
          page={proxiesPage}
          setPage={setProxiesPage}
        />
      )}
      <ServeMetricsSection
        sx={{ marginTop: 4 }}
        metricsConfig={SERVE_SYSTEM_METRICS_CONFIG}
      />
    </Box>
  );
};

export const ServeSystemDetailLayout = () => (
  <React.Fragment>
    <MainNavPageInfo
      pageInfo={{
        title: "System",
        id: "serve-system",
        path: "system",
      }}
    />
    <Outlet />
  </React.Fragment>
);
