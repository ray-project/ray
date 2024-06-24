import { Alert, Box, Theme, Typography, useTheme } from "@mui/material";
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
const useStyles = (theme: Theme) => ({
  root: {
    padding: theme.spacing(3),
  },
  serveInstanceWarning: {
    marginBottom: theme.spacing(2),
  },
  section: {
    marginTop: theme.spacing(4),
  },
});

export const ServeSystemDetailPage = () => {
  const styles = useStyles(useTheme());

  const { serveDetails, proxies, proxiesPage, setProxiesPage, error } =
    useServeDeployments();

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (serveDetails === undefined) {
    return <Loading loading={true} />;
  }

  return (
    <Box sx={styles.root}>
      <MainNavPageInfo
        pageInfo={{
          title: "System",
          id: "serve-system",
          path: "system",
        }}
      />
      {serveDetails.http_options === undefined ? (
        <Alert sx={styles.serveInstanceWarning} severity="warning">
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
        sx={styles.section}
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
