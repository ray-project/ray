import { Alert, Typography } from "@mui/material";
import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
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
const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
    },
    serveInstanceWarning: {
      marginBottom: theme.spacing(2),
    },
    section: {
      marginTop: theme.spacing(4),
    },
  }),
);

export const ServeSystemDetailPage = () => {
  const classes = useStyles();

  const { serveDetails, proxies, proxiesPage, setProxiesPage, error } =
    useServeDeployments();

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (serveDetails === undefined) {
    return <Loading loading={true} />;
  }

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          title: "System",
          id: "serve-system",
          path: "system",
        }}
      />
      {serveDetails.http_options === undefined ? (
        <Alert className={classes.serveInstanceWarning} severity="warning">
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
        className={classes.section}
        metricsConfig={SERVE_SYSTEM_METRICS_CONFIG}
      />
    </div>
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
