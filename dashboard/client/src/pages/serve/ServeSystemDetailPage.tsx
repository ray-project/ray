import { Alert, Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
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

const RootDiv = styled("div")(({ theme }) => ({
  padding: theme.spacing(3),
}));

const ServeInstanceWarningAlert = styled(Alert)(({ theme }) => ({
  marginBottom: theme.spacing(2),
}));

const StyledServeMetricsSection = styled(ServeMetricsSection)(({ theme }) => ({
  marginTop: theme.spacing(4),
}));

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
    <RootDiv>
      <MainNavPageInfo
        pageInfo={{
          title: "System",
          id: "serve-system",
          path: "system",
        }}
      />
      {serveDetails.http_options === undefined ? (
        <ServeInstanceWarningAlert severity="warning">
          Serve not started. Please deploy a serve application first.
        </ServeInstanceWarningAlert>
      ) : (
        <ServeSystemDetails
          serveDetails={serveDetails}
          proxies={proxies}
          page={proxiesPage}
          setPage={setProxiesPage}
        />
      )}
      <StyledServeMetricsSection metricsConfig={SERVE_SYSTEM_METRICS_CONFIG} />
    </RootDiv>
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
