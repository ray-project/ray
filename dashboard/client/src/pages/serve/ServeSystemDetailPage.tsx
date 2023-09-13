import { createStyles, makeStyles, Typography } from "@material-ui/core";
import { Alert } from "@material-ui/lab";
import React from "react";
import { Outlet } from "react-router-dom";
import Loading from "../../components/Loading";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeApplications } from "./hook/useServeApplications";
import { ServeSystemDetails } from "./ServeSystemDetails";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
    },
    serveInstanceWarning: {
      marginBottom: theme.spacing(2),
    },
  }),
);

export const ServeSystemDetailPage = () => {
  const classes = useStyles();

  const {
    serveDetails,
    httpProxies,
    httpProxiesPage,
    setHttpProxiesPage,
    error,
  } = useServeApplications();

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
          httpProxies={httpProxies}
          page={httpProxiesPage}
          setPage={setHttpProxiesPage}
        />
      )}
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
