import { Typography } from "@material-ui/core";
import React from "react";
import { Outlet, useParams } from "react-router-dom";
import Loading from "../../components/Loading";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeReplicaDetails } from "./hook/useServeApplications";

export const ServeReplicaDetailLayout = () => {
  const { applicationName, deploymentName, replicaId } = useParams();
  const { loading, application, deployment, replica, error } =
    useServeReplicaDetails(applicationName, deploymentName, replicaId);

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (loading) {
    return <Loading loading />;
  } else if (!replica || !deployment || !application) {
    return (
      <Typography color="error">
        {applicationName} / {deploymentName} / {replicaId} not found.
      </Typography>
    );
  }

  const appName = application.name ? application.name : "-";
  const { replica_id } = replica;

  return (
    <div>
      <MainNavPageInfo
        pageInfo={{
          id: "serveReplicaDetail",
          title: replica_id,
          pageTitle: `${replica_id} | Serve Replica`,
          path: `/serve/applications/${encodeURIComponent(
            appName,
          )}/${encodeURIComponent(deployment.name)}/${encodeURIComponent(
            replica_id,
          )}`,
        }}
      />
      <Outlet />
    </div>
  );
};
