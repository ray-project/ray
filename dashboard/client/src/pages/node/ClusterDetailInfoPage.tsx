import { makeStyles } from "@material-ui/core";
import React from "react";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { MainNavPageInfo } from "../layout/mainNavContext";

import { useClusterDetail } from "./hook/useClusterDetail";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
}));

export const ClusterDetailInfoPage = () => {
  // TODO(aguo): Add more content to this page!

  const classes = useStyle();
  const { clusterDetail, msg } = useClusterDetail();

  if (!clusterDetail) {
    return (
      <div className={classes.root}>
        <MainNavPageInfo
          pageInfo={{
            title: "Cluster Info",
            id: "cluster-info",
            path: undefined,
          }}
        />
        <Loading loading={msg.startsWith("Loading")} />
        <TitleCard title={`CLUSTER`}>
          <StatusChip type="cluster" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </div>
    );
  }

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          title: "Cluster Info",
          id: "cluster-info",
          path: "/cluster/info",
        }}
      />
      <TitleCard title={"Cluster Info"}>
        <MetadataSection
          metadataList={[
            {
              label: "Ray Version",
              content: clusterDetail.data.rayVersion
                ? {
                    value: clusterDetail.data.rayVersion,
                    copyableValue: clusterDetail.data.rayVersion,
                  }
                : { value: "-" },
            },
            {
              label: "Python Version",
              content: clusterDetail.data.pythonVersion
                ? {
                    value: clusterDetail.data.pythonVersion,
                    copyableValue: clusterDetail.data.pythonVersion,
                  }
                : { value: "-" },
            },
            {
              label: "Ray Commit",
              content: clusterDetail.data.gitCommit
                ? {
                    value: clusterDetail.data.gitCommit,
                    copyableValue: clusterDetail.data.gitCommit,
                  }
                : { value: "-" },
            },
            {
              label: "Operating System",
              content: clusterDetail.data.os
                ? {
                    value: clusterDetail.data.os,
                    copyableValue: clusterDetail.data.os,
                  }
                : { value: "-" },
            },
            {
              label: "Session ID",
              content: clusterDetail.data.sessionId
                ? {
                    value: clusterDetail.data.sessionId,
                    copyableValue: clusterDetail.data.sessionId,
                  }
                : { value: "-" },
            },
          ]}
        />
      </TitleCard>
    </div>
  );
};
