import { Box } from "@mui/material";
import React from "react";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { MainNavPageInfo } from "../layout/mainNavContext";

import { useClusterDetail } from "./hook/useClusterDetail";

export const ClusterDetailInfoPage = () => {
  // TODO(aguo): Add more content to this page!

  const { clusterDetail, msg, isLoading } = useClusterDetail();

  if (!clusterDetail) {
    return (
      <Box sx={{ padding: 2 }}>
        <MainNavPageInfo
          pageInfo={{
            title: "Cluster Info",
            id: "cluster-info",
            path: "info",
          }}
        />
        <Loading loading={isLoading} />
        <TitleCard title={`CLUSTER`}>
          <StatusChip type="cluster" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </Box>
    );
  }

  return (
    <Box sx={{ padding: 2 }}>
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
    </Box>
  );
};
