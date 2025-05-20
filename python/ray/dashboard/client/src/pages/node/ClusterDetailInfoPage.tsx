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
            {
              label: "Bytedance Ray Client Port",
              content: clusterDetail.data.clientPort
                ? {
                    value: clusterDetail.data.clientPort,
                    copyableValue: clusterDetail.data.clientPort,
                  }
                : { value: "-" },
            },
            {
              label: "Bytedance Ray Job Submit Port",
              content: clusterDetail.data.dashboardPort
                ? {
                    value: clusterDetail.data.dashboardPort,
                    copyableValue: clusterDetail.data.dashboardPort,
                  }
                : { value: "-" },
            },
            {
              label: "Bytedance Ray Cluster Name",
              content: clusterDetail.data.clusterName
                ? {
                    value: clusterDetail.data.clusterName,
                    copyableValue: clusterDetail.data.clusterName,
                  }
                : { value: "-" },
            },
            {
              label: "SCM Version",
              content: clusterDetail.data.bytedScmVersion
                ? {
                    value: clusterDetail.data.bytedScmVersion,
                    copyableValue: clusterDetail.data.bytedScmVersion,
                  }
                : { value: "-" },
            },
          ]}
        />
      </TitleCard>
    </Box>
  );
};
