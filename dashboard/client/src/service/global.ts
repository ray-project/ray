import { get } from "./requestHandlers";

export type UsageStatsEnabledResponse = {
  usageStatsEnabled: boolean;
  usageStatsPromptEnabled: boolean;
};

export type ClusterMetadataResponse = {
  result: boolean;
  message: string;
  data: {
    rayVersion: string;
    pythonVersion: string;
    sessionId: string;
    gitCommit: string;
    os: string;
  };
};

export const getUsageStatsEnabled = () =>
  get<UsageStatsEnabledResponse>("/usage_stats_enabled");

export const getClusterMetadata = () =>
  get<ClusterMetadataResponse>("/api/v0/cluster_metadata");
