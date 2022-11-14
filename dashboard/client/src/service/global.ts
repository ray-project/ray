import { get } from "./requestHandlers";

export type UsageStatsEnabledResponse = {
  usageStatsEnabled: boolean;
  usageStatsPromptEnabled: boolean;
};

export const getUsageStatsEnabled = () =>
  get<UsageStatsEnabledResponse>("/usage_stats_enabled");
