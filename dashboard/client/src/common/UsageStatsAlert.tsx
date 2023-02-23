import { Alert } from "@material-ui/lab";
import React, { useEffect, useState } from "react";
import { getUsageStatsEnabled } from "../service/global";

export const UsageStatsAlert = () => {
  const [usageStatsPromptEnabled, setUsageStatsPromptEnabled] = useState(false);
  const [usageStatsEnabled, setUsageStatsEnabled] = useState(false);
  useEffect(() => {
    getUsageStatsEnabled().then(({ data }) => {
      setUsageStatsPromptEnabled(data.usageStatsPromptEnabled);
      setUsageStatsEnabled(data.usageStatsEnabled);
    });
  }, []);

  return usageStatsPromptEnabled ? (
    <Alert style={{ marginTop: 30 }} severity="info">
      {usageStatsEnabled ? (
        <span>
          Usage stats collection is enabled. To disable this, add
          `--disable-usage-stats` to the command that starts the cluster, or run
          the following command: `ray disable-usage-stats` before starting the
          cluster. See{" "}
          <a
            href="https://docs.ray.io/en/master/cluster/usage-stats.html"
            target="_blank"
            rel="noreferrer"
          >
            https://docs.ray.io/en/master/cluster/usage-stats.html
          </a>{" "}
          for more details.
        </span>
      ) : (
        <span>Usage stats collection is disabled.</span>
      )}
    </Alert>
  ) : null;
};
