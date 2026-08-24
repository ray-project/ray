import { useEffect, useState } from "react";
import { get } from "../service/requestHandlers";

let cachedRedactionEnabled: boolean | null = null;
let fetchPromise: Promise<void> | null = null;

// Exported for tests: reset the module-level cache so each test starts fresh.
export const _resetRuntimeEnvRedactionCache = () => {
  cachedRedactionEnabled = null;
  fetchPromise = null;
};

const fetchRedactionEnabled = (): Promise<void> => {
  if (cachedRedactionEnabled !== null) {
    return Promise.resolve();
  }
  if (!fetchPromise) {
    fetchPromise = get("/api/v0/runtime_env_redaction")
      .then((res) => {
        cachedRedactionEnabled = res.data?.data?.redactionEnabled ?? false;
      })
      .catch(() => {
        cachedRedactionEnabled = false;
      });
  }
  return fetchPromise;
};

/**
 * Whether the dashboard redacts secrets (e.g. env_vars values) out of the
 * runtime environments it serves to the browser.
 */
export const useRuntimeEnvRedacted = () => {
  const [redacted, setRedacted] = useState(cachedRedactionEnabled ?? false);

  useEffect(() => {
    fetchRedactionEnabled().then(() =>
      setRedacted(cachedRedactionEnabled ?? false),
    );
  }, []);

  return redacted;
};

/**
 * Placeholder the dashboard substitutes for redacted values.
 * Keep in sync with REDACTED_PLACEHOLDER in dashboard/runtime_env_redaction.py.
 */
export const REDACTED_PLACEHOLDER = "<redacted>";

export const RUNTIME_ENV_REDACTED_TOOLTIP =
  "Environment variable values are redacted for security reasons. " +
  "The Ray CLI and Python SDK still return the raw values: run " +
  "`ray list runtime-envs` or `ray job status <id>` from a trusted shell. " +
  "To show them here instead, set RAY_DASHBOARD_REDACT_RUNTIME_ENV=0 on the Ray head node. " +
  "See https://docs.ray.io/en/latest/cluster/configure-manage-dashboard.html#runtime-env-redaction";
