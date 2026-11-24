# The time in seconds a NodeMonitor may leave a poll unanswered before it is
# reported as failed.
DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S = 60.0

# Timeout in seconds for each poll to wait for the NodeMonitors.
DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S = 1.0

# Time added to on-demand probes' longest timeout_s: once so that a probe whose
# command timed out still returns why, and again for the answer to reach the
# controller.
ON_DEMAND_PROBE_GRACE_S = 5.0

# Seconds between polls for the results of on-demand checks that are waited for.
DEFAULT_ON_DEMAND_PROBE_POLL_INTERVAL_S = 1.0
