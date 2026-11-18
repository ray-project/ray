# TODO: Let each probe configure its own timeout and poll interval.

# The time in seconds a NodeMonitor may leave a poll unanswered before it is
# reported as failed.
DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S = 60.0

# Timeout in seconds for each poll to wait for the NodeMonitors.
DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S = 1.0

# Timeout in seconds for the nodes to answer on-demand checks: the pre-flight
# checks and the checks of a Diagnose.
DEFAULT_ON_DEMAND_PROBE_TIMEOUT_S = 60.0

# Seconds between polls for the results of on-demand checks that are waited for.
DEFAULT_ON_DEMAND_PROBE_POLL_INTERVAL_S = 1.0
