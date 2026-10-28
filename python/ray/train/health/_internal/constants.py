# TODO: Let each probe configure its own timeout and poll interval.

# The time in seconds a NodeMonitor may leave a poll unanswered before it is
# reported as failed.
DEFAULT_NODE_MONITOR_HEALTH_CHECK_TIMEOUT_S = 60.0

# Timeout in seconds for each poll to wait for the NodeMonitors.
DEFAULT_NODE_MONITOR_POLL_TIMEOUT_S = 1.0
