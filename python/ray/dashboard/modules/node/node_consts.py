from ray._private.ray_constants import env_integer

NODE_STATS_UPDATE_INTERVAL_SECONDS = env_integer(
    "NODE_STATS_UPDATE_INTERVAL_SECONDS", 15
)
RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT = env_integer(
    "RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT", 10
)
MAX_COUNT_OF_GCS_RPC_ERROR = 10
# This is consistent with gcs_node_manager.cc
MAX_DEAD_NODES_TO_CACHE = env_integer("RAY_maximum_gcs_dead_node_cached_count", 1000)
RAY_DASHBOARD_NODE_SUBSCRIBER_POLL_SIZE = env_integer(
    "RAY_DASHBOARD_NODE_SUBSCRIBER_POLL_SIZE", 200
)
RAY_DASHBOARD_AGENT_POLL_INTERVAL_S = env_integer(
    "RAY_DASHBOARD_AGENT_POLL_INTERVAL_S", 1
)
# How often the dashboard re-fetches the full node list from GCS to reconcile
# the locally-cached node state. The dashboard normally maintains node state
# via the GCS pub/sub channel, but pub/sub is best-effort and a missed message
# (e.g., during a GCS failover or subscriber timeout) can leave a node stuck
# in a stale state in the dashboard until restart. Periodic reconciliation
# bounds this divergence. Set to 0 to disable.
RAY_DASHBOARD_NODE_RECONCILIATION_INTERVAL_SECONDS = env_integer(
    "RAY_DASHBOARD_NODE_RECONCILIATION_INTERVAL_SECONDS", 60
)
