from ray._private.ray_constants import env_integer

NODE_STATS_UPDATE_INTERVAL_SECONDS = env_integer(
    "NODE_STATS_UPDATE_INTERVAL_SECONDS", 5
)
# Deprecated, not used.
UPDATE_NODES_INTERVAL_SECONDS = env_integer("UPDATE_NODES_INTERVAL_SECONDS", 5)
# Used to set a time range to frequently update the node stats.
# Now, it's timeout for a warning message if the head node is not registered.
FREQUENT_UPDATE_TIMEOUT_SECONDS = env_integer("FREQUENT_UPDATE_TIMEOUT_SECONDS", 10)
MAX_COUNT_OF_GCS_RPC_ERROR = 10
