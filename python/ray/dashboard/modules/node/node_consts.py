from ray._private.ray_constants import env_float, env_integer

NODE_STATS_UPDATE_INTERVAL_SECONDS = env_integer(
    "NODE_STATS_UPDATE_INTERVAL_SECONDS", 5
)
UPDATE_NODES_INTERVAL_SECONDS = env_integer("UPDATE_NODES_INTERVAL_SECONDS", 5)
# Until the head node is registered,
# the API server is doing more frequent update
# with this interval.
FREQUENTY_UPDATE_NODES_INTERVAL_SECONDS = env_float(
    "FREQUENTY_UPDATE_NODES_INTERVAL_SECONDS", 0.1
)
# If the head node is not updated within
# this timeout, it will stop frequent update.
FREQUENT_UPDATE_TIMEOUT_SECONDS = env_integer("FREQUENT_UPDATE_TIMEOUT_SECONDS", 10)
MAX_COUNT_OF_GCS_RPC_ERROR = 10
