NODE_STATS_UPDATE_INTERVAL_SECONDS = 1
UPDATE_NODES_INTERVAL_SECONDS = 5
# Until the head node is registered,
# the API server is doing more frequent update
# with this interval.
FREQUENTY_UPDATE_NODES_INTERVAL_SECONDS = 0.1
# If the head node is not updated within
# this timeout, it will stop frequent update.
FREQUENT_UPDATE_TIMEOUT_SECONDS = 10
MAX_COUNT_OF_GCS_RPC_ERROR = 10
