# Environment variables for the aggregator agent publisher component.
from ray._private import ray_constants

env_var_prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER"
# Timeout for the publisher to publish events to the destination
PUBLISHER_TIMEOUT_SECONDS = ray_constants.env_integer(
    f"{env_var_prefix}_TIMEOUT_SECONDS", 3
)
# Maximum number of retries for publishing events to the destination, if less than 0, will retry indefinitely
PUBLISHER_MAX_RETRIES = ray_constants.env_integer(f"{env_var_prefix}_MAX_RETRIES", -1)
# Initial backoff time for publishing events to the destination
PUBLISHER_INITIAL_BACKOFF_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_INITIAL_BACKOFF_SECONDS", 0.01
)
# Maximum backoff time for publishing events to the destination
PUBLISHER_MAX_BACKOFF_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_MAX_BACKOFF_SECONDS", 5.0
)
# Jitter ratio for publishing events to the destination
PUBLISHER_JITTER_RATIO = ray_constants.env_float(f"{env_var_prefix}_JITTER_RATIO", 0.1)
# Minimum and maximum sleep interval between sending batches of events to the destination.
# These are used to adaptively pick an interval based on buffer fullness per consumer.
PUBLISHER_MIN_BUFFER_SEND_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_MIN_BUFFER_SEND_INTERVAL_SECONDS", 0.1
)
PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_MAX_BUFFER_SEND_INTERVAL_SECONDS", 1.0
)
