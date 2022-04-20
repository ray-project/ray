SCHEMA_VERSION = "0.1"

# The key to store / obtain cluster metadata.
CLUSTER_METADATA_KEY = b"CLUSTER_METADATA"

# The name of a json file where usage stats will be written.
USAGE_STATS_FILE = "usage_stats.json"

USAGE_STATS_ENABLED_MESSAGE = (
    "Usage stats collection is enabled. To disable this, add `--disable-usage-stats` to the command that starts the cluster, or run the following command:"
    " `ray disable-usage-stats`. "
    "See https://github.com/ray-project/ray/issues/20857 for more details.\n"
)

USAGE_STATS_ENABLED_BY_DEFAULT_MESSAGE = (
    "Usage stats collection is enabled by default without user confirmation since it is running non-interactively. To disable this, add `--disable-usage-stats` to the command that starts the cluster, or run the following command:"
    " `ray disable-usage-stats`. "
    "See https://github.com/ray-project/ray/issues/20857 for more details.\n"
)

USAGE_STATS_CONFIRMATION_MESSAGE = "Enable usage stats collection. See https://github.com/ray-project/ray/issues/20857 for more details. It will be enabled if there is no confirmation within 10s."
