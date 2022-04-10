SCHEMA_VERSION = "0.1"

# The key to store / obtain cluster metadata.
CLUSTER_METADATA_KEY = b"CLUSTER_METADATA"

# The name of a json file where usage stats will be written.
USAGE_STATS_FILE = "usage_stats.json"

USAGE_STATS_PROMPT_MESSAGE = (
    'Usage stats collection is enabled. To disable this, add {"usage_stats": false} to ~/.ray/config.json, or run the following command:\n\n'
    "    ray disable-usage-stats\n\n"
    "See https://github.com/ray-project/ray/issues/20857 for more details.\n"
)
