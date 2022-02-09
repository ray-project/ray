import os

SCHEMA_VERSION = "0.1"

# The key to store / obtain cluster metadata.
CLUSTER_METADATA_KEY = b"CLUSTER_METADATA"

# The usage collection server URL.
# The environment variable is testing-purpose only.
USAGE_REPORT_URL = os.getenv(
    "RAY_USAGE_REPORT_URL", "https://usage-stats.ray.io/"
)  # noqa

# How often the data is supposed to be reported to the server.
USAGE_REPORT_INTERVAL_S = int(os.getenv("RAY_USAGE_REPORT_INTERVAL_S", 3600))

# Defines whether or not usage data is reported.
USAGE_REPORT_ENABLED = int(os.getenv("RAY_USAGE_STATS_ENABLE", "0")) == 1

USAGE_STATS_FILE = "usage_stats.json"
