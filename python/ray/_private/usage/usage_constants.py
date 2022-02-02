import os

# The key to store / obtain cluster metadata.
CLUSTER_METADATA_KEY = b"CLUSTER_METADATA"

# The usage collection server URL.
USAGE_REPORT_SERVER_URL = "https://ebezk70x0j.execute-api.us-west-2.amazonaws.com"

# How often the data is supposed to be reported to the server.
USAGE_REPORT_INTERVAL = int(os.getenv("RAY_USAGE_REPORT_INTERVAL", 10))
