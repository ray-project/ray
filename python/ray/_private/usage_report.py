"""This is the module that is in charge of Ray usage report APIs.

When the module is used within Ray repository, it must be imported as `ray_usage_lib`.

Ray usage report follows the specification from
https://docs.google.com/document/d/1ZT-l9YbGHh-iWRUC91jS-ssQ5Qe2UQ43Lsoc1edCalc/edit#heading=h.17dss3b9evbj. # noqa

Also, please refer to the official documentation regarding usage collection
<link>.

To see what usage data is collected and reported by Ray, please see <link>.

Usage report is currently "off by default". You can enable it by setting an environment variable
RAY_USAGE_STATS_ENABLE=1. If you'd like to permanantly disable usage report, please `TODO(sang): Add contents`

Usage report is performed by "ray API server". Ray API server reports the usage to the URL <link>
every configurable internval (1 hour by default). You can also configure the interval by tweaking
the environment variable `TODO(sang): Add an env var`.

Usage collection is performend by multiple components. Fore example, the API server can
query GCS or Ray agents per node to aggregate the usage data from the cluster. Please
check <link> to see how the aggregation is currently performed.
"""
import os
import uuid
import sys

import ray


def usage_report_enabled():
    """Return True if the usage report is enabled.
    """
    pass


def usage_report_status():
    """Return the usage report status.
    TODO(sang): Define schema and update docstring.
    """
    pass


#################
# Internal APIs #
#################
# Schema version of the usage stats.
# {
#   // version of the schema
#   “_version”: string,
#   // oss or product
#   “_source”: string,
#   “session_id”: string,
#   “timestamp_ms”: long
#   “ray_version”: string,
#   “git_commit”: string,
#   “os”: string,
#   “python_version”: string,
# }
_SCHEMA_VERSION = "0.1"
CLUSTER_METADATA_KEY = b"CLUSTER_METADATA"


def get_cluster_metadata():
    """Return a dictionary of cluster metadata that will be reported.
    """
    return {
        "_version": _SCHEMA_VERSION,
        "_source": os.getenv("RAY_USAGE_STATS_SOURCE", "OSS"),
        "session_id": str(uuid.uuid4()),
        "ray_version": ray.__version__,
        "git_commit": ray.__commit__,
        "python_version": ".".join(map(str, sys.version_info[:3])),
        "os": sys.platform,
    }
