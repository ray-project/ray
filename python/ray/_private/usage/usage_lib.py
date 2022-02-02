"""This is the module that is in charge of Ray usage report (telemetry) APIs.

NOTE: Ray's usage report is currently "off by default".
      But we are planning to make it opt-in by default.

Ray usage report follows the specification from
https://docs.google.com/document/d/1ZT-l9YbGHh-iWRUC91jS-ssQ5Qe2UQ43Lsoc1edCalc/edit#heading=h.17dss3b9evbj. # noqa

# Module

The module consists of 2 parts.

## Public API
It contains public APIs to obtain usage report information. 
APIs will be added in the sooner future before usage report becomes opt-in by default.

## Internal APIs for usage processing / report
Telemetry report consists of 5 components. This module is in charge of the top 2 layers.

Report                -> usage_lib
---------------------
Usage data processing -> usage_lib
---------------------
Data storage          -> Ray API server
---------------------
Aggregation           -> Ray API server (curerntly a dashboard server)
---------------------
Usage data collection -> Various components (Ray agent, GCS, etc.) + usage_lib (cluster metadata).

Usage report is currently "off by default". You can enable the report by setting an environment variable
RAY_USAGE_STATS_ENABLE=1. For example, `RAY_USAGE_STATS_ENABLE=1 ray start --head`.
Or `RAY_USAGE_STATS_ENABLE=1 python [drivers with ray.init()]`.

"Ray API server (currently a dashboard server)" reports the usage data to the hard-coded URL.
`ray._private.usage.usage_constants.USAGE_REPORT_SERVER_URL`.

Data is reported
- Every `ray._private.usage.usage_constants.USAGE_REPORT_INTERVAL`.
- When the API server terminates.

Note that it is also possible to configure the interval using the environment variable,
`RAY_USAGE_REPORT_INTERVAL`.

To see collected / reported data, see `ray._private.usage.schema.py`.
"""
import os
import uuid
import sys
import json
import logging

import ray
import requests

from pathlib import Path

import ray.ray_constants as ray_constants
import ray._private.usage.usage_constants as usage_constant

from ray._private.usage.schema import SCHEMA_VERSION, schema

logger = logging.getLogger(__name__)

#################
# Internal APIs #
#################


def _generate_cluster_metadata():
    """Return a dictionary of cluster metadata."""
    ray_version, python_version = ray._private.utils.compute_version_info()
    return {
        "schema_version": SCHEMA_VERSION,
        "source": os.getenv("RAY_USAGE_STATS_SOURCE", "OSS"),
        "session_id": str(uuid.uuid4()),
        "ray_version": ray_version,
        "git_commit": ray.__commit__,
        "python_version": python_version,
        "os": sys.platform,
    }


def put_cluster_metadata(gcs_client, num_retries) -> None:
    """Generate the cluster metadata and store it to GCS.

    It is a blocking API.

    Params:
        gcs_client (GCSClient): The GCS client to perform KV operation PUT.
        num_retries (int): Max number of times to retry if PUT fails.

    Raises:
        gRPC exceptions if PUT fails.
    """
    ray._private.utils.internal_kv_put_with_retry(
        gcs_client,
        usage_constant.CLUSTER_METADATA_KEY,
        json.dumps(_generate_cluster_metadata()).encode(),
        namespace=ray_constants.KV_NAMESPACE_CLUSTER,
        num_retries=num_retries,
    )


def get_cluster_metadata(gcs_client, num_retries) -> dict:
    """Get the cluster metadata from GCS.

    It is a blocking API.

    This will return None if `put_cluster_metadata` was never called.

    Params:
        gcs_client (GCSClient): The GCS client to perform KV operation GET.
        num_retries (int): Max number of times to retry if GET fails.

    Returns:
        The cluster metadata in a dictinoary.

    Raises:
        RuntimeError if it fails to obtain cluster metadata from GCS.
    """
    return json.loads(
        ray._private.utils.internal_kv_get_with_retry(
            gcs_client,
            usage_constant.CLUSTER_METADATA_KEY,
            namespace=ray_constants.KV_NAMESPACE_CLUSTER,
            num_retries=num_retries,
        )
    )


def validate_schema(payload: dict):
    """Perform the lightweight json schema validation.

    TODO(sang): Use protobuf for schema validation instead.

    Params:
        payload (dict): The payload to validate schema.

    Raises:
        ValueError if the schema validation fails.
    """
    for k, v in payload.items():
        if k not in schema:
            raise ValueError(
                f"Given payload doesn't contain a key {k} that's required "
                f"for the schema.\nschema: {schema}\npayload: {payload}"
            )
        if schema[k] != type(v):
            raise ValueError(
                f"The key {k}'s value {v} has an incorrect type. "
                f"key type: {type(k)}. schema type: {schema[k]}"
            )


def write_usage_data(data: dict, dir_path: str) -> None:
    """Write the usage data to the directory.

    Params:
        data (dict): Data to report
        dir_path (Path): The path to the directory to write usage data.

    Raises:

    """
    # Atomically update the file.
    dir_path = Path(dir_path)
    destination = dir_path / "usage_stats.json"
    logger.error("write data")
    logger.error(data)
    logger.error(destination)
    temp = dir_path / "usage_stats_tmp.json"
    with temp.open(mode="w") as json_file:
        json_file.write(json.dumps(data))
        temp.rename(destination)


def report_usage_data(data: dict) -> None:
    """Report the usage data to the hard-coded usage server.

    Params:
        data (dict): Data to report.

    Raises:
        requests.HTTPError if requests fails.
    """
    logger.error("send data")
    logger.error(data)
    r = requests.request(
        "POST",
        usage_constant.USAGE_REPORT_SERVER_URL,
        headers={"Content-Type": "application/json"},
        json=data,
        timeout=10,
    )
    r.raise_for_status()
    logger.error(f"Status code: {r.status_code}, body: {r.json()}")
