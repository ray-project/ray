"""This is the module that is in charge of Ray usage report (telemetry) APIs.

NOTE: Ray's usage report is currently "off by default".
      But we are planning to make it opt-in by default.

Ray usage report follows the specification from
https://docs.google.com/document/d/1ZT-l9YbGHh-iWRUC91jS-ssQ5Qe2UQ43Lsoc1edCalc/edit#heading=h.17dss3b9evbj. # noqa

# Module

The module consists of 2 parts.

## Public API
It contains public APIs to obtain usage report information. 
APIs will be added before the usage report becomes opt-in by default.

## Internal APIs for usage processing/report
The telemetry report consists of 5 components. This module is in charge of the top 2 layers.

Report                -> usage_lib
---------------------
Usage data processing -> usage_lib
---------------------
Data storage          -> Ray API server
---------------------
Aggregation           -> Ray API server (currently a dashboard server)
---------------------
Usage data collection -> Various components (Ray agent, GCS, etc.) + usage_lib (cluster metadata).

Usage report is currently "off by default". You can enable the report by setting an environment variable
RAY_USAGE_STATS_ENABLED=1. For example, `RAY_USAGE_STATS_ENABLED=1 ray start --head`.
Or `RAY_USAGE_STATS_ENABLED=1 python [drivers with ray.init()]`.

"Ray API server (currently a dashboard server)" reports the usage data to https://usage-stats.ray.io/.

Data is reported ever hour by default.

Note that it is also possible to configure the interval using the environment variable,
`RAY_USAGE_STATS_REPORT_INTERVAL_S`.

To see collected/reported data, see `usage_stats.json` inside a temp
folder (e.g., /tmp/ray/session_[id]/*).
"""
import asyncio
import os
import uuid
import sys
import json
import logging
import time

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from pathlib import Path

import ray
import requests

import ray.ray_constants as ray_constants
import ray._private.usage.usage_constants as usage_constant

logger = logging.getLogger(__name__)

#################
# Internal APIs #
#################


@dataclass(init=True)
class UsageStatsToReport:
    """Usage stats to report"""

    ray_version: str
    python_version: str
    schema_version: str
    source: str
    session_id: str
    git_commit: str
    os: str
    collect_timestamp_ms: int
    session_start_timestamp_ms: int
    # The total number of successful reports for the lifetime of the cluster.
    total_success: int
    # The total number of failed reports for the lifetime of the cluster.
    total_failed: int
    # The sequence number of the report.
    seq_number: int


@dataclass(init=True)
class UsageStatsToWrite:
    """Usage stats to write to `USAGE_STATS_FILE`

    We are writing extra metadata such as the status of report
    to this file.
    """

    usage_stats: UsageStatsToReport
    # Whether or not the last report succeeded.
    success: bool
    # The error message of the last report if it happens.
    error: str


def _usage_stats_report_url():
    # The usage collection server URL.
    # The environment variable is testing-purpose only.
    return os.getenv("RAY_USAGE_STATS_REPORT_URL", "https://usage-stats.ray.io/")


def _usage_stats_report_interval_s():
    return int(os.getenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", 3600))


def _usage_stats_enabled():
    """
    NOTE: This is the private API, and it is not a reliable
    way to know if usage stats are enabled from the cluster.
    """
    return int(os.getenv("RAY_USAGE_STATS_ENABLED", "0")) == 1


def _generate_cluster_metadata():
    """Return a dictionary of cluster metadata."""
    ray_version, python_version = ray._private.utils.compute_version_info()
    # These two metadata is necessary although usage report is not enabled
    # to check version compatibility.
    metadata = {
        "ray_version": ray_version,
        "python_version": python_version,
    }
    # Additional metadata is recorded only when usage stats are enabled.
    if _usage_stats_enabled():
        metadata.update(
            {
                "schema_version": usage_constant.SCHEMA_VERSION,
                "source": os.getenv("RAY_USAGE_STATS_SOURCE", "OSS"),
                "session_id": str(uuid.uuid4()),
                "git_commit": ray.__commit__,
                "os": sys.platform,
                "session_start_timestamp_ms": int(time.time() * 1000),
            }
        )
    return metadata


def put_cluster_metadata(gcs_client, num_retries) -> None:
    """Generate the cluster metadata and store it to GCS.

    It is a blocking API.

    Params:
        gcs_client (GCSClient): The GCS client to perform KV operation PUT.
        num_retries (int): Max number of times to retry if PUT fails.

    Raises:
        gRPC exceptions if PUT fails.
    """
    metadata = _generate_cluster_metadata()
    ray._private.utils.internal_kv_put_with_retry(
        gcs_client,
        usage_constant.CLUSTER_METADATA_KEY,
        json.dumps(metadata).encode(),
        namespace=ray_constants.KV_NAMESPACE_CLUSTER,
        num_retries=num_retries,
    )
    return metadata


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


def generate_report_data(
    cluster_metadata: dict, total_success: int, total_failed: int, seq_number: int
) -> UsageStatsToReport:
    """Generate the report data.

    Params:
        cluster_metadata (dict): The cluster metadata of the system generated by
            `_generate_cluster_metadata`.
        total_success(int): The total number of successful report
            for the lifetime of the cluster.
        total_failed(int): The total number of failed report
            for the lifetime of the cluster.
        seq_number(int): The sequence number that's incremented whenever
            a new report is sent.

    Returns:
        UsageStats
    """
    data = UsageStatsToReport(
        ray_version=cluster_metadata["ray_version"],
        python_version=cluster_metadata["python_version"],
        schema_version=cluster_metadata["schema_version"],
        source=cluster_metadata["source"],
        session_id=cluster_metadata["session_id"],
        git_commit=cluster_metadata["git_commit"],
        os=cluster_metadata["os"],
        collect_timestamp_ms=int(time.time() * 1000),
        session_start_timestamp_ms=cluster_metadata["session_start_timestamp_ms"],
        total_success=total_success,
        total_failed=total_failed,
        seq_number=seq_number,
    )
    return data


def generate_write_data(
    usage_stats: UsageStatsToReport,
    error: str,
) -> UsageStatsToWrite:
    """Generate the report data.

    Params:
        usage_stats (UsageStatsToReport): The usage stats that were reported.
        error(str): The error message of failed reports.

    Returns:
        UsageStatsToWrite
    """
    data = UsageStatsToWrite(
        usage_stats=usage_stats,
        success=error is None,
        error=error,
    )
    return data


class UsageReportClient:
    """The client implementation for usage report.

    It is in charge of writing usage stats to the directory
    and report usage stats.
    """

    def _write_usage_data(self, data: UsageStatsToWrite, dir_path: str) -> None:
        """Write the usage data to the directory.

        Params:
            data (dict): Data to report
            dir_path (Path): The path to the directory to write usage data.
        """
        if not _usage_stats_enabled():
            return

        # Atomically update the file.
        dir_path = Path(dir_path)
        destination = dir_path / usage_constant.USAGE_STATS_FILE
        temp = dir_path / f"{usage_constant.USAGE_STATS_FILE}.tmp"
        with temp.open(mode="w") as json_file:
            json_file.write(json.dumps(asdict(data)))
        if sys.platform == "win32":
            # Windows 32 doesn't support atomic renaming, so we should delete
            # the file first.
            destination.unlink(missing_ok=True)
        temp.rename(destination)

    def _report_usage_data(self, url: str, data: UsageStatsToReport) -> None:
        """Report the usage data to the usage server.

        Params:
            url (str): The URL to update resource usage.
            data (dict): Data to report.

        Raises:
            requests.HTTPError if requests fails.
        """
        if not _usage_stats_enabled():
            return

        r = requests.request(
            "POST",
            url,
            headers={"Content-Type": "application/json"},
            json=asdict(data),
            timeout=10,
        )
        r.raise_for_status()
        return r

    async def write_usage_data_async(
        self, data: UsageStatsToWrite, dir_path: str
    ) -> None:
        """Asynchronously write the data to the `dir_path`.

        It uses a thread pool to implement asynchronous write.
        https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
        """
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, self._write_usage_data, data, dir_path)

    async def report_usage_data_async(self, url: str, data: UsageStatsToReport) -> None:
        """Asynchronously report the data to the `url`.

        It uses a thread pool to implement asynchronous write
        instead of using dedicated library such as httpx
        since that's too heavy dependency.
        """
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, self._report_usage_data, url, data)
