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

Data is reported every hour by default.

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
import yaml

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from typing import Optional, List
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
    cloud_provider: Optional[str]
    min_workers: Optional[int]
    max_workers: Optional[int]
    head_node_instance_type: Optional[str]
    worker_node_instance_types: Optional[List[str]]
    total_num_cpus: Optional[int]
    total_num_gpus: Optional[int]
    total_memory_gb: Optional[float]
    total_object_store_memory_gb: Optional[float]
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


def _usage_stats_prompt_enabled():
    return int(os.getenv("RAY_USAGE_STATS_PROMPT_ENABLED", "1")) == 1


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


def print_usage_stats_heads_up_message() -> None:
    try:
        if (not _usage_stats_prompt_enabled()) or _usage_stats_enabled():
            return

        print(usage_constant.USAGE_STATS_HEADS_UP_MESSAGE, file=sys.stderr)
    except Exception:
        # Silently ignore the exception since it doesn't affect the use of ray.
        pass


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


def get_cluster_status(gcs_client, num_retries) -> dict:
    """Get the current status of this cluster.

    It is a blocking API.

    Params:
        gcs_client (GCSClient): The GCS client to perform KV operation GET.
        num_retries (int): Max number of times to retry if GET fails.

    Returns:
        The current cluster status or empty dict if it fails to get that information.
    """
    try:
        cluster_status = ray._private.utils.internal_kv_get_with_retry(
            gcs_client,
            ray.ray_constants.DEBUG_AUTOSCALING_STATUS,
            namespace=None,
            num_retries=num_retries,
        )
        if not cluster_status:
            return {}

        result = {}
        to_GiB = 1 / 2 ** 30
        cluster_status = json.loads(cluster_status.decode("utf-8"))
        if (
            "load_metrics_report" not in cluster_status
            or "usage" not in cluster_status["load_metrics_report"]
        ):
            return {}

        usage = cluster_status["load_metrics_report"]["usage"]
        if "CPU" in usage:
            result["total_num_cpus"] = usage["CPU"][1]
        if "GPU" in usage:
            result["total_num_gpus"] = usage["GPU"][1]
        if "memory" in usage:
            result["total_memory_gb"] = usage["memory"][1] * to_GiB
        if "object_store_memory" in usage:
            result["total_object_store_memory_gb"] = (
                usage["object_store_memory"][1] * to_GiB
            )
        return result
    except Exception:
        return {}


def get_cluster_config(cluster_config_file_path) -> dict:
    """Get the static cluster (autoscaler) config used to launch this cluster.

    Params:
        cluster_config_file_path (str): The file path to the cluster config file.

    Returns:
        The cluster (autoscaler) config or empty if it fails to get that information.
    """

    def get_instance_type(node_config):
        if not node_config:
            return None
        if "InstanceType" in node_config:
            # aws
            return node_config["InstanceType"]
        if "machineType" in node_config:
            # gcp
            return node_config["machineType"]
        if (
            "azure_arm_parameters" in node_config
            and "vmSize" in node_config["azure_arm_parameters"]
        ):
            return node_config["azure_arm_parameters"]["vmSize"]
        return None

    try:
        with open(cluster_config_file_path) as f:
            config = yaml.safe_load(f)
            result = {}
            if "min_workers" in config:
                result["min_workers"] = config["min_workers"]
            if "max_workers" in config:
                result["max_workers"] = config["max_workers"]

            if "provider" in config and "type" in config["provider"]:
                result["cloud_provider"] = config["provider"]["type"]

            if "head_node_type" not in config:
                return result
            if "available_node_types" not in config:
                return result
            head_node_type = config["head_node_type"]
            available_node_types = config["available_node_types"]
            for available_node_type in available_node_types:
                if available_node_type == head_node_type:
                    head_node_instance_type = get_instance_type(
                        available_node_types[available_node_type].get("node_config")
                    )
                    if head_node_instance_type:
                        result["head_node_instance_type"] = head_node_instance_type
                else:
                    worker_node_instance_type = get_instance_type(
                        available_node_types[available_node_type].get("node_config")
                    )
                    if worker_node_instance_type:
                        result["worker_node_instance_types"] = result.get(
                            "worker_node_instance_types", set()
                        )
                        result["worker_node_instance_types"].add(
                            worker_node_instance_type
                        )
            if "worker_node_instance_types" in result:
                result["worker_node_instance_types"] = list(
                    result["worker_node_instance_types"]
                )
            return result
    except Exception:
        # If the file doesn't exist or the config file is invalid,
        # then it's not a ray cluster or it's a k8s cluster.
        return {}


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
    cluster_metadata: dict,
    cluster_config: dict,
    total_success: int,
    total_failed: int,
    seq_number: int,
) -> UsageStatsToReport:
    """Generate the report data.

    Params:
        cluster_metadata (dict): The cluster metadata of the system generated by
            `_generate_cluster_metadata`.
        cluster_config (dcit): The cluster (autoscaler) config generated by
            `get_cluster_config`.
        total_success(int): The total number of successful report
            for the lifetime of the cluster.
        total_failed(int): The total number of failed report
            for the lifetime of the cluster.
        seq_number(int): The sequence number that's incremented whenever
            a new report is sent.

    Returns:
        UsageStats
    """
    cluster_status = get_cluster_status(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(),
        num_retries=20,
    )
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
        cloud_provider=cluster_config.get("cloud_provider"),
        min_workers=cluster_config.get("min_workers"),
        max_workers=cluster_config.get("max_workers"),
        head_node_instance_type=cluster_config.get("head_node_instance_type"),
        worker_node_instance_types=cluster_config.get("worker_node_instance_types"),
        total_num_cpus=cluster_status.get("total_num_cpus"),
        total_num_gpus=cluster_status.get("total_num_gpus"),
        total_memory_gb=cluster_status.get("total_memory_gb"),
        total_object_store_memory_gb=cluster_status.get("total_object_store_memory_gb"),
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
