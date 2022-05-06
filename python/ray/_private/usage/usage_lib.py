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
import os
import uuid
import sys
import json
import logging
import time
import yaml

from dataclasses import dataclass, asdict
from typing import Optional, List
from pathlib import Path
from enum import Enum, auto

import ray
import requests

import ray.ray_constants as ray_constants
import ray._private.usage.usage_constants as usage_constant
from ray.experimental.internal_kv import (
    _internal_kv_put,
    _internal_kv_initialized,
)

logger = logging.getLogger(__name__)

#################
# Internal APIs #
#################


@dataclass(init=True)
class ClusterConfigToReport:
    cloud_provider: Optional[str] = None
    min_workers: Optional[int] = None
    max_workers: Optional[int] = None
    head_node_instance_type: Optional[str] = None
    worker_node_instance_types: Optional[List[str]] = None


@dataclass(init=True)
class ClusterStatusToReport:
    total_num_cpus: Optional[int] = None
    total_num_gpus: Optional[int] = None
    total_memory_gb: Optional[float] = None
    total_object_store_memory_gb: Optional[float] = None


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
    library_usages: Optional[List[str]]
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


class UsageStatsEnabledness(Enum):
    ENABLED_EXPLICITLY = auto()
    DISABLED_EXPLICITLY = auto()
    ENABLED_BY_DEFAULT = auto()


_recorded_library_usages = set()


def _put_library_usage(library_usage: str):
    assert _internal_kv_initialized()
    try:
        _internal_kv_put(
            f"{usage_constant.LIBRARY_USAGE_PREFIX}{library_usage}",
            "",
            namespace=usage_constant.USAGE_STATS_NAMESPACE,
        )
    except Exception as e:
        logger.debug(f"Failed to put library usage, {e}")


def record_library_usage(library_usage: str):
    """Record library usage (e.g. which library is used)"""
    if library_usage in _recorded_library_usages:
        return
    _recorded_library_usages.add(library_usage)

    if not _internal_kv_initialized():
        # This happens if the library is imported before ray.init
        return

    # Only report library usage from driver to reduce
    # the load to kv store.
    if ray.worker.global_worker.mode == ray.SCRIPT_MODE:
        _put_library_usage(library_usage)


def _put_pre_init_library_usages():
    assert _internal_kv_initialized()
    if ray.worker.global_worker.mode != ray.SCRIPT_MODE:
        return
    for library_usage in _recorded_library_usages:
        _put_library_usage(library_usage)


ray.worker._post_init_hooks.append(_put_pre_init_library_usages)


def _usage_stats_report_url():
    # The usage collection server URL.
    # The environment variable is testing-purpose only.
    return os.getenv("RAY_USAGE_STATS_REPORT_URL", "https://usage-stats.ray.io/")


def _usage_stats_report_interval_s():
    return int(os.getenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", 3600))


def _usage_stats_config_path():
    return os.getenv(
        "RAY_USAGE_STATS_CONFIG_PATH", os.path.expanduser("~/.ray/config.json")
    )


def _usage_stats_enabledness() -> UsageStatsEnabledness:
    # Env var has higher priority than config file.
    usage_stats_enabled_env_var = os.getenv(usage_constant.USAGE_STATS_ENABLED_ENV_VAR)
    if usage_stats_enabled_env_var == "0":
        return UsageStatsEnabledness.DISABLED_EXPLICITLY
    elif usage_stats_enabled_env_var == "1":
        return UsageStatsEnabledness.ENABLED_EXPLICITLY
    elif usage_stats_enabled_env_var is not None:
        raise ValueError(
            f"Valid value for {usage_constant.USAGE_STATS_ENABLED_ENV_VAR} "
            f"env var is 0 or 1, but got {usage_stats_enabled_env_var}"
        )

    usage_stats_enabled_config_var = None
    try:
        with open(_usage_stats_config_path()) as f:
            config = json.load(f)
            usage_stats_enabled_config_var = config.get("usage_stats")
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug(f"Failed to load usage stats config {e}")

    if usage_stats_enabled_config_var is False:
        return UsageStatsEnabledness.DISABLED_EXPLICITLY
    elif usage_stats_enabled_config_var is True:
        return UsageStatsEnabledness.ENABLED_EXPLICITLY
    elif usage_stats_enabled_config_var is not None:
        raise ValueError(
            f"Valid value for 'usage_stats' in {_usage_stats_config_path()}"
            f" is true or false, but got {usage_stats_enabled_config_var}"
        )

    # Usage stats is enabled by default.
    return UsageStatsEnabledness.ENABLED_BY_DEFAULT


def usage_stats_enabled() -> bool:
    return _usage_stats_enabledness() is not UsageStatsEnabledness.DISABLED_EXPLICITLY


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
    if usage_stats_enabled():
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


def show_usage_stats_prompt() -> None:
    if not _usage_stats_prompt_enabled():
        return

    from ray.autoscaler._private.cli_logger import cli_logger

    usage_stats_enabledness = _usage_stats_enabledness()
    if usage_stats_enabledness is UsageStatsEnabledness.DISABLED_EXPLICITLY:
        cli_logger.print(usage_constant.USAGE_STATS_DISABLED_MESSAGE)
    elif usage_stats_enabledness is UsageStatsEnabledness.ENABLED_BY_DEFAULT:

        if cli_logger.interactive:
            enabled = cli_logger.confirm(
                False,
                usage_constant.USAGE_STATS_CONFIRMATION_MESSAGE,
                _default=True,
                _timeout_s=10,
            )
            set_usage_stats_enabled_via_env_var(enabled)
            # Remember user's choice.
            try:
                set_usage_stats_enabled_via_config(enabled)
            except Exception as e:
                logger.debug(
                    f"Failed to persist usage stats choice for future clusters: {e}"
                )
            if enabled:
                cli_logger.print(usage_constant.USAGE_STATS_ENABLED_MESSAGE)
            else:
                cli_logger.print(usage_constant.USAGE_STATS_DISABLED_MESSAGE)
        else:
            cli_logger.print(
                usage_constant.USAGE_STATS_ENABLED_BY_DEFAULT_MESSAGE,
            )
    else:
        assert usage_stats_enabledness is UsageStatsEnabledness.ENABLED_EXPLICITLY
        cli_logger.print(usage_constant.USAGE_STATS_ENABLED_MESSAGE)


def set_usage_stats_enabled_via_config(enabled) -> None:
    config = {}
    try:
        with open(_usage_stats_config_path()) as f:
            config = json.load(f)
        if not isinstance(config, dict):
            logger.debug(
                f"Invalid ray config file, should be a json dict but got {type(config)}"
            )
            config = {}
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug(f"Failed to load ray config file {e}")

    config["usage_stats"] = enabled

    try:
        os.makedirs(os.path.dirname(_usage_stats_config_path()), exist_ok=True)
        with open(_usage_stats_config_path(), "w") as f:
            json.dump(config, f)
    except Exception as e:
        raise Exception(
            "Failed to "
            f'{"enable" if enabled else "disable"}'
            ' usage stats by writing {"usage_stats": '
            f'{"true" if enabled else "false"}'
            "} to "
            f"{_usage_stats_config_path()}"
        ) from e


def set_usage_stats_enabled_via_env_var(enabled) -> None:
    os.environ[usage_constant.USAGE_STATS_ENABLED_ENV_VAR] = "1" if enabled else "0"


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


def get_library_usages_to_report(gcs_client, num_retries) -> List[str]:
    try:
        result = []
        library_usages = ray._private.utils.internal_kv_list_with_retry(
            gcs_client,
            usage_constant.LIBRARY_USAGE_PREFIX,
            namespace=usage_constant.USAGE_STATS_NAMESPACE,
            num_retries=num_retries,
        )
        for library_usage in library_usages:
            library_usage = library_usage.decode("utf-8")
            result.append(library_usage[len(usage_constant.LIBRARY_USAGE_PREFIX) :])
        return result
    except Exception as e:
        logger.info(f"Failed to get library usages to report {e}")
        return []


def get_cluster_status_to_report(gcs_client, num_retries) -> ClusterStatusToReport:
    """Get the current status of this cluster.

    It is a blocking API.

    Params:
        gcs_client (GCSClient): The GCS client to perform KV operation GET.
        num_retries (int): Max number of times to retry if GET fails.

    Returns:
        The current cluster status or empty if it fails to get that information.
    """
    try:
        cluster_status = ray._private.utils.internal_kv_get_with_retry(
            gcs_client,
            ray.ray_constants.DEBUG_AUTOSCALING_STATUS,
            namespace=None,
            num_retries=num_retries,
        )
        if not cluster_status:
            return ClusterStatusToReport()

        result = ClusterStatusToReport()
        to_GiB = 1 / 2 ** 30
        cluster_status = json.loads(cluster_status.decode("utf-8"))
        if (
            "load_metrics_report" not in cluster_status
            or "usage" not in cluster_status["load_metrics_report"]
        ):
            return ClusterStatusToReport()

        usage = cluster_status["load_metrics_report"]["usage"]
        # usage is a map from resource to (used, total) pair
        if "CPU" in usage:
            result.total_num_cpus = int(usage["CPU"][1])
        if "GPU" in usage:
            result.total_num_gpus = int(usage["GPU"][1])
        if "memory" in usage:
            result.total_memory_gb = usage["memory"][1] * to_GiB
        if "object_store_memory" in usage:
            result.total_object_store_memory_gb = (
                usage["object_store_memory"][1] * to_GiB
            )
        return result
    except Exception as e:
        logger.info(f"Failed to get cluster status to report {e}")
        return ClusterStatusToReport()


def get_cluster_config_to_report(cluster_config_file_path) -> ClusterConfigToReport:
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
            result = ClusterConfigToReport()
            if "min_workers" in config:
                result.min_workers = config["min_workers"]
            if "max_workers" in config:
                result.max_workers = config["max_workers"]

            if "provider" in config and "type" in config["provider"]:
                result.cloud_provider = config["provider"]["type"]

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
                        result.head_node_instance_type = head_node_instance_type
                else:
                    worker_node_instance_type = get_instance_type(
                        available_node_types[available_node_type].get("node_config")
                    )
                    if worker_node_instance_type:
                        result.worker_node_instance_types = (
                            result.worker_node_instance_types or set()
                        )
                        result.worker_node_instance_types.add(worker_node_instance_type)
            if result.worker_node_instance_types:
                result.worker_node_instance_types = list(
                    result.worker_node_instance_types
                )
            return result
    except FileNotFoundError:
        # It's a manually started cluster or k8s cluster
        result = ClusterConfigToReport()
        if "KUBERNETES_SERVICE_HOST" in os.environ:
            result.cloud_provider = "kubernetes"
        return result
    except Exception as e:
        logger.info(f"Failed to get cluster config to report {e}")
        return ClusterConfigToReport()


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
    cluster_config_to_report: ClusterConfigToReport,
    total_success: int,
    total_failed: int,
    seq_number: int,
) -> UsageStatsToReport:
    """Generate the report data.

    Params:
        cluster_metadata (dict): The cluster metadata of the system generated by
            `_generate_cluster_metadata`.
        cluster_config_to_report (ClusterConfigToReport): The cluster (autoscaler)
            config generated by `get_cluster_config_to_report`.
        total_success(int): The total number of successful report
            for the lifetime of the cluster.
        total_failed(int): The total number of failed report
            for the lifetime of the cluster.
        seq_number(int): The sequence number that's incremented whenever
            a new report is sent.

    Returns:
        UsageStats
    """
    cluster_status_to_report = get_cluster_status_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(),
        num_retries=20,
    )
    library_usages = get_library_usages_to_report(
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
        cloud_provider=cluster_config_to_report.cloud_provider,
        min_workers=cluster_config_to_report.min_workers,
        max_workers=cluster_config_to_report.max_workers,
        head_node_instance_type=cluster_config_to_report.head_node_instance_type,
        worker_node_instance_types=cluster_config_to_report.worker_node_instance_types,
        total_num_cpus=cluster_status_to_report.total_num_cpus,
        total_num_gpus=cluster_status_to_report.total_num_gpus,
        total_memory_gb=cluster_status_to_report.total_memory_gb,
        total_object_store_memory_gb=cluster_status_to_report.total_object_store_memory_gb,  # noqa: E501
        library_usages=library_usages,
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

    def write_usage_data(self, data: UsageStatsToWrite, dir_path: str) -> None:
        """Write the usage data to the directory.

        Params:
            data (dict): Data to report
            dir_path (Path): The path to the directory to write usage data.
        """
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

    def report_usage_data(self, url: str, data: UsageStatsToReport) -> None:
        """Report the usage data to the usage server.

        Params:
            url (str): The URL to update resource usage.
            data (dict): Data to report.

        Raises:
            requests.HTTPError if requests fails.
        """
        r = requests.request(
            "POST",
            url,
            headers={"Content-Type": "application/json"},
            json=asdict(data),
            timeout=10,
        )
        r.raise_for_status()
        return r
