"""This is the module that is in charge of Ray usage report (telemetry) APIs.

NOTE: Ray's usage report is currently "on by default".
      One could opt-out, see details at https://docs.ray.io/en/master/cluster/usage-stats.html. # noqa

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
import glob
import json
import logging
import threading
import os
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml

import ray
import ray._private.ray_constants as ray_constants
import ray._private.usage.usage_constants as usage_constant
from ray._private import gcs_utils
from ray.experimental.internal_kv import _internal_kv_initialized, _internal_kv_put
from ray.core.generated.usage_pb2 import TagKey as TagKey

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

    #: The Ray version in use.
    ray_version: str
    #: The Python version in use.
    python_version: str
    #: The schema version of the report.
    schema_version: str
    #: The source of the data (i.e. OSS).
    source: str
    #: A random id of the cluster session.
    session_id: str
    #: The git commit hash of Ray (i.e. ray.__commit__).
    git_commit: str
    #: The operating system in use.
    os: str
    #: When the data is collected and reported.
    collect_timestamp_ms: int
    #: When the cluster is started.
    session_start_timestamp_ms: int
    #: The cloud provider found in the cluster.yaml file (e.g., aws).
    cloud_provider: Optional[str]
    #: The min_workers found in the cluster.yaml file.
    min_workers: Optional[int]
    #: The max_workers found in the cluster.yaml file.
    max_workers: Optional[int]
    #: The head node instance type found in the cluster.yaml file (e.g., i3.8xlarge).
    head_node_instance_type: Optional[str]
    #: The worker node instance types found in the cluster.yaml file (e.g., i3.8xlarge).
    worker_node_instance_types: Optional[List[str]]
    #: The total num of cpus in the cluster.
    total_num_cpus: Optional[int]
    #: The total num of gpus in the cluster.
    total_num_gpus: Optional[int]
    #: The total size of memory in the cluster.
    total_memory_gb: Optional[float]
    #: The total size of object store memory in the cluster.
    total_object_store_memory_gb: Optional[float]
    #: The Ray libraries that are used (e.g., rllib).
    library_usages: Optional[List[str]]
    #: The total number of successful reports for the lifetime of the cluster.
    total_success: int
    #: The total number of failed reports for the lifetime of the cluster.
    total_failed: int
    #: The sequence number of the report.
    seq_number: int
    #: The extra tags to report when specified by an
    #  environment variable RAY_USAGE_STATS_EXTRA_TAGS
    extra_usage_tags: Optional[Dict[str, str]]
    #: The number of alive nodes when the report is generated.
    total_num_nodes: Optional[int]
    #: The total number of running jobs excluding internal ones
    #  when the report is generated.
    total_num_running_jobs: Optional[int]


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
_recorded_library_usages_lock = threading.Lock()
_recorded_extra_usage_tags = dict()
_recorded_extra_usage_tags_lock = threading.Lock()


# NOTE: Do not change the write / read protocol. That will cause
# version incompatibility issues.
class LibUsageRecorder:
    """A class to put/get the library usage to the ray tmp folder.

    See https://github.com/ray-project/ray/pull/25842 for more details.
    """

    def __init__(self, temp_dir_path: str):
        self._lib_usage_dir = Path(temp_dir_path)
        self._lib_usage_prefix = "_ray_lib_usage-"
        self._lib_usage_filename_match = re.compile(
            f"{self._lib_usage_prefix}([0-9a-zA-Z_.]+).txt"
        )

    def put_lib_usage(self, lib_name: str):
        """Put the library usage to the ray tmp folder."""
        lib_usage_file = self._lib_usage_dir / self._lib_usage_filename(lib_name)
        lib_usage_file.touch(exist_ok=True)

    def read_lib_usages(self) -> List[str]:
        """Read a list of library usages from the ray tmp folder."""
        # For checking if the file exists, it is okay to have a minor chance of
        # having race condition.
        lib_usages = []
        file_paths = glob.glob(f"{self._lib_usage_dir}/{self._lib_usage_prefix}*")
        for file_path in file_paths:
            file_path = Path(file_path)
            if file_path.exists():
                lib_usages.append(self._get_lib_usage_from_filename(file_path.name))
        return lib_usages

    def delete_lib_usages(self):
        """Delete all usage files. Test only"""
        file_paths = glob.glob(f"{self._lib_usage_dir}/{self._lib_usage_prefix}*")
        for file_path in file_paths:
            file_path = Path(file_path)
            file_path.unlink()

    def _lib_usage_filename(self, lib_name: str) -> str:
        return f"{self._lib_usage_prefix}{lib_name}.txt"

    def _get_lib_usage_from_filename(self, filename: str) -> str:
        return self._lib_usage_filename_match.match(filename).group(1)


def _put_library_usage(library_usage: str):
    assert _internal_kv_initialized()
    try:
        _internal_kv_put(
            f"{usage_constant.LIBRARY_USAGE_PREFIX}{library_usage}".encode(),
            b"",
            namespace=usage_constant.USAGE_STATS_NAMESPACE.encode(),
        )
    except Exception as e:
        logger.debug(f"Failed to put library usage, {e}")

    # Record the library usage to the temp (e.g., /tmp/ray) folder.
    # Note that although we always write this file, it is not
    # reported when the usage stats is disabled.
    if ray._private.worker.global_worker.mode == ray.SCRIPT_MODE:
        try:
            lib_usage_recorder = LibUsageRecorder(ray._private.utils.get_ray_temp_dir())
            lib_usage_recorder.put_lib_usage(library_usage)
        except Exception as e:
            logger.debug(f"Failed to write a library usage to the home folder, {e}")


def record_extra_usage_tag(key: TagKey, value: str):
    """Record extra kv usage tag.

    If the key already exists, the value will be overwritten.

    To record an extra tag, first add the key to the TagKey enum and
    then call this function.
    It will make a synchronous call to the internal kv store if the tag is updated.
    """
    key = TagKey.Name(key).lower()
    with _recorded_extra_usage_tags_lock:
        if _recorded_extra_usage_tags.get(key) == value:
            return
        _recorded_extra_usage_tags[key] = value

    if not _internal_kv_initialized():
        # This happens if the record is before ray.init
        return

    _put_extra_usage_tag(key, value)


def _put_extra_usage_tag(key: str, value: str):
    assert _internal_kv_initialized()
    try:
        _internal_kv_put(
            f"{usage_constant.EXTRA_USAGE_TAG_PREFIX}{key}".encode(),
            value.encode(),
            namespace=usage_constant.USAGE_STATS_NAMESPACE.encode(),
        )
    except Exception as e:
        logger.debug(f"Failed to put extra usage tag, {e}")


def record_library_usage(library_usage: str):
    """Record library usage (e.g. which library is used)"""
    if "-" in library_usage:
        # - is not permitted since it should be used as a separator
        # of the lib usage file name. See LibUsageRecorder.
        raise ValueError("The library name contains a char - which is not permitted.")

    with _recorded_library_usages_lock:
        if library_usage in _recorded_library_usages:
            return
        _recorded_library_usages.add(library_usage)

    if not _internal_kv_initialized():
        # This happens if the library is imported before ray.init
        return

    # Only report lib usage for driver / ray client / workers. Otherwise,
    # it can be reported if the library is imported from
    # e.g., API server.
    if (
        ray._private.worker.global_worker.mode == ray.SCRIPT_MODE
        or ray._private.worker.global_worker.mode == ray.WORKER_MODE
        or ray.util.client.ray.is_connected()
    ):
        _put_library_usage(library_usage)


def _put_pre_init_library_usages():
    assert _internal_kv_initialized()
    # NOTE: When the lib is imported from a worker, ray should
    # always be initialized, so there's no need to register the
    # pre init hook.
    if not (
        ray._private.worker.global_worker.mode == ray.SCRIPT_MODE
        or ray.util.client.ray.is_connected()
    ):
        return

    for library_usage in _recorded_library_usages:
        _put_library_usage(library_usage)


def _put_pre_init_extra_usage_tags():
    assert _internal_kv_initialized()
    for k, v in _recorded_extra_usage_tags.items():
        _put_extra_usage_tag(k, v)


def put_pre_init_usage_stats():
    _put_pre_init_library_usages()
    _put_pre_init_extra_usage_tags()


ray._private.worker._post_init_hooks.append(put_pre_init_usage_stats)


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


def is_nightly_wheel() -> bool:
    return ray.__commit__ != "{{RAY_COMMIT_SHA}}" and "dev" in ray.__version__


def usage_stats_enabled() -> bool:
    return _usage_stats_enabledness() is not UsageStatsEnabledness.DISABLED_EXPLICITLY


def usage_stats_prompt_enabled():
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


def show_usage_stats_prompt(cli: bool) -> None:
    if not usage_stats_prompt_enabled():
        return

    from ray.autoscaler._private.cli_logger import cli_logger

    prompt_print = cli_logger.print if cli else print

    usage_stats_enabledness = _usage_stats_enabledness()
    if usage_stats_enabledness is UsageStatsEnabledness.DISABLED_EXPLICITLY:
        prompt_print(usage_constant.USAGE_STATS_DISABLED_MESSAGE)
    elif usage_stats_enabledness is UsageStatsEnabledness.ENABLED_BY_DEFAULT:
        if not cli:
            prompt_print(
                usage_constant.USAGE_STATS_ENABLED_BY_DEFAULT_FOR_RAY_INIT_MESSAGE
            )
        elif cli_logger.interactive:
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
                prompt_print(usage_constant.USAGE_STATS_ENABLED_FOR_CLI_MESSAGE)
            else:
                prompt_print(usage_constant.USAGE_STATS_DISABLED_MESSAGE)
        else:
            prompt_print(
                usage_constant.USAGE_STATS_ENABLED_BY_DEFAULT_FOR_CLI_MESSAGE,
            )
    else:
        assert usage_stats_enabledness is UsageStatsEnabledness.ENABLED_EXPLICITLY
        prompt_print(
            usage_constant.USAGE_STATS_ENABLED_FOR_CLI_MESSAGE
            if cli
            else usage_constant.USAGE_STATS_ENABLED_FOR_RAY_INIT_MESSAGE
        )


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


def put_cluster_metadata(gcs_client) -> None:
    """Generate the cluster metadata and store it to GCS.

    It is a blocking API.

    Params:
        gcs_client: The GCS client to perform KV operation PUT.

    Raises:
        gRPC exceptions if PUT fails.
    """
    metadata = _generate_cluster_metadata()
    gcs_client.internal_kv_put(
        usage_constant.CLUSTER_METADATA_KEY,
        json.dumps(metadata).encode(),
        overwrite=True,
        namespace=ray_constants.KV_NAMESPACE_CLUSTER,
    )
    return metadata


def get_total_num_running_jobs_to_report(gcs_client) -> Optional[int]:
    """Return the total number of running jobs in the cluster excluding internal ones"""
    try:
        result = gcs_client.get_all_job_info()
        total_num_running_jobs = 0
        for job in result.job_info_list:
            if not job.is_dead and not job.config.ray_namespace.startswith(
                "_ray_internal"
            ):
                total_num_running_jobs += 1
        return total_num_running_jobs
    except Exception as e:
        logger.info(f"Faile to query number of running jobs in the cluster: {e}")
        return None


def get_total_num_nodes_to_report(gcs_client, timeout=None) -> Optional[int]:
    """Return the total number of alive nodes in the cluster"""
    try:
        result = gcs_client.get_all_node_info(timeout=timeout)
        total_num_nodes = 0
        for node in result.node_info_list:
            if node.state == gcs_utils.GcsNodeInfo.GcsNodeState.ALIVE:
                total_num_nodes += 1
        return total_num_nodes
    except Exception as e:
        logger.info(f"Faile to query number of nodes in the cluster: {e}")
        return None


def get_library_usages_to_report(gcs_client) -> List[str]:
    try:
        result = []
        library_usages = gcs_client.internal_kv_keys(
            usage_constant.LIBRARY_USAGE_PREFIX.encode(),
            namespace=usage_constant.USAGE_STATS_NAMESPACE.encode(),
        )
        for library_usage in library_usages:
            library_usage = library_usage.decode("utf-8")
            result.append(library_usage[len(usage_constant.LIBRARY_USAGE_PREFIX) :])

        try:
            historical_lib_usages = LibUsageRecorder(
                ray._private.utils.get_ray_temp_dir()
            ).read_lib_usages()
            for library_usage in historical_lib_usages:
                if library_usage not in result:
                    result.append(library_usage)
        except Exception as e:
            logger.info(f"Failed to read historical library usage {e}")
        return result
    except Exception as e:
        logger.info(f"Failed to get library usages to report {e}")
        return []


def get_extra_usage_tags_to_report(gcs_client) -> Dict[str, str]:
    """Get the extra usage tags from env var and gcs kv store.

    The env var should be given this way; key=value;key=value.
    If parsing is failed, it will return the empty data.

    Returns:
        Extra usage tags as kv pairs.
    """
    extra_usage_tags = dict()

    extra_usage_tags_env_var = os.getenv("RAY_USAGE_STATS_EXTRA_TAGS", None)
    if extra_usage_tags_env_var:
        try:
            kvs = extra_usage_tags_env_var.strip(";").split(";")
            for kv in kvs:
                k, v = kv.split("=")
                extra_usage_tags[k] = v
        except Exception as e:
            logger.info(f"Failed to parse extra usage tags env var. Error: {e}")

    valid_tag_keys = [tag_key.lower() for tag_key in TagKey.keys()]
    try:
        keys = gcs_client.internal_kv_keys(
            usage_constant.EXTRA_USAGE_TAG_PREFIX.encode(),
            namespace=usage_constant.USAGE_STATS_NAMESPACE.encode(),
        )
        for key in keys:
            value = gcs_client.internal_kv_get(
                key, namespace=usage_constant.USAGE_STATS_NAMESPACE.encode()
            )
            key = key.decode("utf-8")
            key = key[len(usage_constant.EXTRA_USAGE_TAG_PREFIX) :]
            assert key in valid_tag_keys
            extra_usage_tags[key] = value.decode("utf-8")
    except Exception as e:
        logger.info(f"Failed to get extra usage tags from kv store {e}")
    print("==============")
    print(extra_usage_tags)
    return extra_usage_tags


def get_cluster_status_to_report(gcs_client) -> ClusterStatusToReport:
    """Get the current status of this cluster.

    It is a blocking API.

    Params:
        gcs_client: The GCS client to perform KV operation GET.

    Returns:
        The current cluster status or empty if it fails to get that information.
    """
    try:
        cluster_status = gcs_client.internal_kv_get(
            ray._private.ray_constants.DEBUG_AUTOSCALING_STATUS.encode(),
            namespace=None,
        )
        if not cluster_status:
            return ClusterStatusToReport()

        result = ClusterStatusToReport()
        to_GiB = 1 / 2**30
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


def get_cluster_config_to_report(
    cluster_config_file_path: str,
) -> ClusterConfigToReport:
    """Get the static cluster (autoscaler) config used to launch this cluster.

    Params:
        cluster_config_file_path: The file path to the cluster config file.

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
        # Check if we're on Kubernetes
        if usage_constant.KUBERNETES_SERVICE_HOST_ENV in os.environ:
            # Check if we're using KubeRay >= 0.4.0.
            if usage_constant.KUBERAY_ENV in os.environ:
                result.cloud_provider = usage_constant.PROVIDER_KUBERAY
            # Else, we're on Kubernetes but not in either of the above categories.
            else:
                result.cloud_provider = usage_constant.PROVIDER_KUBERNETES_GENERIC
        return result
    except Exception as e:
        logger.info(f"Failed to get cluster config to report {e}")
        return ClusterConfigToReport()


def get_cluster_metadata(gcs_client) -> dict:
    """Get the cluster metadata from GCS.

    It is a blocking API.

    This will return None if `put_cluster_metadata` was never called.

    Params:
        gcs_client: The GCS client to perform KV operation GET.

    Returns:
        The cluster metadata in a dictinoary.

    Raises:
        RuntimeError if it fails to obtain cluster metadata from GCS.
    """
    return json.loads(
        gcs_client.internal_kv_get(
            usage_constant.CLUSTER_METADATA_KEY,
            namespace=ray_constants.KV_NAMESPACE_CLUSTER,
        ).decode("utf-8")
    )


def generate_report_data(
    cluster_config_to_report: ClusterConfigToReport,
    total_success: int,
    total_failed: int,
    seq_number: int,
    gcs_address: str,
) -> UsageStatsToReport:
    """Generate the report data.

    Params:
        cluster_config_to_report: The cluster (autoscaler)
            config generated by `get_cluster_config_to_report`.
        total_success: The total number of successful report
            for the lifetime of the cluster.
        total_failed: The total number of failed report
            for the lifetime of the cluster.
        seq_number: The sequence number that's incremented whenever
            a new report is sent.
        gcs_address: the address of gcs to get data to report.

    Returns:
        UsageStats
    """
    gcs_client = gcs_utils.GcsClient(address=gcs_address, nums_reconnect_retry=20)

    cluster_metadata = get_cluster_metadata(gcs_client)
    cluster_status_to_report = get_cluster_status_to_report(gcs_client)

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
        library_usages=get_library_usages_to_report(gcs_client),
        total_success=total_success,
        total_failed=total_failed,
        seq_number=seq_number,
        extra_usage_tags=get_extra_usage_tags_to_report(gcs_client),
        total_num_nodes=get_total_num_nodes_to_report(gcs_client),
        total_num_running_jobs=get_total_num_running_jobs_to_report(gcs_client),
    )
    return data


def generate_write_data(
    usage_stats: UsageStatsToReport,
    error: str,
) -> UsageStatsToWrite:
    """Generate the report data.

    Params:
        usage_stats: The usage stats that were reported.
        error: The error message of failed reports.

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
            data: Data to report
            dir_path: The path to the directory to write usage data.
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
            url: The URL to update resource usage.
            data: Data to report.

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
