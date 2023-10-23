"""Autoscaler autoscalering loop daemon."""

import argparse
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    ThreadedRayInstaller,
)
import yaml
import json
import logging
import os
import signal
import sys
import time
import traceback
from collections import Counter
from dataclasses import asdict
from typing import Any, Callable, Dict, List, Optional, Union

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils
from ray._private.event.event_logger import get_event_logger
from ray._private.ray_logging import setup_component_logger
from ray._raylet import GcsClient
from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.commands import teardown_cluster
from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE,
    AUTOSCALER_METRIC_PORT,
    AUTOSCALER_UPDATE_INTERVAL_S,
)
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler._private.providers import _get_node_provider
from ray.autoscaler._private.util import ConcurrentCounter, format_readonly_node_type
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.instance_manager.instance_manager import SimpleInstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.instance_launcher import (
    InstanceLauncher,
)
from ray.autoscaler.v2.instance_manager.subscribers.reconciler import InstanceReconciler
from ray.core.generated import gcs_pb2, gcs_service_pb2, gcs_service_pb2_grpc
from ray.core.generated.event_pb2 import Event as RayEvent
from ray.experimental.internal_kv import (
    _initialize_internal_kv,
    _internal_kv_del,
    _internal_kv_get,
    _internal_kv_initialized,
    _internal_kv_put,
)

try:
    import prometheus_client
except ImportError:
    prometheus_client = None


logger = logging.getLogger(__name__)


def run(
    address: str,
    autoscaling_config: Union[str, Callable[[], Dict[str, Any]]],
    session_name: str,
    log_dir: str = None,
    prefix_cluster_info: bool = False,
    monitor_ip: Optional[str] = None,
    retry_on_failure: bool = True,
):
    """
    TODO:
        1. autoscaler error handling:
            - kill any nodes?
            - inform the user.
        2.
    """

    gcs_client = GcsClient(address=address)

    # autoscaler metrics address setup.
    _initialize_internal_kv(gcs_client)
    if monitor_ip:
        autoscaler_addr = f"{monitor_ip}:{AUTOSCALER_METRIC_PORT}"
        gcs_client.internal_kv_put(
            b"AutoscalerMetricsAddress", autoscaler_addr.encode(), True, None
        )

    # Set up the prometheus metrics.
    # TODO

    # Local laptop (static cluster) mode, i.e. read_only
    # TODO

    # Set up event logger.
    # TODO

    ins_storage = InstanceStorage(
        cluster_id=session_name,
        storage=InMemoryStorage(),
    )
    node_provider_v1 = _get_node_provider(
        autoscaling_config["provider"], autoscaling_config["cluster_name"]
    )

    # TODO: are the inits still relevant now?
    node_launcher = BaseNodeLauncher(
        provider=node_provider_v1,
        pending=ConcurrentCounter(),
        event_summarizer=EventSummarizer(),
        node_provider_availability_tracker=NodeProviderAvailabilityTracker(),
        session_name=session_name,
        node_types=autoscaling_config["available_node_types"],
        prom_metrics=AutoscalerPrometheusMetrics(session_name=session_name),
    )

    node_provider = NodeProviderAdapter(
        provider=node_provider_v1,
        node_launcher=node_launcher,
        instance_config_provider=NodeProviderConfig(node_configs=autoscaling_config),
    )
    subscribers: List[InstanceUpdatedSubscriber] = []

    if not node_provider.disable_ray_installer():
        subscribers.append(
            ThreadedRayInstaller(
                head_node_ip=monitor_ip,
                instance_storage=ins_storage,
                ray_installer=RayInstaller(
                    provider=node_provider_v1,
                    config=NodeProviderConfig(node_configs=autoscaling_config),
                ),
            )
        )

    # TODO: pass in configs
    subscribers.append(
        InstanceLauncher(instance_storage=ins_storage, node_provider=node_provider)
    )

    # TODO: pass in configs
    reconciler = InstanceReconciler(
        instance_storage=ins_storage, node_provider=node_provider
    )
    subscribers.append(reconciler)

    im = SimpleInstanceManager(
        instance_storage=ins_storage,
        available_node_types=autoscaling_config["available_node_types"],
        status_change_subscribers=subscribers,
    )

    # Run the autoscaling loop.
    from ray.autoscaler.v2.sdk import AutoscalerV2

    autoscaler = AutoscalerV2(
        config=autoscaling_config,
        instance_manager=im,
        gcs_client=gcs_client,
    )
    reconciler.start()
    while True:
        autoscaler.update()
        time.sleep(5)

    # TODO: handle autoscaler errors?
    reconciler.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse GCS server for the autoscaler to connect to.")
    )
    parser.add_argument(
        "--gcs-address", required=False, type=str, help="The address (ip:port) of GCS."
    )
    parser.add_argument(
        "--redis-address", required=False, type=str, help="This is deprecated"
    )
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="This is deprecated",
    )
    parser.add_argument(
        "--autoscaling-config",
        # TODO: provide a default config file.
        required=True,
        type=str,
        help="the path to the autoscaling config file",
    )
    parser.add_argument(
        "--logging-level",
        required=False,
        type=str,
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP,
    )
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP,
    )
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=ray_constants.MONITOR_LOG_FILE_NAME,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is "
        f'"{ray_constants.MONITOR_LOG_FILE_NAME}"',
    )
    parser.add_argument(
        "--logs-dir",
        required=True,
        type=str,
        help="Specify the path of the temporary directory used by Ray processes.",
    )
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is "
        f"{ray_constants.LOGGING_ROTATE_BYTES} bytes.",
    )
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is "
        f"{ray_constants.LOGGING_ROTATE_BACKUP_COUNT}.",
    )
    parser.add_argument(
        "--monitor-ip",
        required=False,
        type=str,
        default=None,
        help="The IP address of the machine hosting the autoscaler process.",
    )
    parser.add_argument(
        "--session-name",
        required=False,
        type=str,
        default="",
        help="The name of the session to use for the autoscaler.",
    )

    args = parser.parse_args()
    setup_component_logger(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.logs_dir,
        filename=args.logging_filename,
        max_bytes=args.logging_rotate_bytes,
        backup_count=args.logging_rotate_backup_count,
    )

    logger.info(f"Starting autoscaler using ray installation: {ray.__file__}")
    logger.info(f"Ray version: {ray.__version__}")
    logger.info(f"Ray commit: {ray.__commit__}")
    logger.info(f"autoscaler started with command: {sys.argv}")

    autoscaling_config_path = os.path.expanduser(args.autoscaling_config)
    # Read the config
    with open(autoscaling_config_path) as f:
        config = yaml.safe_load(f.read())

    bootstrap_address = args.gcs_address
    if bootstrap_address is None:
        raise ValueError("--gcs-address must be set!")

    run(
        bootstrap_address,
        config,
        log_dir=args.logs_dir,
        monitor_ip=args.monitor_ip,
        session_name=args.session_name,
    )
