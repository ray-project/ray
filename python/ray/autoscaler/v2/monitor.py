"""Autoscaler autoscalering loop daemon."""

import argparse
from ray.autoscaler.v2.autoscaler import AutoscalerV2
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
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceManager,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
)
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.config import FileConfigReader
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    ThreadedRayInstaller,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopper

from ray.experimental.internal_kv import (
    _initialize_internal_kv,
)

try:
    import prometheus_client
except ImportError:
    prometheus_client = None


logger = logging.getLogger(__name__)


def run(
    address: str,
    autoscaling_config_path: str,
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

    config_reader = FileConfigReader(autoscaling_config_path, skip_content_hash=True)
    autoscaler = AutoscalerV2(session_name, config_reader, gcs_client)

    while True:
        # Get the ray status.



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
        autoscaling_config_path,
        log_dir=args.logs_dir,
        monitor_ip=args.monitor_ip,
        session_name=args.session_name,
    )
