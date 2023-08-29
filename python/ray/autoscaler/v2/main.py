"""Autoscaler monitoring loop daemon."""

import argparse
import json
import logging
import os
import signal
import sys
import time
import traceback
from collections import Counter
from dataclasses import asdict
from typing import Any, Callable, Dict, Optional, Union

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
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler._private.util import format_readonly_node_type
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
        log_dir: str = None,
        prefix_cluster_info: bool = False,
        monitor_ip: Optional[str] = None,
        retry_on_failure: bool = True,
):
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse GCS server for the monitor to connect to.")
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
        required=False,
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
        help="The IP address of the machine hosting the monitor process.",
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

    logger.info(f"Starting monitor using ray installation: {ray.__file__}")
    logger.info(f"Ray version: {ray.__version__}")
    logger.info(f"Ray commit: {ray.__commit__}")
    logger.info(f"Monitor started with command: {sys.argv}")

    if args.autoscaling_config:
        autoscaling_config = os.path.expanduser(args.autoscaling_config)
    else:
        autoscaling_config = None

    bootstrap_address = args.gcs_address
    if bootstrap_address is None:
        raise ValueError("--gcs-address must be set!")
    

    run(
        bootstrap_address,
        autoscaling_config,
        log_dir=args.logs_dir,
        monitor_ip=args.monitor_ip,
    )

