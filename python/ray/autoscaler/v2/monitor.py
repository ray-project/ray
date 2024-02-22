"""Autoscaler monitoring loop daemon."""

import argparse
import logging
import os
import sys
import time
from typing import Any, Callable, Dict, Optional, Union

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils
from ray._private.event.event_logger import get_event_logger
from ray._private.ray_logging import setup_component_logger
from ray._raylet import GcsClient
from ray.autoscaler._private.constants import (
    AUTOSCALER_METRIC_PORT,
    AUTOSCALER_UPDATE_INTERVAL_S,
)
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import FileConfigReader
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.core.generated.autoscaler_pb2 import AutoscalingState
from ray.core.generated.event_pb2 import Event as RayEvent
from ray.experimental.internal_kv import (
    _initialize_internal_kv,
    _internal_kv_initialized,
)

try:
    import prometheus_client
except ImportError:
    prometheus_client = None


logger = logging.getLogger(__name__)


class AutoscalerMonitor:
    """Autoscaling monitor.

    This process periodically collects stats from the GCS and triggers
    autoscaler updates.
    """

    def __init__(
        self,
        gcs_address: str,
        autoscaling_config: Union[str, Callable[[], Dict[str, Any]]],
        log_dir: str = None,
        monitor_ip: str = None,
    ):
        self.gcs_address = gcs_address
        worker = ray._private.worker.global_worker
        # TODO: eventually plumb ClusterID through to here
        self.gcs_client = GcsClient(address=self.gcs_address)
        _initialize_internal_kv(self.gcs_client)
        if monitor_ip:
            monitor_addr = f"{monitor_ip}:{AUTOSCALER_METRIC_PORT}"
            self.gcs_client.internal_kv_put(
                b"AutoscalerMetricsAddress", monitor_addr.encode(), True, None
            )
        self._session_name = self._get_session_name(self.gcs_client)
        logger.info(f"session_name: {self._session_name}")
        worker.mode = 0
        head_node_ip = self.gcs_address.split(":")[0]

        self.autoscaling_config = autoscaling_config
        self.autoscaler = None
        if log_dir:
            try:
                ray_event_logger = get_event_logger(
                    RayEvent.SourceType.AUTOSCALER, log_dir
                )
                self.event_logger = AutoscalerEventLogger(ray_event_logger)
            except Exception:
                self.event_logger = None
        else:
            self.event_logger = None

        prom_metrics = AutoscalerPrometheusMetrics(session_name=self._session_name)
        self.metric_reporter = AutoscalerMetricsReporter(prom_metrics)

        if monitor_ip and prometheus_client:
            # If monitor_ip wasn't passed in, then don't attempt to start the
            # metric server to keep behavior identical to before metrics were
            # introduced
            try:
                logger.info(
                    "Starting autoscaler metrics server on port {}".format(
                        AUTOSCALER_METRIC_PORT
                    )
                )
                kwargs = {"addr": "127.0.0.1"} if head_node_ip == "127.0.0.1" else {}
                prometheus_client.start_http_server(
                    port=AUTOSCALER_METRIC_PORT,
                    registry=prom_metrics.registry,
                    **kwargs,
                )
            except Exception:
                logger.exception(
                    "An exception occurred while starting the metrics server."
                )
        elif not prometheus_client:
            logger.warning(
                "`prometheus_client` not found, so metrics will not be exported."
            )

        config_reader = FileConfigReader(
            config_file=autoscaling_config, skip_content_hash=True
        )

        self.autoscaler = Autoscaler(
            session_name=self._session_name,
            config_reader=config_reader,
            gcs_client=self.gcs_client,
            event_logger=self.event_logger,
            metrics_reporter=self.metric_reporter,
        )

    @staticmethod
    def _get_session_name(gcs_client: GcsClient) -> Optional[str]:
        """Obtain the session name from the GCS.

        If the GCS doesn't respond, session name is considered None.
        In this case, the metrics reported from the monitor won't have
        the correct session name.
        """
        if not _internal_kv_initialized():
            return None

        session_name = gcs_client.internal_kv_get(
            b"session_name",
            ray_constants.KV_NAMESPACE_SESSION,
            timeout=10,
        )

        if session_name:
            session_name = session_name.decode()

        return session_name

    @staticmethod
    def _report_autoscaling_state(
        gcs_client: GcsClient, autoscaling_state: AutoscalingState
    ):
        """Report the autoscaling state to the GCS."""
        try:
            gcs_client.report_autoscaling_state(autoscaling_state.SerializeToString())
        except Exception:
            logger.exception("Error reporting autoscaling state to GCS.")

    def _run(self):
        """Run the monitor loop."""

        while True:
            autoscaling_state = self.autoscaler.update_autoscaling_state()
            if autoscaling_state:
                # report autoscaling state
                self._report_autoscaling_state(self.gcs_client, autoscaling_state)
            else:
                logger.warning("No autoscaling state to report.")

            # Wait for a autoscaler update interval before processing the next
            # round of messages.
            time.sleep(AUTOSCALER_UPDATE_INTERVAL_S)

    def run(self):
        # FIXME:
        # Sleep for now to wait til head node is ready
        time.sleep(1)
        try:
            self._run()
        except Exception:
            logger.exception("Error in monitor loop")
            raise


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

    if not args.autoscaling_config:
        logger.info("No autoscaling config provided: Not running autoscaler monitor.")
        exit(0)

    autoscaling_config = os.path.expanduser(args.autoscaling_config)
    logger.info(f"Starting monitor using ray installation: {ray.__file__}")
    logger.info(f"Ray version: {ray.__version__}")
    logger.info(f"Ray commit: {ray.__commit__}")
    logger.info(f"AutoscalerMonitor started with command: {sys.argv}")

    bootstrap_address = args.gcs_address
    if bootstrap_address is None:
        raise ValueError("--gcs-address must be set!")

    monitor = AutoscalerMonitor(
        bootstrap_address,
        autoscaling_config,
        log_dir=args.logs_dir,
        monitor_ip=args.monitor_ip,
    )

    monitor.run()
