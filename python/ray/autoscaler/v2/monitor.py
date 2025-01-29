"""Autoscaler monitoring loop daemon.

See autoscaler._private/monitor.py for the legacy implementation. All the legacy flags
are supported here, but the new implementation uses the new autoscaler v2.
"""

import argparse
import logging
import os
import sys
import time
from typing import Optional

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils
from ray._private.event.event_logger import get_event_logger
from ray._private.ray_logging import setup_component_logger
from ray._private.usage.usage_lib import record_extra_usage_tag
from ray._private.worker import SCRIPT_MODE
from ray._raylet import GcsClient
from ray.autoscaler._private.constants import (
    AUTOSCALER_METRIC_PORT,
    AUTOSCALER_UPDATE_INTERVAL_S,
)
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import (
    FileConfigReader,
    IConfigReader,
    ReadOnlyProviderConfigReader,
)
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.core.generated.autoscaler_pb2 import AutoscalingState
from ray.core.generated.event_pb2 import Event as RayEvent
from ray.core.generated.usage_pb2 import TagKey

try:
    import prometheus_client
except ImportError:
    prometheus_client = None


logger = logging.getLogger(__name__)


class AutoscalerMonitor:
    """Autoscaling monitor.

    This process periodically collects stats from the GCS and triggers
    autoscaler updates.

    TODO:
    We should also handle autoscaler failures properly in the future.
    Right now, we don't restart autoscaler if it fails (internal reconciliation
    however, should not fail the autoscaler process).
    With the Reconciler able to handle extra cloud instances, we could in fact
    recover the autoscaler process from reconciliation.
    """

    def __init__(
        self,
        address: str,
        config_reader: IConfigReader,
        log_dir: Optional[str] = None,
        monitor_ip: Optional[str] = None,
    ):
        # Record v2 usage (we do this as early as possible to capture usage)
        record_autoscaler_v2_usage(GcsClient(address))

        self.gcs_address = address
        worker = ray._private.worker.global_worker
        # TODO: eventually plumb ClusterID through to here
        self.gcs_client = GcsClient(address=self.gcs_address)

        if monitor_ip:
            monitor_addr = f"{monitor_ip}:{AUTOSCALER_METRIC_PORT}"
            self.gcs_client.internal_kv_put(
                b"AutoscalerMetricsAddress", monitor_addr.encode(), True, None
            )
        self._session_name = self._get_session_name(self.gcs_client)
        logger.info(f"session_name: {self._session_name}")
        worker.set_mode(SCRIPT_MODE)
        head_node_ip = self.gcs_address.split(":")[0]

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
        try:
            self._run()
        except Exception:
            logger.exception("Error in monitor loop")
            raise


def record_autoscaler_v2_usage(gcs_client: GcsClient) -> None:
    """
    Record usage for autoscaler v2.
    """
    try:
        record_extra_usage_tag(TagKey.AUTOSCALER_VERSION, "v2", gcs_client)
    except Exception:
        logger.exception("Error recording usage for autoscaler v2.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse GCS server for the monitor to connect to.")
    )
    parser.add_argument(
        "--gcs-address", required=False, type=str, help="The address (ip:port) of GCS."
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

    logger.info(
        f"Starting autoscaler v2 monitor using ray installation: {ray.__file__}"
    )
    logger.info(f"Ray version: {ray.__version__}")
    logger.info(f"Ray commit: {ray.__commit__}")
    logger.info(f"AutoscalerMonitor started with command: {sys.argv}")

    gcs_address = args.gcs_address
    if gcs_address is None:
        raise ValueError("--gcs-address must be set!")

    if not args.autoscaling_config:
        logger.info("No autoscaling config provided: use read only node provider.")
        config_reader = ReadOnlyProviderConfigReader(gcs_address)
    else:
        autoscaling_config = os.path.expanduser(args.autoscaling_config)
        config_reader = FileConfigReader(
            config_file=autoscaling_config, skip_content_hash=True
        )

    monitor = AutoscalerMonitor(
        gcs_address,
        config_reader,
        log_dir=args.logs_dir,
        monitor_ip=args.monitor_ip,
    )

    monitor.run()
