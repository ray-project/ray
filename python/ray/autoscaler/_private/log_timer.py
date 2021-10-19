import datetime
import logging

from ray.autoscaler._private.cli_logger import cli_logger

logger = logging.getLogger(__name__)


class LogTimer:
    def __init__(self, message, show_status=False):
        self._message = message
        self._show_status = show_status

    def __enter__(self):
        self._start_time = datetime.datetime.utcnow()

    def __exit__(self, *error_vals):
        if cli_logger.log_style != "record":
            return

        td = datetime.datetime.utcnow() - self._start_time
        status = ""
        if self._show_status:
            status = "failed" if any(error_vals) else "succeeded"
        cli_logger.print(" ".join([
            self._message, status,
            "[LogTimer={:.0f}ms]".format(td.total_seconds() * 1000)
        ]))
