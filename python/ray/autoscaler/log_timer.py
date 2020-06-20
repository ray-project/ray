import datetime
import logging

logger = logging.getLogger(__name__)


class LogTimer:
    def __init__(self, message, show_status=False):
        self._message = message
        self._show_status = show_status

    def __enter__(self):
        self._start_time = datetime.datetime.utcnow()

    def __exit__(self, *error_vals):
        td = datetime.datetime.utcnow() - self._start_time
        status = ""
        if self._show_status:
            status = "failed" if any(error_vals) else "succeeded"
        logger.info(" ".join([
            self._message, status,
            "[LogTimer={:.0f}ms]".format(td.total_seconds() * 1000)
        ]))
