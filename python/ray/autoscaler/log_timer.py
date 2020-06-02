import datetime
import logging

logger = logging.getLogger(__name__)


class LogTimer:
    def __init__(self, message, success_string=""):
        self._message = message
        self._status_string = success_string

    def __enter__(self):
        self._start_time = datetime.datetime.utcnow()

    def __exit__(self, *error_vals):
        td = datetime.datetime.utcnow() - self._start_time
        if any(error_vals):
            self._status_string = "FAILED"
        logger.info(" ".join([
            self._message, self._status_string,
            "[LogTimer={:.0f}ms]".format(td.total_seconds() * 1000)
        ]))
