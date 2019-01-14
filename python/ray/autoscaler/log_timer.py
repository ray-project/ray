import datetime
import logging
import sys

def get_logger():
    l = logging.getLogger("ray.autoscaler")
    if l.handlers:
        # It's already set up.
        return l

    h = logging.StreamHandler(stream=sys.stdout)

    h.setFormatter(logging.Formatter(
        "%(asctime)s - %(message)s".format()
    ))

    l.setLevel(logging.INFO)
    h.setLevel(logging.INFO)

    l.addHandler(h)
    l.propagate = False

    return l

logger = get_logger()

def _logFormat(caller, obj, td=None):
    if td is None:
        td = ""
    else:
        td = "{:.0f}ms".format(td.total_seconds()*1000)

    return " - ".join([caller, td, str(obj)])

def logDebug(*args, **kwargs):
    logger.debug(_logFormat(*args, **kwargs))

def logInfo(*args, **kwargs):
    logger.info(_logFormat(*args, **kwargs))

def logWarning(*args, **kwargs):
    logger.warning(_logFormat(*args, **kwargs))

def logError(*args, **kwargs):
    logger.error(_logFormat(*args, **kwargs))

def logException(*args, **kwargs):
    logger.exception(_logFormat(*args, **kwargs))

def logCritical(*args, **kwargs):
    logger.critical(_logFormat(*args, **kwargs))

class LogTimer:
    def __init__(self, caller, message, log_fn=logInfo):
        self._caller = caller
        self._message = message
        self._log_fn = log_fn

    def __enter__(self):
        self._start_time = datetime.datetime.utcnow()

    def __exit__(self, *_):
        self._log_fn(
            self._caller, self._message,
            td=(datetime.datetime.utcnow() - self._start_time)
        )
