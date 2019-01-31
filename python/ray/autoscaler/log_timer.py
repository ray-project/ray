from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import logging

logger = logging.getLogger(__name__)


class LogTimer():
    def __init__(self, message):
        self._message = message

    def __enter__(self):
        self._start_time = datetime.datetime.utcnow()

    def __exit__(self, *_):
        td = datetime.datetime.utcnow() - self._start_time
        logger.info(self._message +
                    " [LogTimer={:.0f}ms]".format(td.total_seconds() * 1000))
