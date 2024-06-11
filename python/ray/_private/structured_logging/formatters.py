import logging
import json
from ray._private.structured_logging.constants import LogKey, LOGRECORD_STANDARD_ATTRS


class JSONFormatter(logging.Formatter):
    def format(self, record):
        record_format = {
            LogKey.ASCTIME: self.formatTime(record),
            LogKey.LEVELNAME: record.levelname,
            LogKey.MESSAGE: record.getMessage(),
            LogKey.FILENAME: record.filename,
            LogKey.LINENO: record.lineno,
        }
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            record_format[LogKey.EXC_TEXT] = record.exc_text

        for key, value in record.__dict__.items():
            # Both Ray and user-provided context are stored in `record_format`.
            if key not in LOGRECORD_STANDARD_ATTRS:
                record_format[key] = value
        return json.dumps(record_format)
