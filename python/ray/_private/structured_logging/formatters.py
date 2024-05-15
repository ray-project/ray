import logging
import json
from ray._private.structured_logging.constants import LogKey, LOGRECORD_STANDARD_ATTRS


def generate_record_format_attrs(
    formatter: logging.Formatter, record: logging.LogRecord
) -> dict:
    record_format_attrs = {
        LogKey.ASCTIME: formatter.formatTime(record),
        LogKey.LEVELNAME: record.levelname,
        LogKey.MESSAGE: record.getMessage(),
        LogKey.FILENAME: record.filename,
        LogKey.LINENO: record.lineno,
    }
    if record.exc_info:
        if not record.exc_text:
            record.exc_text = formatter.formatException(record.exc_info)
        record_format_attrs[LogKey.EXC_TEXT] = record.exc_text

    for key, value in record.__dict__.items():
        # Both Ray and user-provided context are stored in `record_format`.
        if key not in LOGRECORD_STANDARD_ATTRS:
            record_format_attrs[key] = value
    return record_format_attrs


class JSONFormatter(logging.Formatter):
    def format(self, record):
        record_format_attrs = generate_record_format_attrs(self, record)
        return json.dumps(record_format_attrs)


class LogfmtFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record_format_attrs = generate_record_format_attrs(self, record)
        return " ".join(
            [f"{key}={value}" for key, value in record_format_attrs.items()]
        )
