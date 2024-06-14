import logging
import json
from ray._private.ray_logging.constants import LogKey, LOGRECORD_STANDARD_ATTRS
from ray._private.ray_constants import LOGGER_FORMAT


def generate_record_format_attrs(
    formatter: logging.Formatter,
    record: logging.LogRecord,
    exclude_standard_attrs,
) -> dict:
    record_format_attrs = {}
    # If `exclude_standard_attrs` is False, include the standard attributes.
    # Otherwise, include only Ray and user-provided context.
    if not exclude_standard_attrs:
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
        record_format_attrs = generate_record_format_attrs(
            self, record, exclude_standard_attrs=False
        )
        return json.dumps(record_format_attrs)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        self._inner_formatter = logging.Formatter(LOGGER_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        s = self._inner_formatter.format(record)
        record_format_attrs = generate_record_format_attrs(
            self, record, exclude_standard_attrs=True
        )
        additional_attrs = " ".join(
            [f"{key}={value}" for key, value in record_format_attrs.items()]
        )
        return f"{s} {additional_attrs}"
