import logging
import os
import json
from ray._private.ray_logging.constants import (
    LogKey,
    LOGRECORD_STANDARD_ATTRS,
    LOGGER_FLATTEN_KEYS,
)
from ray._private.ray_constants import (
    LOGGER_FORMAT,
    LOGGER_FORMAT_STDERR_ENVIRONMENTAL_VARIABLE,
    LOGGER_FORMAT_STDERR_DEFAULT,
)
from typing import Any, Dict, Optional


def _get_logging_redirect_stderr_format(component: Optional[str]) -> str:
    """Get the logging format for logs redirected to stderr.

    Use the users format string if it is set, otherwise use the default format string.
    Format with the component if it is provided and present in the format string.
    """

    desired_format = os.environ.get(
        LOGGER_FORMAT_STDERR_ENVIRONMENTAL_VARIABLE, LOGGER_FORMAT_STDERR_DEFAULT
    )

    if component is not None:
        # this is a no-op if `{component}` is not in the str
        desired_format = desired_format.format(component=component)

    return desired_format


def _append_flatten_attributes(formatted_attrs: Dict[str, Any], key: str, value: Any):
    """Flatten the dictionary values for special keys and append the values in place.

    If the key is in `LOGGER_FLATTEN_KEYS`, the value will be flattened and appended
    to the `formatted_attrs` dictionary. Otherwise, the key-value pair will be appended
    directly.
    """
    if key in LOGGER_FLATTEN_KEYS:
        if not isinstance(value, dict):
            raise ValueError(
                f"Expected a dictionary passing into {key}, but got {type(value)}"
            )
        for k, v in value.items():
            if k in formatted_attrs:
                raise KeyError(f"Found duplicated key in the log record: {k}")
            formatted_attrs[k] = v
    else:
        formatted_attrs[key] = value


def generate_record_format_attrs(
    formatter: logging.Formatter,
    record: logging.LogRecord,
    exclude_standard_attrs,
) -> dict:
    record_format_attrs = {}

    # If `exclude_standard_attrs` is False, include the standard attributes.
    # Otherwise, include only Ray and user-provided context.
    if not exclude_standard_attrs:
        record_format_attrs.update(
            {
                LogKey.ASCTIME.value: formatter.formatTime(record),
                LogKey.LEVELNAME.value: record.levelname,
                LogKey.MESSAGE.value: record.getMessage(),
                LogKey.FILENAME.value: record.filename,
                LogKey.LINENO.value: record.lineno,
            }
        )
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = formatter.formatException(record.exc_info)
            record_format_attrs[LogKey.EXC_TEXT.value] = record.exc_text

    for key, value in record.__dict__.items():
        # Both Ray and user-provided context are stored in `record_format`.
        if key not in LOGRECORD_STANDARD_ATTRS:
            _append_flatten_attributes(record_format_attrs, key, value)
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
