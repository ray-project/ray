from abc import ABC, abstractmethod
import logging
import json
from ray._private.log import INTERNAL_TIMESTAMP_LOG_KEY
from ray._private.ray_logging.constants import (
    LogKey,
    LOGRECORD_STANDARD_ATTRS,
    LOGGER_FLATTEN_KEYS,
)
from ray._private.ray_constants import LOGGER_FORMAT
from typing import Any, Dict, List


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


class AbstractFormatter(logging.Formatter, ABC):
    def __init__(self, fmt=None, datefmt=None, style="%", validate=True) -> None:
        super().__init__(fmt, datefmt, style, validate)
        self._additional_log_standard_attrs = []

    def set_additional_log_standard_attrs(
        self, additional_log_standard_attrs: List[str]
    ) -> None:
        self._additional_log_standard_attrs = additional_log_standard_attrs

    def generate_record_format_attrs(
        self,
        record: logging.LogRecord,
        exclude_default_standard_attrs,
    ) -> dict:
        record_format_attrs = {}

        # If `exclude_default_standard_attrs` is False, include the standard attributes.
        # Otherwise, include only Ray and user-provided context.
        if not exclude_default_standard_attrs:
            record_format_attrs.update(
                {
                    LogKey.ASCTIME.value: self.formatTime(record),
                    LogKey.LEVELNAME.value: record.levelname,
                    LogKey.MESSAGE.value: record.getMessage(),
                    LogKey.FILENAME.value: record.filename,
                    LogKey.LINENO.value: record.lineno,
                }
            )
            if record.exc_info:
                if not record.exc_text:
                    record.exc_text = self.formatException(record.exc_info)
                record_format_attrs[LogKey.EXC_TEXT.value] = record.exc_text

        # Add the user specified additional standard attributes.
        for key in self._additional_log_standard_attrs:
            _append_flatten_attributes(
                record_format_attrs, key, getattr(record, key, None)
            )

        for key, value in record.__dict__.items():
            # Both Ray and user-provided context are stored in `record_format`.
            if key not in LOGRECORD_STANDARD_ATTRS:
                _append_flatten_attributes(record_format_attrs, key, value)

        # Format the internal timestamp to the standardized `timestamp_ns` key.
        if INTERNAL_TIMESTAMP_LOG_KEY in record_format_attrs:
            record_format_attrs[LogKey.TIMESTAMP_NS.value] = record_format_attrs.pop(
                INTERNAL_TIMESTAMP_LOG_KEY
            )

        return record_format_attrs

    @abstractmethod
    def format(self, record: logging.LogRecord) -> str:
        pass


class JSONFormatter(AbstractFormatter):
    def format(self, record: logging.LogRecord) -> str:
        record_format_attrs = self.generate_record_format_attrs(
            record, exclude_default_standard_attrs=False
        )
        return json.dumps(record_format_attrs)


class TextFormatter(AbstractFormatter):
    def __init__(self, fmt=None, datefmt=None, style="%", validate=True) -> None:
        super().__init__(fmt, datefmt, style, validate)
        self._inner_formatter = logging.Formatter(LOGGER_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        s = self._inner_formatter.format(record)
        record_format_attrs = self.generate_record_format_attrs(
            record, exclude_default_standard_attrs=True
        )

        additional_attrs = " ".join(
            [f"{key}={value}" for key, value in record_format_attrs.items()]
        )
        return f"{s} {additional_attrs}"
