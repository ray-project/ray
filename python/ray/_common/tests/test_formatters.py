import json
import logging
import logging.config
import sys

import pytest

from ray._common.formatters import JSONFormatter, TextFormatter


class TestJSONFormatter:
    def test_empty_record(self, shutdown_only):
        formatter = JSONFormatter()
        record = logging.makeLogRecord({})
        formatted = formatter.format(record)

        record_dict = json.loads(formatted)
        should_exist = [
            "process",
            "asctime",
            "levelname",
            "message",
            "filename",
            "lineno",
            "timestamp_ns",
        ]
        for key in should_exist:
            assert key in record_dict
        assert len(record_dict) == len(should_exist)
        assert "exc_text" not in record_dict

    def test_record_with_exception(self, shutdown_only):
        formatter = JSONFormatter()
        record = logging.makeLogRecord({})
        try:
            raise ValueError("test")
        except ValueError:
            record.exc_info = sys.exc_info()
        formatted = formatter.format(record)
        record_dict = json.loads(formatted)
        should_exist = [
            "process",
            "asctime",
            "levelname",
            "message",
            "filename",
            "lineno",
            "exc_text",
            "timestamp_ns",
        ]
        for key in should_exist:
            assert key in record_dict
        assert "Traceback (most recent call last):" in record_dict["exc_text"]
        assert len(record_dict) == len(should_exist)

    def test_record_with_user_provided_context(self, shutdown_only):
        formatter = JSONFormatter()
        record = logging.makeLogRecord({"user": "ray"})
        formatted = formatter.format(record)
        record_dict = json.loads(formatted)
        should_exist = [
            "process",
            "asctime",
            "levelname",
            "message",
            "filename",
            "lineno",
            "user",
            "timestamp_ns",
        ]
        for key in should_exist:
            assert key in record_dict
        assert record_dict["user"] == "ray"
        assert len(record_dict) == len(should_exist)
        assert "exc_text" not in record_dict

    def test_record_with_flatten_keys_invalid_value(self, shutdown_only):
        formatter = JSONFormatter()
        record = logging.makeLogRecord({"ray_serve_extra_fields": "not_a_dict"})
        with pytest.raises(ValueError):
            formatter.format(record)

    def test_record_with_flatten_keys_valid_dict(self, shutdown_only):
        formatter = JSONFormatter()
        record = logging.makeLogRecord(
            {"ray_serve_extra_fields": {"key1": "value1", "key2": 2}}
        )
        formatted = formatter.format(record)
        record_dict = json.loads(formatted)
        should_exist = [
            "process",
            "asctime",
            "levelname",
            "message",
            "filename",
            "lineno",
            "key1",
            "key2",
            "timestamp_ns",
        ]
        for key in should_exist:
            assert key in record_dict
        assert record_dict["key1"] == "value1", record_dict
        assert record_dict["key2"] == 2
        assert "ray_serve_extra_fields" not in record_dict
        assert len(record_dict) == len(should_exist)
        assert "exc_text" not in record_dict

    def test_record_with_valid_additional_log_standard_attrs(self, shutdown_only):
        formatter = JSONFormatter()
        formatter.set_additional_log_standard_attrs(["name"])
        record = logging.makeLogRecord({})
        formatted = formatter.format(record)

        record_dict = json.loads(formatted)
        should_exist = [
            "process",
            "asctime",
            "levelname",
            "message",
            "filename",
            "lineno",
            "timestamp_ns",
            "name",
        ]
        for key in should_exist:
            assert key in record_dict
        assert len(record_dict) == len(should_exist)


class TestTextFormatter:
    def test_record_with_user_provided_context(self):
        formatter = TextFormatter()
        record = logging.makeLogRecord({"user": "ray"})
        formatted = formatter.format(record)
        assert "user=ray" in formatted

    def test_record_with_exception(self):
        formatter = TextFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1000,
            msg="Test message",
            args=None,
            exc_info=None,
        )
        formatted = formatter.format(record)
        for s in ["INFO", "Test message", "test.py:1000", "--"]:
            assert s in formatted

    def test_record_with_valid_additional_log_standard_attrs(self, shutdown_only):
        formatter = TextFormatter()
        formatter.set_additional_log_standard_attrs(["name"])
        record = logging.makeLogRecord({})
        formatted = formatter.format(record)
        assert "name=" in formatted


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
