import logging.config
import pytest
import ray
import os
import logging
import sys
import json

from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter, TextFormatter
from ray.job_config import LoggingConfig
from ray._private.test_utils import run_string_as_driver


class TestCoreContextFilter:
    def test_driver_process(self, shutdown_only):
        filter = CoreContextFilter()
        record = logging.makeLogRecord({})
        assert filter.filter(record)
        should_exist = ["job_id", "worker_id", "node_id"]
        runtime_context = ray.get_runtime_context()
        expected_values = {
            "job_id": runtime_context.get_job_id(),
            "worker_id": runtime_context.get_worker_id(),
            "node_id": runtime_context.get_node_id(),
        }
        for attr in should_exist:
            assert hasattr(record, attr)
            assert getattr(record, attr) == expected_values[attr]
        # This is not a worker process, so actor_id and task_id should not exist.
        should_not_exist = ["actor_id", "task_id"]
        for attr in should_not_exist:
            assert not hasattr(record, attr)

    def test_task_process(self, shutdown_only):
        @ray.remote
        def f():
            filter = CoreContextFilter()
            record = logging.makeLogRecord({})
            assert filter.filter(record)
            should_exist = ["job_id", "worker_id", "node_id", "task_id"]
            runtime_context = ray.get_runtime_context()
            expected_values = {
                "job_id": runtime_context.get_job_id(),
                "worker_id": runtime_context.get_worker_id(),
                "node_id": runtime_context.get_node_id(),
                "task_id": runtime_context.get_task_id(),
            }
            for attr in should_exist:
                assert hasattr(record, attr)
                assert getattr(record, attr) == expected_values[attr]
            assert not hasattr(record, "actor_id")

        obj_ref = f.remote()
        ray.get(obj_ref)

    def test_actor_process(self, shutdown_only):
        @ray.remote
        class A:
            def f(self):
                filter = CoreContextFilter()
                record = logging.makeLogRecord({})
                assert filter.filter(record)
                should_exist = ["job_id", "worker_id", "node_id", "actor_id", "task_id"]
                runtime_context = ray.get_runtime_context()
                expected_values = {
                    "job_id": runtime_context.get_job_id(),
                    "worker_id": runtime_context.get_worker_id(),
                    "node_id": runtime_context.get_node_id(),
                    "actor_id": runtime_context.get_actor_id(),
                    "task_id": runtime_context.get_task_id(),
                }
                for attr in should_exist:
                    assert hasattr(record, attr)
                    assert getattr(record, attr) == expected_values[attr]

        actor = A.remote()
        ray.get(actor.f.remote())


class TestJSONFormatter:
    def test_empty_record(self, shutdown_only):
        formatter = JSONFormatter()
        record = logging.makeLogRecord({})
        formatted = formatter.format(record)
        record_dict = json.loads(formatted)
        should_exist = ["asctime", "levelname", "message", "filename", "lineno"]
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
            "asctime",
            "levelname",
            "message",
            "filename",
            "lineno",
            "exc_text",
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
        should_exist = ["asctime", "levelname", "message", "filename", "lineno", "user"]
        for key in should_exist:
            assert key in record_dict
        assert record_dict["user"] == "ray"
        assert len(record_dict) == len(should_exist)
        assert "exc_text" not in record_dict


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


class TestLoggingConfig:
    def test_log_level(self):
        log_level = "DEBUG"
        logging_config = LoggingConfig(log_level=log_level)
        dict_config = logging_config._get_dict_config()
        assert dict_config["handlers"]["console"]["level"] == log_level
        assert dict_config["root"]["level"] == log_level

    def test_invalid_dict_config(self):
        with pytest.raises(ValueError):
            LoggingConfig(encoding="INVALID")._get_dict_config()


class TestTextModeE2E:
    def test_text_mode_task(self, shutdown_only):
        script = """
import ray
import logging

ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT")
)

@ray.remote
def f():
    logger = logging.getLogger(__name__)
    logger.info("This is a Ray task")

obj_ref = f.remote()
ray.get(obj_ref)
"""
        stderr = run_string_as_driver(script)
        should_exist = [
            "job_id",
            "worker_id",
            "node_id",
            "task_id",
            "INFO",
            "This is a Ray task",
        ]
        for s in should_exist:
            assert s in stderr
        assert "actor_id" not in stderr

    def test_text_mode_actor(self, shutdown_only):
        script = """
import ray
import logging

ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT")
)

@ray.remote
class actor:
    def __init__(self):
        pass

    def print_message(self):
        logger = logging.getLogger(__name__)
        logger.info("This is a Ray actor")

actor_instance = actor.remote()
ray.get(actor_instance.print_message.remote())
"""
        stderr = run_string_as_driver(script)
        should_exist = [
            "job_id",
            "worker_id",
            "node_id",
            "actor_id",
            "task_id",
            "INFO",
            "This is a Ray actor",
        ]
        for s in should_exist:
            assert s in stderr


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
