import logging.config
import pytest
import ray
import logging
import sys
import json

from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter, TextFormatter
from ray._private.ray_logging.logging_config import LoggingConfig
from ray._private.test_utils import run_string_as_driver


class TestCoreContextFilter:
    def test_driver_process(self, shutdown_only):
        log_context = ["job_id", "worker_id", "node_id"]
        filter = CoreContextFilter()
        record = logging.makeLogRecord({})
        assert filter.filter(record)
        # Ray is not initialized so no context
        for attr in log_context:
            assert not hasattr(record, attr)
        assert hasattr(record, "_ray_timestamp_ns")

        ray.init()
        record = logging.makeLogRecord({})
        assert filter.filter(record)
        runtime_context = ray.get_runtime_context()
        expected_values = {
            "job_id": runtime_context.get_job_id(),
            "worker_id": runtime_context.get_worker_id(),
            "node_id": runtime_context.get_node_id(),
        }
        for attr in log_context:
            assert hasattr(record, attr)
            assert getattr(record, attr) == expected_values[attr]
        # This is not a worker process, so actor_id and task_id should not exist.
        for attr in ["actor_id", "task_id"]:
            assert not hasattr(record, attr)
        assert hasattr(record, "_ray_timestamp_ns")

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
                "task_name": runtime_context.get_task_name(),
                "task_func_name": runtime_context.get_task_function_name(),
            }
            for attr in should_exist:
                assert hasattr(record, attr)
                assert getattr(record, attr) == expected_values[attr]
            assert not hasattr(record, "actor_id")
            assert not hasattr(record, "actor_name")
            assert hasattr(record, "_ray_timestamp_ns")

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
                    "actor_name": runtime_context.get_actor_name(),
                    "task_id": runtime_context.get_task_id(),
                    "task_name": runtime_context.get_task_name(),
                    "task_func_name": runtime_context.get_task_function_name(),
                }
                for attr in should_exist:
                    assert hasattr(record, attr)
                    assert getattr(record, attr) == expected_values[attr]
                assert hasattr(record, "_ray_timestamp_ns")

        actor = A.remote()
        ray.get(actor.f.remote())


class TestJSONFormatter:
    def test_empty_record(self, shutdown_only):
        formatter = JSONFormatter()
        record = logging.makeLogRecord({})
        formatted = formatter.format(record)

        record_dict = json.loads(formatted)
        should_exist = [
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


def test_invalid_encoding():
    with pytest.raises(ValueError):
        LoggingConfig(encoding="INVALID")


def test_invalid_additional_log_standard_attrs():
    with pytest.raises(ValueError):
        LoggingConfig(additional_log_standard_attrs=["invalid"])


class TestTextModeE2E:
    def test_text_mode_task(self, shutdown_only):
        script = """
import ray
import logging

ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT", additional_log_standard_attrs=["name"])
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
            "timestamp_ns",
            "job_id",
            "worker_id",
            "node_id",
            "task_id",
            "INFO",
            "This is a Ray task",
            "name=",
        ]
        for s in should_exist:
            assert s in stderr
        assert "actor_id" not in stderr

    def test_text_mode_actor(self, shutdown_only):
        script = """
import ray
import logging

ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT", additional_log_standard_attrs=["name"])
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
            "timestamp_ns",
            "job_id",
            "worker_id",
            "node_id",
            "actor_id",
            "task_id",
            "INFO",
            "This is a Ray actor",
            "name=",
        ]
        for s in should_exist:
            assert s in stderr

    def test_text_mode_driver(self, shutdown_only):
        script = """
import ray
import logging

ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT", additional_log_standard_attrs=["name"])
)

logger = logging.getLogger()
logger.info("This is a Ray driver")
"""
        stderr = run_string_as_driver(script)
        should_exist = [
            "timestamp_ns",
            "job_id",
            "worker_id",
            "node_id",
            "INFO",
            "This is a Ray driver",
            "name=",
        ]
        for s in should_exist:
            assert s in stderr

        should_not_exist = ["actor_id", "task_id"]
        for s in should_not_exist:
            assert s not in stderr

    @pytest.mark.parametrize(
        "ray_start_cluster_head_with_env_vars",
        [
            {
                "env_vars": {
                    "RAY_LOGGING_CONFIG_ENCODING": "TEXT",
                },
            }
        ],
        indirect=True,
    )
    def test_env_setup_logger_encoding(
        self, ray_start_cluster_head_with_env_vars, shutdown_only
    ):
        script = """
import ray
import logging

ray.init()

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
            "timestamp_ns",
        ]
        for s in should_exist:
            assert s in stderr

    def test_logger_not_set(self, shutdown_only):
        script = """
import ray
import logging

ray.init()

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
        should_not_exist = [
            "job_id",
            "worker_id",
            "node_id",
            "actor_id",
            "task_id",
            "This is a Ray actor",
            "timestamp_ns",
        ]
        for s in should_not_exist:
            assert s not in stderr

    def test_text_mode_driver_system_log(self, shutdown_only):
        script = """
import ray
ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT")
)
"""
        stderr = run_string_as_driver(script)
        should_exist = "timestamp_ns="
        assert should_exist in stderr


def test_structured_logging_with_working_dir(tmp_path, shutdown_only):
    working_dir = tmp_path / "test-working-dir"
    working_dir.mkdir()
    runtime_env = {
        "working_dir": str(working_dir),
    }
    ray.init(
        runtime_env=runtime_env,
        logging_config=ray.LoggingConfig(encoding="TEXT"),
    )


def test_text_mode_no_prefix(shutdown_only):
    """
    If logging_config is set, remove the prefix that contains
    the actor or task's name and their PIDs.
    """
    script = """
import ray
import logging
ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT")
)
@ray.remote
class MyActor:
    def print_message(self):
        logger = logging.getLogger(__name__)
        logger.info("This is a Ray actor")
my_actor = MyActor.remote()
ray.get(my_actor.print_message.remote())
"""
    stderr = run_string_as_driver(script)
    assert "This is a Ray actor" in stderr
    assert "(MyActor pid=" not in stderr


def test_configure_both_structured_logging_and_lib_logging(shutdown_only):
    """
    Configure the `ray.test` logger. Then, configure the `root` and `ray`
    loggers in `ray.init()`. Ensure that the `ray.test` logger is not affected.
    """
    script = """
import ray
import logging

old_test_logger = logging.getLogger("ray.test")
assert old_test_logger.getEffectiveLevel() != logging.DEBUG
old_test_logger.setLevel(logging.DEBUG)

ray.init(logging_config=ray.LoggingConfig(encoding="TEXT", log_level="INFO"))

new_test_logger = logging.getLogger("ray.test")
assert old_test_logger.getEffectiveLevel() == logging.DEBUG
"""
    run_string_as_driver(script)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
