import sys

import pytest

import ray
from ray._private.ray_logging.logging_config import LoggingConfig
from ray._private.test_utils import run_string_as_driver


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
