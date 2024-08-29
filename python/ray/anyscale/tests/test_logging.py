import sys

import pytest

import ray
from ray._private.test_utils import run_string_as_driver


@pytest.fixture
def shutdown_only():
    yield None
    ray.shutdown()


class TestJSONModeE2E:
    def test_json_mode_task(self, shutdown_only):
        script = """
import ray
import logging
ray.init(
    logging_config=ray.LoggingConfig(encoding="JSON")
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
            '"levelname": "INFO"',
            '"message": "This is a Ray task"',
        ]
        for s in should_exist:
            assert s in stderr
        assert "actor_id" not in stderr

    def test_json_mode_actor(self, shutdown_only):
        script = """
import ray
import logging
ray.init(
    logging_config=ray.LoggingConfig(encoding="JSON")
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
            '"levelname": "INFO"',
            '"message": "This is a Ray actor"',
        ]
        for s in should_exist:
            assert s in stderr

    def test_json_mode_driver_system_log(self, shutdown_only):
        script = """
import ray
ray.init(
    logging_config=ray.LoggingConfig(encoding="JSON")
)
"""
        stderr = run_string_as_driver(script)
        # The log is slightly different depending on whether the
        # Ray cluster exists or not.
        should_exist = [
            '"levelname": "INFO", "message": "Started a local Ray instance. '
            "View the dashboard at",
            '"levelname": "INFO", "message": "Connecting to existing '
            "Ray cluster at address:",
        ]
        exists = [s for s in should_exist if s in stderr]
        assert len(exists) == 1, f"Expected one of {should_exist} in {stderr}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
