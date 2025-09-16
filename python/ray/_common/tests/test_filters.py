import logging
import logging.config
import sys

import pytest

import ray
from ray._common.filters import CoreContextFilter


class TestCoreContextFilter:
    def test_driver_process(self, shutdown_only):
        log_context = ["job_id", "worker_id", "node_id"]
        filter = CoreContextFilter()
        record = logging.makeLogRecord({})
        assert filter.filter(record)
        # Ray is not initialized so no context except PID which should be available
        for attr in log_context:
            assert not hasattr(record, attr)
        # PID should be available even when Ray is not initialized
        assert hasattr(record, "process")
        assert hasattr(record, "_ray_timestamp_ns")

        ray.init()
        record = logging.makeLogRecord({})
        assert filter.filter(record)
        runtime_context = ray.get_runtime_context()
        expected_values = {
            "job_id": runtime_context.get_job_id(),
            "worker_id": runtime_context.get_worker_id(),
            "node_id": runtime_context.get_node_id(),
            "process": record.process,
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
            should_exist = ["job_id", "worker_id", "node_id", "task_id", "process"]
            runtime_context = ray.get_runtime_context()
            expected_values = {
                "job_id": runtime_context.get_job_id(),
                "worker_id": runtime_context.get_worker_id(),
                "node_id": runtime_context.get_node_id(),
                "task_id": runtime_context.get_task_id(),
                "task_name": runtime_context.get_task_name(),
                "task_func_name": runtime_context.get_task_function_name(),
                "process": record.process,
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
                should_exist = [
                    "job_id",
                    "worker_id",
                    "node_id",
                    "actor_id",
                    "task_id",
                    "process",
                ]
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
                    "process": record.process,
                }
                for attr in should_exist:
                    assert hasattr(record, attr)
                    assert getattr(record, attr) == expected_values[attr]
                assert hasattr(record, "_ray_timestamp_ns")

        actor = A.remote()
        ray.get(actor.f.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
