from collections import defaultdict
import random
import os

import pytest

import ray
from ray import serve
from ray.serve.config import BackendConfig
from ray.serve.long_poll import LongPollNamespace

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False) == 1:
    serve.controller._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture(scope="session")
def _shared_serve_instance():
    # Note(simon):
    # This line should be not turned on on master because it leads to very
    # spammy and not useful log in case of a failure in CI.
    # To run locally, please use this instead.
    # SERVE_LOG_DEBUG=1 pytest -v -s test_api.py
    # os.environ["SERVE_LOG_DEBUG"] = "1" <- Do not uncomment this.

    # Overriding task_retry_delay_ms to relaunch actors more quickly
    ray.init(
        num_cpus=36,
        _metrics_export_port=9999,
        _system_config={
            "metrics_report_interval_ms": 1000,
            "task_retry_delay_ms": 50
        })
    yield serve.start(detached=True)


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    controller = serve.api._global_client._controller
    # Clear all state between tests to avoid naming collisions.
    for endpoint in ray.get(controller.get_all_endpoints.remote()):
        serve.delete_endpoint(endpoint)
    for backend in ray.get(controller.get_all_backends.remote()).keys():
        serve.delete_backend(backend, force=True)


@pytest.fixture
def mock_controller_with_name():
    @ray.remote(num_cpus=0)
    class MockControllerActor:
        def __init__(self):
            from ray.serve.long_poll import LongPollHost
            self.host = LongPollHost()
            self.backend_replicas = defaultdict(list)
            self.backend_configs = dict()

        async def listen_for_change(self, snapshot_ids):
            return await self.host.listen_for_change(snapshot_ids)

        def set_traffic(self, endpoint, traffic_policy):
            self.host.notify_changed(
                (LongPollNamespace.TRAFFIC_POLICIES, endpoint), traffic_policy)

        def add_new_replica(self,
                            backend_tag,
                            runner_actor,
                            backend_config=BackendConfig()):
            self.backend_replicas[backend_tag].append(runner_actor)
            self.backend_configs[backend_tag] = backend_config

            self.host.notify_changed(
                (LongPollNamespace.REPLICA_HANDLES, backend_tag),
                self.backend_replicas[backend_tag],
            )
            self.host.notify_changed(
                (LongPollNamespace.BACKEND_CONFIGS, backend_tag),
                self.backend_configs[backend_tag],
            )

        def update_backend(self, backend_tag: str,
                           backend_config: BackendConfig):
            self.backend_configs[backend_tag] = backend_config
            self.host.notify_changed(
                (LongPollNamespace.BACKEND_CONFIGS, backend_tag),
                self.backend_configs[backend_tag],
            )

    name = f"MockController{random.randint(0,10e4)}"
    yield name, MockControllerActor.options(name=name).remote()


@pytest.fixture
def mock_controller(mock_controller_with_name):
    yield mock_controller_with_name[1]
