import os
import tempfile

import pytest

import ray
from ray import serve

# TODO(edoakes): the failure tests currently fail with the GCS service enabled.
os.environ["RAY_GCS_SERVICE_ENABLED"] = "false"

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False):
    serve.master._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture(scope="session")
def serve_instance():
    _, new_db_path = tempfile.mkstemp(suffix=".test.db")
    serve.init(
        kv_store_path=new_db_path,
        blocking=True,
        ray_init_kwargs={"num_cpus": 36})
    yield
    os.remove(new_db_path)


@pytest.fixture(scope="session")
def ray_instance():
    ray_already_initialized = ray.is_initialized()
    if not ray_already_initialized:
        ray.init(object_store_memory=int(1e8))
    yield
    if not ray_already_initialized:
        ray.shutdown()
