import pytest
import os
import shutil
import tempfile
import json

import ray
import ray.util.sgd.v2 as sgd
from ray.util.sgd.v2 import Trainer
from ray.util.sgd.v2.constants import (
    TRAINING_ITERATION, DETAILED_AUTOFILLED_KEYS, BASIC_AUTOFILLED_KEYS,
    ENABLE_DETAILED_AUTOFILLED_METRICS_ENV)
from ray.util.sgd.v2.callbacks import JsonLoggerCallback
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendInterface
from ray.util.sgd.v2.worker_group import WorkerGroup


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def make_temp_dir():
    tmpdir = str(tempfile.mkdtemp())
    yield tmpdir
    # The code after the yield will run as teardown code.
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(BackendInterface):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: TestConfig):
        pass


@pytest.mark.parametrize("workers_to_log", [0, None, [0, 1]])
@pytest.mark.parametrize("detailed", [False, True])
@pytest.mark.parametrize("filename", [None, "my_own_filename.json"])
def test_json(ray_start_4_cpus, make_temp_dir, workers_to_log, detailed,
              filename):
    if detailed:
        os.environ[ENABLE_DETAILED_AUTOFILLED_METRICS_ENV] = "1"
    else:
        os.environ.pop(ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, 0)

    config = TestConfig()

    num_iters = 5
    num_workers = 4

    if workers_to_log is None:
        num_workers_to_log = num_workers
    elif isinstance(workers_to_log, int):
        num_workers_to_log = 1
    else:
        num_workers_to_log = len(workers_to_log)

    def train_func():
        for i in range(num_iters):
            sgd.report(index=i)
        return 1

    if filename is None:
        # if None, use default value
        callback = JsonLoggerCallback(
            make_temp_dir, workers_to_log=workers_to_log)
        assert str(
            callback.log_path.name) == JsonLoggerCallback._default_filename
    else:
        callback = JsonLoggerCallback(
            make_temp_dir, filename=filename, workers_to_log=workers_to_log)
        assert str(callback.log_path.name) == filename
    trainer = Trainer(config, num_workers=num_workers)
    trainer.start()
    trainer.run(train_func, callbacks=[callback])

    with open(callback.log_path, "r") as f:
        log = json.load(f)
    print(log)
    assert len(log) == num_iters
    assert len(log[0]) == num_workers_to_log
    assert all(len(element) == len(log[0]) for element in log)
    assert all(
        all(worker["index"] == worker[TRAINING_ITERATION] - 1
            for worker in element) for element in log)
    assert all(
        all(
            all(key in worker for key in BASIC_AUTOFILLED_KEYS)
            for worker in element) for element in log)
    if detailed:
        assert all(
            all(
                all(key in worker for key in DETAILED_AUTOFILLED_KEYS)
                for worker in element) for element in log)
    else:
        assert all(
            all(not any(key in worker for key in DETAILED_AUTOFILLED_KEYS)
                for worker in element) for element in log)
