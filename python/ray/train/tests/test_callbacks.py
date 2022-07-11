import glob
import io
import json
from collections import defaultdict
from contextlib import redirect_stdout
from pathlib import Path
from typing import Dict, List

import pytest

import ray
import ray.train as train
from ray.train import Trainer
from ray.train._internal.results_preprocessors.preprocessor import (
    SequentialResultsPreprocessor,
)
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.callbacks import (
    JsonLoggerCallback,
    PrintCallback,
    TBXLoggerCallback,
    TorchTensorboardProfilerCallback,
    TrainingCallback,
)
from ray.train.callbacks.logging import (
    MLflowLoggerCallback,
    _TrainCallbackLogdirManager,
)
from ray.train.constants import (
    BASIC_AUTOFILLED_KEYS,
    DETAILED_AUTOFILLED_KEYS,
    ENABLE_DETAILED_AUTOFILLED_METRICS_ENV,
    TRAINING_ITERATION,
)

try:
    from tensorflow.python.summary.summary_iterator import summary_iterator
except ImportError:
    summary_iterator = None


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass


def test_print(ray_start_4_cpus):
    num_workers = 4

    def train_func():
        train.report(rank=train.world_rank())

    stream = io.StringIO()
    with redirect_stdout(stream):
        trainer = Trainer(TestConfig(), num_workers=num_workers)
        trainer.start()
        trainer.run(train_func, callbacks=[PrintCallback()])
        trainer.shutdown()

    output = stream.getvalue()
    results = json.loads(output)

    assert len(results) == num_workers
    for i, result in enumerate(results):
        assert set(result.keys()) == (BASIC_AUTOFILLED_KEYS | {"rank"})
        assert result["rank"] == i


@pytest.mark.parametrize("input", [None, "dir", "file"])
def test_train_callback_logdir_manager(tmp_path, input):
    default_dir = tmp_path / "default_dir"

    if input == "dir":
        input_logdir = tmp_path / "dir"
        input_logdir.mkdir(parents=True)
    elif input == "file":
        input_logdir = tmp_path / "file"
        input_logdir.touch()
    else:
        input_logdir = None

    logdir_manager = _TrainCallbackLogdirManager(input_logdir)

    if input_logdir:
        path = logdir_manager.logdir_path
        assert path == logdir_manager.logdir_path
    else:
        with pytest.raises(RuntimeError):
            path = logdir_manager.logdir_path

    if input_logdir and not Path(input_logdir).is_dir():
        with pytest.raises(FileExistsError):
            logdir_manager.setup_logdir(str(default_dir))
    else:
        path = logdir_manager.setup_logdir(str(default_dir))
        assert path == logdir_manager.logdir_path


@pytest.mark.parametrize("workers_to_log", [0, None, [0, 1]])
@pytest.mark.parametrize("detailed", [False, True])
@pytest.mark.parametrize("filename", [None, "my_own_filename.json"])
def test_json(
    monkeypatch, ray_start_4_cpus, tmp_path, workers_to_log, detailed, filename
):
    if detailed:
        monkeypatch.setenv(ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, "1")

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
            train.report(index=i)
        return 1

    if filename is None:
        # if None, use default value
        callback = JsonLoggerCallback(workers_to_log=workers_to_log)
    else:
        callback = JsonLoggerCallback(filename=filename, workers_to_log=workers_to_log)
    trainer = Trainer(config, num_workers=num_workers, logdir=str(tmp_path))
    trainer.start()
    trainer.run(train_func, callbacks=[callback])
    if filename is None:
        assert str(callback.log_path.name) == JsonLoggerCallback._default_filename
    else:
        assert str(callback.log_path.name) == filename

    with open(callback.log_path, "r") as f:
        log = json.load(f)
    print(log)
    assert len(log) == num_iters
    assert len(log[0]) == num_workers_to_log
    assert all(len(element) == len(log[0]) for element in log)
    assert all(
        all(worker["index"] == worker[TRAINING_ITERATION] - 1 for worker in element)
        for element in log
    )
    assert all(
        all(all(key in worker for key in BASIC_AUTOFILLED_KEYS) for worker in element)
        for element in log
    )
    if detailed:
        assert all(
            all(
                all(key in worker for key in DETAILED_AUTOFILLED_KEYS)
                for worker in element
            )
            for element in log
        )
    else:
        assert all(
            all(
                not any(key in worker for key in DETAILED_AUTOFILLED_KEYS)
                for worker in element
            )
            for element in log
        )


def _validate_tbx_result(events_dir):
    events_file = list(glob.glob(f"{events_dir}/events*"))[0]
    results = defaultdict(list)
    for event in summary_iterator(events_file):
        for v in event.summary.value:
            assert v.tag.startswith("ray/train")
            results[v.tag[10:]].append(v.simple_value)

    assert len(results["episode_reward_mean"]) == 3
    assert [int(res) for res in results["episode_reward_mean"]] == [4, 5, 6]
    assert len(results["score"]) == 1
    assert len(results["hello/world"]) == 1


def test_TBX(ray_start_4_cpus, tmp_path):
    config = TestConfig()

    temp_dir = tmp_path
    num_workers = 4

    def train_func():
        train.report(episode_reward_mean=4)
        train.report(episode_reward_mean=5)
        train.report(episode_reward_mean=6, score=[1, 2, 3], hello={"world": 1})
        return 1

    callback = TBXLoggerCallback(temp_dir)
    trainer = Trainer(config, num_workers=num_workers)
    trainer.start()
    trainer.run(train_func, callbacks=[callback])

    _validate_tbx_result(temp_dir)


def test_mlflow(ray_start_4_cpus, tmp_path):
    config = TestConfig()

    params = {"p1": "p1"}

    temp_dir = tmp_path
    num_workers = 4

    def train_func(config):
        train.report(episode_reward_mean=4)
        train.report(episode_reward_mean=5)
        train.report(episode_reward_mean=6)
        return 1

    callback = MLflowLoggerCallback(experiment_name="test_exp", logdir=temp_dir)
    trainer = Trainer(config, num_workers=num_workers)
    trainer.start()
    trainer.run(train_func, config=params, callbacks=[callback])

    from mlflow.tracking import MlflowClient

    client = MlflowClient(tracking_uri=callback.mlflow_util._mlflow.get_tracking_uri())

    experiment_id = client.get_experiment_by_name("test_exp").experiment_id
    all_runs = callback.mlflow_util._mlflow.search_runs(experiment_ids=[experiment_id])
    assert len(all_runs) == 1
    # all_runs is a pandas dataframe.
    all_runs = all_runs.to_dict(orient="records")
    run_id = all_runs[0]["run_id"]
    run = client.get_run(run_id)

    assert run.data.params == params
    assert (
        "episode_reward_mean" in run.data.metrics
        and run.data.metrics["episode_reward_mean"] == 6.0
    )
    assert (
        TRAINING_ITERATION in run.data.metrics
        and run.data.metrics[TRAINING_ITERATION] == 3.0
    )

    metric_history = client.get_metric_history(run_id=run_id, key="episode_reward_mean")

    assert len(metric_history) == 3
    iterations = [metric.step for metric in metric_history]
    assert iterations == [1, 2, 3]
    rewards = [metric.value for metric in metric_history]
    assert rewards == [4, 5, 6]


def test_torch_tensorboard_profiler_callback(ray_start_4_cpus, tmp_path):
    config = TestConfig()

    temp_dir = tmp_path
    num_workers = 4
    num_epochs = 2

    def train_func():
        from torch.profiler import profile, record_function, schedule

        from ray.train.torch import TorchWorkerProfiler

        twp = TorchWorkerProfiler()
        with profile(
            activities=[],
            schedule=schedule(wait=0, warmup=0, active=1),
            on_trace_ready=twp.trace_handler,
        ) as p:

            for epoch in range(num_epochs):
                with record_function("test_function"):
                    pass

                p.step()

                profile_results = twp.get_and_clear_profile_traces()
                train.report(epoch=epoch, **profile_results)

    callback = TorchTensorboardProfilerCallback(temp_dir)
    trainer = Trainer(config, num_workers=num_workers)
    trainer.start()
    trainer.run(train_func, callbacks=[callback])

    assert temp_dir.exists()

    count = 0
    for path in temp_dir.iterdir():
        assert path.is_file()
        count += 1
    assert count == num_workers * num_epochs


# fix issue: repeat assignments for preprocessor results nested recursive calling
# see https://github.com/ray-project/ray/issues/25005
def test_hotfix_callback_nested_recusive_calling():
    # test callback used to simulate the nested recursive calling for preprocess()
    class TestCallback(TrainingCallback):
        def __init__(self):
            self.max_process_time = 0

        def count_process_times(self, processor):
            count = 0
            if processor:
                if isinstance(processor, SequentialResultsPreprocessor):
                    for preprocessor in processor.preprocessors:
                        # recursive calling preprocessors in list
                        count += self.count_process_times(preprocessor)
                else:
                    count = 1
            return count

        def handle_result(self, results: List[Dict], **info):
            process_times = self.count_process_times(self.results_preprocessor)
            if process_times > self.max_process_time:
                self.max_process_time = process_times
            print(f"process times: {process_times}")

    def train_func():
        for idx in range(num_iterates):
            train.report(iterate=idx + 1)

    # python default limitation for iterate depth
    num_iterates = 1000
    trainer = Trainer(TestConfig(), num_workers=1)
    trainer.start()
    test_callback = TestCallback()
    trainer.run(train_func, callbacks=[test_callback])
    assert test_callback.max_process_time == 1
    print(f"callback max process time: {test_callback.max_process_time}")
    trainer.shutdown()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
