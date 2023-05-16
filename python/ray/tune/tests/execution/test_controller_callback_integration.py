from typing import Dict, Optional

import pytest
import sys

import ray
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.tune import Callback
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


class StatefulCallback(Callback):
    CKPT_FILE_TMPL = "test-callback-state-{}.json"

    def __init__(self):
        self.counter = 0

    def on_trial_result(self, iteration, trials, trial, result, **info):
        self.counter += 1

    def get_state(self) -> Optional[Dict]:
        return {"counter": self.counter}

    def set_state(self, state: Dict):
        self.counter = state["counter"]


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_callback_save_restore(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir
):
    """Check that callback state is restored correctly.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testCallbackSaveRestore
    """
    runner = TuneController(
        callbacks=[StatefulCallback()],
        experiment_path=str(tmpdir),
    )
    runner.add_trial(Trial("__fake", stub=True))
    for i in range(3):
        runner._callbacks.on_trial_result(
            iteration=i, trials=None, trial=None, result=None
        )
    runner.checkpoint(force=True)
    callback = StatefulCallback()
    runner2 = TuneController(
        callbacks=[callback],
        experiment_path=str(tmpdir),
    )
    assert callback.counter == 0
    runner2.resume()
    assert callback.counter == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
