import os
import time
from pathlib import Path

import pytest
import ray
from ray import tune
from ray.air import RunConfig, Checkpoint, session, FailureConfig
from ray.tune import Callback
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=4, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def _train_fn_sometimes_failing(config, checkpoint_dir=None):
    # Fails if config["failing"] is set and marker file exists
    if checkpoint_dir:
        state = Checkpoint.from_directory(checkpoint_dir).to_dict()
    else:
        state = {"it": 0}

    state["it"] += 1

    session.report(state, checkpoint=Checkpoint.from_dict(state))

    if config.get("sleeping"):
        time.sleep(config["sleeping"])

    # We fail after reporting one checkpoint.
    if config["failing"] is True and config["marker"].exists():
        after_sleep_marker = config.get("after_sleep_marker")
        if after_sleep_marker:
            after_sleep_marker.write_text("", encoding="utf-8")
        raise RuntimeError("I am failing")

    state["it"] += 1
    session.report(state, checkpoint=Checkpoint.from_dict(state))


class _FailOnMarker(Callback):
    def __init__(self, marker: Path):
        self.marker = marker
        self.fail_at = None

    def on_step_begin(self, iteration: int, trials: list, **info):
        if self.marker.exists():
            self.fail_at = iteration + 4
        if iteration >= self.fail_at:
            raise RuntimeError("Failing")


def test_tuner_restore_num_trials(ray_start_4_cpus, tmpdir):
    tuner = Tuner(
        lambda config: 1,
        tune_config=TuneConfig(num_samples=4),
        run_config=RunConfig(
            name="test_tuner_restore_num_trials", local_dir=str(tmpdir)
        ),
    )
    tuner.fit()

    del tuner
    tuner = Tuner.restore(str(tmpdir / "test_tuner_restore_num_trials"))
    results = tuner.fit()
    assert len(results) == 4


def test_tuner_restore_resume_errored(ray_start_4_cpus, tmpdir):
    marker = tmpdir / "marker"
    marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(
            num_samples=1,
        ),
        run_config=RunConfig(
            name="test_tuner_restore_resume_errored", local_dir=str(tmpdir)
        ),
        param_space={
            "failing": tune.grid_search([False, True, False, True]),
            "marker": marker,
        },
    )
    results = tuner.fit()

    assert len(results) == 4
    assert len(results.errors) == 2
    assert [r.metrics["it"] for r in results] == [2, 1, 2, 1]

    del tuner
    marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_restore_resume_errored"), resume_errored=True
    )
    results = tuner.fit()
    assert len(results) == 4
    assert len(results.errors) == 0
    # Since the errored trials are being resumed and then report two more times,
    # we should observe 3 here.
    assert sorted([r.metrics["it"] for r in results]) == sorted([2, 3, 2, 3])


def test_tuner_restore_restart_errored(ray_start_4_cpus, tmpdir):
    marker = tmpdir / "marker"
    marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            name="test_tuner_restore_restart_errored",
            local_dir=str(tmpdir),
        ),
        param_space={
            "failing": tune.grid_search([False, True, False, True]),
            "marker": marker,
        },
    )
    results = tuner.fit()

    assert len(results) == 4
    assert len(results.errors) == 2
    assert [r.metrics["it"] for r in results] == [2, 1, 2, 1]

    del tuner
    marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_restore_restart_errored"), restart_errored=True
    )
    results = tuner.fit()
    assert len(results) == 4
    assert len(results.errors) == 0
    # Since the errored trials are being restarted from scratch, they should report 2
    assert [r.metrics["it"] for r in results] == [2, 2, 2, 2]


def test_tuner_resume_unfinished(ray_start_2_cpus, tmpdir):
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"
    marker = tmpdir / "marker"
    marker.write_text("", encoding="utf-8")

    fail_marker = tmpdir / "fail_marker"

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            name="test_tuner_resume_unfinished",
            local_dir=str(tmpdir),
            failure_config=FailureConfig(fail_fast=True),
            callbacks=[_FailOnMarker(fail_marker)],
        ),
        param_space={
            "failing": tune.grid_search([False, True]),
            "sleeping": tune.grid_search([1, 10]),
            "marker": marker,
            "after_sleep_marker": fail_marker,
        },
    )
    with pytest.raises(tune.TuneError):
        tuner.fit()

    del tuner
    marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_resume_unfinished"), resume_unfinished=True
    )
    tuner._local_tuner._run_config.callbacks = None

    results = tuner.fit()
    assert len(results) == 4
    assert len(results.errors) == 1
    assert sorted([r.metrics["it"] for r in results]) == sorted([2, 1, 3, 3])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
