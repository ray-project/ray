import os
import pytest
import sys
import time

import ray
from ray import tune, logger
from ray.tune import Trainable, run_experiments, register_trainable
from ray.tune.error import TuneError
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler


@pytest.fixture
def ray_start_1_cpu():
    address_info = ray.init(num_cpus=1)
    os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"
    yield address_info
    ray.shutdown()
    os.environ.pop("TUNE_STATE_REFRESH_PERIOD", None)


@pytest.fixture
def ray_start_4_cpus_extra():
    address_info = ray.init(num_cpus=4, resources={"extra": 4})
    yield address_info
    ray.shutdown()


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.PAUSE


class MyResettableClass(Trainable):
    def setup(self, config):
        self.config = config
        self.num_resets = 0
        self.iter = 0
        self.msg = config.get("message", None)
        self.sleep = int(config.get("sleep", 0))
        self.fail = config.get("fail", False)

    def step(self):
        self.iter += 1

        if self.msg:
            print("PRINT_STDOUT: {}".format(self.msg))
            print("PRINT_STDERR: {}".format(self.msg), file=sys.stderr)
            logger.info("LOG_STDERR: {}".format(self.msg))

        if self.fail:
            raise RuntimeError("Failing")

        if self.sleep:
            time.sleep(self.sleep)

        return {
            "id": self.config.get("id", 0),
            "num_resets": self.num_resets,
            "done": self.iter > 1,
            "iter": self.iter,
        }

    def save_checkpoint(self, chkpt_dir):
        return {"iter": self.iter}

    def load_checkpoint(self, item):
        self.iter = item["iter"]

    def reset_config(self, new_config):
        if "fake_reset_not_supported" in self.config:
            return False
        self.num_resets += 1
        self.iter = 0
        self.msg = new_config.get("message", None)
        self.fail = new_config.get("fail", False)
        return True

    @classmethod
    def default_resource_request(cls, config):
        required_resources = config.get("required_resources", None)
        if required_resources:
            return required_resources
        return None


def _run_trials_with_frequent_pauses(trainable, reuse=False):
    analysis = tune.run(
        trainable,
        num_samples=1,
        config={"id": tune.grid_search([0, 1, 2, 3])},
        reuse_actors=reuse,
        scheduler=FrequentPausesScheduler(),
        verbose=0,
    )
    return analysis.trials


def test_trial_reuse_disabled(ray_start_1_cpu):
    """Test that reuse=False disables actor re-use.

    Setup: Pass `reuse_actors=False` to tune.run()

    We assert the `num_resets` of each trainable class to be 0 (no reuse).
    """
    trials = _run_trials_with_frequent_pauses(MyResettableClass, reuse=False)
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [0, 0, 0, 0]


def test_trial_reuse_disabled_per_default(ray_start_1_cpu):
    """Test that reuse=None disables actor re-use for class trainables.

    Setup: Pass `reuse_actors=None` to tune.run()

    We assert the `num_resets` of each trainable class to be 0 (no reuse).
    """
    trials = _run_trials_with_frequent_pauses(MyResettableClass, reuse=None)
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [0, 0, 0, 0]


def test_trial_reuse_enabled(ray_start_1_cpu):
    """Test that reuse=True enables actor re-use.

    Setup: Pass `reuse_actors=True` to tune.run()

    We assert the `num_resets` of each trainable class to be 4, 5, 6, and 7,
    respectively:

    - Each trial runs for 2 iterations
    - Only one trial can run at a time
    - After each iteration, trials are paused and actors cached for reuse
    - Thus, the first trial finishes after 4 resets, the second after 5, etc.
    """
    trials = _run_trials_with_frequent_pauses(MyResettableClass, reuse=True)
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [4, 5, 6, 7]


def test_trial_reuse_with_failing(ray_start_1_cpu):
    """Test that failing actors won't be reused.

    - 1 trial can run at a time
    - Some trials are failing
    - We assert that trials after failing trials are scheduled on fresh actors
        (num_resets = 0)
    - We assert that trials after successful trials are schedule on reused actors
        (num_reset = last_num_resets + 1)
    """
    trials = tune.run(
        MyResettableClass,
        reuse_actors=True,
        config={
            "fail": tune.grid_search(
                [False, True, False, False, True, True, False, False, False]
            )
        },
        raise_on_failed_trial=False,
    ).trials

    assert [t.last_result.get("iter") for t in trials] == [
        2,
        None,
        2,
        2,
        None,
        None,
        2,
        2,
        2,
    ]
    assert [t.last_result.get("num_resets") for t in trials] == [
        0,
        None,
        0,
        1,
        None,
        None,
        0,
        1,
        2,
    ]


def test_reuse_enabled_error(ray_start_1_cpu):
    """Test that a class without reset() enabled throws an error on actor reuse."""
    with pytest.raises(TuneError):
        run_experiments(
            {
                "foo": {
                    "run": MyResettableClass,
                    "max_failures": 1,
                    "num_samples": 1,
                    "config": {
                        "id": tune.grid_search([0, 1, 2, 3]),
                        "fake_reset_not_supported": True,
                    },
                }
            },
            reuse_actors=True,
            scheduler=FrequentPausesScheduler(),
        )


def test_trial_reuse_log_to_file(ray_start_1_cpu):
    """Check that log outputs from trainables are correctly stored with actor reuse.

    We run two trials with actor reuse. When the actor is reused, we expect
    the log output to be written to the log file of the new trial - i.e. we expect
    that the old trial logfile is closed and a new one is open.
    """
    register_trainable("foo2", MyResettableClass)

    # Log to default files
    [trial1, trial2] = tune.run(
        "foo2",
        config={"message": tune.grid_search(["First", "Second"]), "id": -1},
        log_to_file=True,
        scheduler=FrequentPausesScheduler(),
        reuse_actors=True,
    ).trials

    # Check trial 1
    assert trial1.last_result["num_resets"] == 2
    assert os.path.exists(os.path.join(trial1.logdir, "stdout"))
    assert os.path.exists(os.path.join(trial1.logdir, "stderr"))

    # We expect that only "First" output is found in the first trial output
    with open(os.path.join(trial1.logdir, "stdout"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDOUT: First" in content
        assert "PRINT_STDOUT: Second" not in content
    with open(os.path.join(trial1.logdir, "stderr"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDERR: First" in content
        assert "LOG_STDERR: First" in content
        assert "PRINT_STDERR: Second" not in content
        assert "LOG_STDERR: Second" not in content

    # Check trial 2
    assert trial2.last_result["num_resets"] == 3
    assert os.path.exists(os.path.join(trial2.logdir, "stdout"))
    assert os.path.exists(os.path.join(trial2.logdir, "stderr"))

    # We expect that only "Second" output is found in the first trial output
    with open(os.path.join(trial2.logdir, "stdout"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDOUT: Second" in content
        assert "PRINT_STDOUT: First" not in content
    with open(os.path.join(trial2.logdir, "stderr"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDERR: Second" in content
        assert "LOG_STDERR: Second" in content
        assert "PRINT_STDERR: First" not in content
        assert "LOG_STDERR: First" not in content


def test_multi_trial_reuse(ray_start_4_cpus_extra):
    """Test that actors from multiple trials running in parallel will be reused.

    - 2 trials can run at the same time
    - Trial 3 will be scheduled after trial 1 succeeded, so will reuse actor
    - Trial 4 will be scheduled after trial 2 succeeded, so will reuse actor
    """
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "2"

    register_trainable("foo2", MyResettableClass)

    # We sleep here for one second so that the third actor
    # does not finish training before the fourth can be scheduled.
    # This helps ensure that both remote runners are re-used and
    # not just one.
    [trial1, trial2, trial3, trial4] = tune.run(
        "foo2",
        config={
            "message": tune.grid_search(["First", "Second", "Third", "Fourth"]),
            "id": -1,
            "sleep": 2,
        },
        reuse_actors=True,
        resources_per_trial={"cpu": 2},
    ).trials

    assert trial3.last_result["num_resets"] == 1
    assert trial4.last_result["num_resets"] == 1


def test_multi_trial_reuse_with_failing(ray_start_4_cpus_extra):
    """Test that failing trial's actors are not reused.

    - 2 trials can run at the same time
    - Trial 1 succeeds, trial 2 fails
    - Trial 3 will be scheduled after trial 2 failed, so won't reuse actor
    - Trial 4 will be scheduled after trial 1 succeeded, so will reuse actor
    """
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "2"

    register_trainable("foo2", MyResettableClass)

    [trial1, trial2, trial3, trial4] = tune.run(
        "foo2",
        config={
            "fail": tune.grid_search([False, True, False, False]),
            "id": -1,
            "sleep": 2,
        },
        reuse_actors=True,
        resources_per_trial={"cpu": 2},
        raise_on_failed_trial=False,
    ).trials

    assert trial1.last_result["num_resets"] == 0
    assert trial3.last_result["num_resets"] == 0
    assert trial4.last_result["num_resets"] == 1


def test_multi_trial_reuse_one_by_one(ray_start_4_cpus_extra):
    """Test that we still reuse actors even if we run with concurrency = 1.

    - Run 6 trials, but only 1 concurrent at the time
    - This means there won't be any PENDING trials until the trial completed
    - We still want to reuse actors

    """
    register_trainable("foo2", MyResettableClass)

    trials = tune.run(
        "foo2",
        config={"id": -1},
        reuse_actors=True,
        num_samples=6,
        max_concurrent_trials=1,
    ).trials

    assert sorted([t.last_result["num_resets"] for t in trials]) == [0, 1, 2, 3, 4, 5]


def test_multi_trial_reuse_heterogeneous(ray_start_4_cpus_extra):
    """Test that actors with heterogeneous resource requirements are reused efficiently.

    - Run 6 trials in total
    - Only 1 trial can run at the same time
    - Trials 1 and 6, 2 and 4, and 3 and 5, have the same resource request, respectively
    - Assert that trials 4, 5, and 6 re-use their respective previous actors

    """
    # We need to set this to 6 so that all trials will be scheduled and actors will
    # be correctly cached.
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "6"

    register_trainable("foo2", MyResettableClass)

    trials = tune.run(
        "foo2",
        config={
            # The extra resources are selected so that only any 1 placement group
            # can be scheduled at the same time at all times (to force sequential
            # execution of trials)
            "required_resources": tune.grid_search(
                [
                    {"cpu": 4, "custom_resources": {"extra": 4}},
                    {"cpu": 2, "custom_resources": {"extra": 4}},
                    {"cpu": 1, "custom_resources": {"extra": 4}},
                    {"cpu": 2, "custom_resources": {"extra": 4}},
                    {"cpu": 1, "custom_resources": {"extra": 4}},
                    {"cpu": 4, "custom_resources": {"extra": 4}},
                ]
            ),
            "id": -1,
        },
        reuse_actors=True,
    ).trials

    # Actors may be re-used in a different order as the staged_trials set is unsorted
    assert sorted([t.last_result["num_resets"] for t in trials]) == [0, 0, 0, 1, 1, 1]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
