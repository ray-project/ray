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
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.PAUSE


def create_resettable_class():
    class MyResettableClass(Trainable):
        def setup(self, config):
            self.config = config
            self.num_resets = 0
            self.iter = 0
            self.msg = config.get("message", None)
            self.sleep = int(config.get("sleep", 0))

        def step(self):
            self.iter += 1

            if self.msg:
                print("PRINT_STDOUT: {}".format(self.msg))
                print("PRINT_STDERR: {}".format(self.msg), file=sys.stderr)
                logger.info("LOG_STDERR: {}".format(self.msg))

            if self.sleep:
                time.sleep(self.sleep)

            return {
                "id": self.config["id"],
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
            self.msg = new_config.get("message", "No message")
            return True

    return MyResettableClass


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
    trials = _run_trials_with_frequent_pauses(create_resettable_class(), reuse=False)
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [0, 0, 0, 0]


def test_trial_reuse_disabled_per_default(ray_start_1_cpu):
    """Test that reuse=None disables actor re-use for class trainables.

    Setup: Pass `reuse_actors=None` to tune.run()

    We assert the `num_resets` of each trainable class to be 0 (no reuse).
    """
    trials = _run_trials_with_frequent_pauses(create_resettable_class(), reuse=None)
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
    trials = _run_trials_with_frequent_pauses(create_resettable_class(), reuse=True)
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [4, 5, 6, 7]


def test_reuse_enabled_error(ray_start_1_cpu):
    """Test that a class without reset() enabled throws an error on actor reuse."""
    with pytest.raises(TuneError):
        run_experiments(
            {
                "foo": {
                    "run": create_resettable_class(),
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
    register_trainable("foo2", create_resettable_class())

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


def test_multi_trial_reuse(ray_start_4_cpus):
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "2"

    register_trainable("foo2", create_resettable_class())

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
