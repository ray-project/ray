import os
import tempfile

from ray.tune import Callback
from ray.tune.execution.trial_runner import TrialRunner


class TrialResultObserver(Callback):
    """Helper class to control runner.step() count."""

    def __init__(self):
        self._counter = 0
        self._last_counter = 0

    def reset(self):
        self._last_counter = self._counter

    def just_received_a_result(self):
        if self._last_counter == self._counter:
            return False
        else:
            self._last_counter = self._counter
            return True

    def on_trial_result(self, **kwargs):
        self._counter += 1


def create_tune_experiment_checkpoint(trials: list, **runner_kwargs) -> str:
    experiment_dir = tempfile.mkdtemp()
    runner_kwargs.setdefault("local_checkpoint_dir", experiment_dir)

    # Update environment
    orig_env = os.environ.copy()

    # Set to 1 to disable ray cluster resource lookup. That way we can
    # create experiment checkpoints without initializing ray.
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

    try:
        runner = TrialRunner(**runner_kwargs)

        for trial in trials:
            runner.add_trial(trial)

        runner.checkpoint(force=True)
    finally:
        os.environ.clear()
        os.environ.update(orig_env)

    return experiment_dir
