from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune.variant_generator import generate_trials
from ray.tune.result import DEFAULT_RESULTS_DIR


class Experiment(object):
    """Tracks experiment specifications.

    Parameters:
        name (str): Name of experiment.
        run (str): The algorithm or model to train. This may refer to the
            name of a built-on algorithm (e.g. RLLib's DQN or PPO), or a
            user-defined trainable function or class
            registered in the tune registry.
        stop (dict): The stopping criteria. The keys may be any field in
            TrainingResult, whichever is reached first.  Defaults to
            empty dict.
        config (dict): Algorithm-specific configuration
            (e.g. env, hyperparams). Defaults to empty dict.
        resources (dict): Machine resources to allocate per trial,
            e.g. ``{"cpu": 64, "gpu": 8}``. Note that GPUs will not be
            assigned unless you specify them here. Defaults to 1 CPU and 0
            GPUs.
        repeat (int): Number of times to repeat each trial. Defaults to 1.
        local_dir (str): Local dir to save training results to.
            Defaults to ``~/ray_results``.
        upload_dir (str): Optional URI to sync training results
            to (e.g. ``s3://bucket``).
        checkpoint_freq (int): How many training iterations between
            checkpoints. A value of 0 (default) disables checkpointing.
        max_failures (int): Try to recover a trial from its last
            checkpoint at least this many times. Only applies if
            checkpointing is enabled. Defaults to 3.
    """
    def __init__(self, name, run, stop=None, config=None,
                 resources=None, repeat=1, local_dir=None,
                 upload_dir="", checkpoint_freq=0, max_failures=3):
        spec = {
            "run": run,
            "stop": stop or {},
            "config": config or {},
            "resources": resources or {"cpu": 1, "gpu": 0},
            "repeat": repeat,
            "local_dir": local_dir or DEFAULT_RESULTS_DIR,
            "upload_dir": upload_dir,
            "checkpoint_freq": checkpoint_freq,
            "max_failures": max_failures
        }

        self.name = name
        self.spec = spec
        self._initialize_generator(spec, name)

    def _initialize_generator(self, spec, name):
        """Creates a trial generator. Can be overwritten by subclass."""
        self.trial_generator = generate_trials(spec, name)

    def on_trial_stop(self, trial, error=False):
        """Hook for when the trial is completely stopped.

        For completed trials, this is called after `on_trial_complete`.
        """
        pass

    def on_trial_complete(self, trial):
        """Hook for when trial is completed.

        Note that this is only for trials that reach the stopping
        criteria of the experiment or the scheduler."""
        pass

    def ready(self):
        """Whether there are trials ready to be queued."""
        return True

    def next_trial(self):
        """Getting the next trial.

        Only called if self.ready() is True."""
        return next(self.trial_generator)


class JSONExperiment(Experiment):
    """Tracks experiment specifications given JSON."""
    def __init__(self, name, spec):
        self.name = name
        self.spec = spec
        self.trial_generator = generate_trials(spec, name)
