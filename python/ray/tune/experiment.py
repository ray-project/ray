from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.error import TuneError


class Experiment(object):
    """Tracks experiment specifications.

    Parameters:
        name (str): Name of experiment.
        run (str): The algorithm or model to train. This may refer to the
            name of a built-on algorithm (e.g. RLLib's DQN or PPO), or a
            user-defined trainable function or class
            registered in the tune registry.
        stop (dict): The stopping criteria. The keys may be any field in
            the return result of 'train()', whichever is reached first.
            Defaults to empty dict.
        config (dict): Algorithm-specific configuration
            (e.g. env, hyperparams). Defaults to empty dict.
        trial_resources (dict): Machine resources to allocate per trial,
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

    def __init__(self,
                 name,
                 run,
                 stop=None,
                 config=None,
                 trial_resources=None,
                 repeat=1,
                 local_dir=None,
                 upload_dir="",
                 checkpoint_freq=0,
                 max_failures=3):
        spec = {
            "run": run,
            "stop": stop or {},
            "config": config or {},
            "trial_resources": trial_resources or {
                "cpu": 1,
                "gpu": 0
            },
            "repeat": repeat,
            "local_dir": local_dir or DEFAULT_RESULTS_DIR,
            "upload_dir": upload_dir,
            "checkpoint_freq": checkpoint_freq,
            "max_failures": max_failures
        }

        self.name = name
        self.spec = spec

    @classmethod
    def from_json(cls, name, spec):
        """Generates an Experiment object from JSON.

        Args:
            name (str): Name of Experiment.
            spec (dict): JSON configuration of experiment.
        """
        if "run" not in spec:
            raise TuneError("No trainable specified!")
        exp = cls(name, spec["run"])
        exp.name = name
        exp.spec = spec
        return exp


def convert_to_experiment_list(experiments):
    """Produces a list of Experiment objects.

    Converts input from dict, single experiment, or list of
    experiments to list of experiments. If input is None,
    will return an empty list.

    Arguments:
        experiments (Experiment | list | dict): Experiments to run.

    Returns:
        List of experiments.
    """
    exp_list = experiments

    # Transform list if necessary
    if experiments is None:
        exp_list = []
    elif isinstance(experiments, Experiment):
        exp_list = [experiments]
    elif type(experiments) is dict:
        exp_list = [
            Experiment.from_json(name, spec)
            for name, spec in experiments.items()
        ]

    # Validate exp_list
    if (type(exp_list) is list
            and all(isinstance(exp, Experiment) for exp in exp_list)):
        if len(exp_list) > 1:
            print("Warning: All experiments will be"
                  " using the same Search Algorithm.")
    else:
        raise TuneError("Invalid argument: {}".format(experiments))

    return exp_list
