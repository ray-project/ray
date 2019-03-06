from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import logging
import os
import six
import types

from ray.tune.error import TuneError
from ray.tune.registry import register_trainable
from ray.tune.result import DEFAULT_RESULTS_DIR

logger = logging.getLogger(__name__)


def _raise_deprecation_note(deprecated, replacement, soft=False):
    """User notification for deprecated parameter.

    Arguments:
        deprecated (str): Deprecated parameter.
        replacement (str): Replacement parameter to use instead.
        soft (bool): Fatal if True.
    """
    error_msg = ("`{deprecated}` is deprecated. Please use `{replacement}`. "
                 "`{deprecated}` will be removed in future versions of "
                 "Ray.".format(deprecated=deprecated, replacement=replacement))
    if soft:
        logger.warning(error_msg)
    else:
        raise DeprecationWarning(error_msg)


class Experiment(object):
    """Tracks experiment specifications.

    Parameters:
        name (str): Name of experiment.
        run (function|class|str): The algorithm or model to train.
            This may refer to the name of a built-on algorithm
            (e.g. RLLib's DQN or PPO), a user-defined trainable
            function or class, or the string identifier of a
            trainable function or class registered in the tune registry.
        stop (dict): The stopping criteria. The keys may be any field in
            the return result of 'train()', whichever is reached first.
            Defaults to empty dict.
        config (dict): Algorithm-specific configuration for Tune variant
            generation (e.g. env, hyperparams). Defaults to empty dict.
            Custom search algorithms may ignore this.
        resources_per_trial (dict): Machine resources to allocate per trial,
            e.g. ``{"cpu": 64, "gpu": 8}``. Note that GPUs will not be
            assigned unless you specify them here. Defaults to 1 CPU and 0
            GPUs in ``Trainable.default_resource_request()``.
        num_samples (int): Number of times to sample from the
            hyperparameter space. Defaults to 1. If `grid_search` is
            provided as an argument, the grid will be repeated
            `num_samples` of times.
        local_dir (str): Local dir to save training results to.
            Defaults to ``~/ray_results``.
        upload_dir (str): Optional URI to sync training results
            to (e.g. ``s3://bucket``).
        trial_name_creator (func): Optional function for generating
            the trial string representation.
        loggers (list): List of logger creators to be used with
            each Trial. If None, defaults to ray.tune.logger.DEFAULT_LOGGERS.
            See `ray/tune/logger.py`.
        sync_function (func|str): Function for syncing the local_dir to
            upload_dir. If string, then it must be a string template for
            syncer to run. If not provided, the sync command defaults
            to standard S3 or gsutil sync comamnds.
        checkpoint_freq (int): How many training iterations between
            checkpoints. A value of 0 (default) disables checkpointing.
        checkpoint_at_end (bool): Whether to checkpoint at the end of the
            experiment regardless of the checkpoint_freq. Default is False.
        export_formats (list): List of formats that exported at the end of
            the experiment. Default is None.
        max_failures (int): Try to recover a trial from its last
            checkpoint at least this many times. Only applies if
            checkpointing is enabled. Setting to -1 will lead to infinite
            recovery retries. Defaults to 3.
        restore (str): Path to checkpoint. Only makes sense to set if
            running 1 trial. Defaults to None.
        repeat: Deprecated and will be removed in future versions of
            Ray. Use `num_samples` instead.
        trial_resources: Deprecated and will be removed in future versions of
            Ray. Use `resources_per_trial` instead.
        custom_loggers: Deprecated and will be removed in future versions of
            Ray. Use `loggers` instead.


    Examples:
        >>> experiment_spec = Experiment(
        >>>     "my_experiment_name",
        >>>     my_func,
        >>>     stop={"mean_accuracy": 100},
        >>>     config={
        >>>         "alpha": tune.grid_search([0.2, 0.4, 0.6]),
        >>>         "beta": tune.grid_search([1, 2]),
        >>>     },
        >>>     resources_per_trial={
        >>>         "cpu": 1,
        >>>         "gpu": 0
        >>>     },
        >>>     num_samples=10,
        >>>     local_dir="~/ray_results",
        >>>     upload_dir="s3://your_bucket/path",
        >>>     checkpoint_freq=10,
        >>>     max_failures=2)

    """

    def __init__(self,
                 name,
                 run,
                 stop=None,
                 config=None,
                 resources_per_trial=None,
                 num_samples=1,
                 local_dir=None,
                 upload_dir=None,
                 trial_name_creator=None,
                 loggers=None,
                 custom_loggers=None,
                 sync_function=None,
                 checkpoint_freq=0,
                 checkpoint_at_end=False,
                 export_formats=None,
                 max_failures=3,
                 restore=None,
                 repeat=None,
                 trial_resources=None):
        if sync_function:
            assert upload_dir, "Need `upload_dir` if sync_function given."

        if repeat:
            _raise_deprecation_note("repeat", "num_samples", soft=False)
        if trial_resources:
            _raise_deprecation_note(
                "trial_resources", "resources_per_trial", soft=True)
            resources_per_trial = trial_resources
        if custom_loggers:
            _raise_deprecation_note("custom_loggers", "loggers", soft=False)

        spec = {
            "run": Experiment._register_if_needed(run),
            "stop": stop or {},
            "config": config or {},
            "resources_per_trial": resources_per_trial,
            "num_samples": num_samples,
            "local_dir": os.path.expanduser(local_dir or DEFAULT_RESULTS_DIR),
            "upload_dir": upload_dir or "",  # argparse converts None to "null"
            "trial_name_creator": trial_name_creator,
            "loggers": loggers,
            "sync_function": sync_function,
            "checkpoint_freq": checkpoint_freq,
            "checkpoint_at_end": checkpoint_at_end,
            "export_formats": export_formats or [],
            "max_failures": max_failures,
            "restore": restore
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

        # Special case the `env` param for RLlib by automatically
        # moving it into the `config` section.
        if "env" in spec:
            spec["config"] = spec.get("config", {})
            spec["config"]["env"] = spec["env"]
            del spec["env"]

        spec = copy.deepcopy(spec)

        run_value = spec.pop("run")
        try:
            exp = cls(name, run_value, **spec)
        except TypeError:
            raise TuneError("Improper argument from JSON: {}.".format(spec))
        return exp

    @classmethod
    def _register_if_needed(cls, run_object):
        """Registers Trainable or Function at runtime.

        Assumes already registered if run_object is a string. Does not
        register lambdas because they could be part of variant generation.
        Also, does not inspect interface of given run_object.

        Arguments:
            run_object (str|function|class): Trainable to run. If string,
                assumes it is an ID and does not modify it. Otherwise,
                returns a string corresponding to the run_object name.

        Returns:
            A string representing the trainable identifier.
        """

        if isinstance(run_object, six.string_types):
            return run_object
        elif isinstance(run_object, types.FunctionType):
            if run_object.__name__ == "<lambda>":
                logger.warning(
                    "Not auto-registering lambdas - resolving as variant.")
                return run_object
            else:
                name = run_object.__name__
                register_trainable(name, run_object)
                return name
        elif isinstance(run_object, type):
            name = run_object.__name__
            register_trainable(name, run_object)
            return name
        else:
            raise TuneError("Improper 'run' - not string nor trainable.")


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
            logger.warning("All experiments will be "
                           "using the same SearchAlgorithm.")
    else:
        raise TuneError("Invalid argument: {}".format(experiments))

    return exp_list
