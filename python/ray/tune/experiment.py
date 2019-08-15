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

    Implicitly registers the Trainable if needed.

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
                 sync_to_driver=None,
                 checkpoint_freq=0,
                 checkpoint_at_end=False,
                 keep_checkpoints_num=None,
                 checkpoint_score_attr=None,
                 export_formats=None,
                 max_failures=3,
                 restore=None,
                 repeat=None,
                 trial_resources=None,
                 custom_loggers=None,
                 sync_function=None):
        if repeat:
            _raise_deprecation_note("repeat", "num_samples", soft=False)
        if trial_resources:
            _raise_deprecation_note(
                "trial_resources", "resources_per_trial", soft=False)
        if sync_function:
            _raise_deprecation_note(
                "sync_function", "sync_to_driver", soft=False)

        config = config or {}
        run_identifier = Experiment._register_if_needed(run)
        spec = {
            "run": run_identifier,
            "stop": stop or {},
            "config": config,
            "resources_per_trial": resources_per_trial,
            "num_samples": num_samples,
            "local_dir": os.path.abspath(
                os.path.expanduser(local_dir or DEFAULT_RESULTS_DIR)),
            "upload_dir": upload_dir,
            "trial_name_creator": trial_name_creator,
            "loggers": loggers,
            "sync_to_driver": sync_to_driver,
            "checkpoint_freq": checkpoint_freq,
            "checkpoint_at_end": checkpoint_at_end,
            "keep_checkpoints_num": keep_checkpoints_num,
            "checkpoint_score_attr": checkpoint_score_attr,
            "export_formats": export_formats or [],
            "max_failures": max_failures,
            "restore": os.path.abspath(os.path.expanduser(restore))
            if restore else None
        }

        self.name = name or run_identifier
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

    @property
    def local_dir(self):
        return self.spec.get("local_dir")

    @property
    def checkpoint_dir(self):
        if self.local_dir:
            return os.path.join(self.local_dir, self.name)

    @property
    def remote_checkpoint_dir(self):
        if self.spec["upload_dir"]:
            return os.path.join(self.spec["upload_dir"], self.name)


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
