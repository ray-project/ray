from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import pickle

try:  # Python 3 only -- needed for lint test.
    import dragonfly
except ImportError:
    dragonfly = None

from ray.tune.suggest.suggestion import Searcher

logger = logging.getLogger(__name__)


class DragonflySearch(Searcher):
    """Uses Dragonfly to optimize hyperparameters.

    Dragonfly provides an array of tools to scale up Bayesian optimisation to
    expensive large scale problems, including high dimensional optimisation.
    parallel evaluations in synchronous or asynchronous settings,
    multi-fidelity optimisation (using cheap approximations to speed up the
    optimisation process), and multi-objective optimisation. For more info:

    * Dragonfly Website: https://github.com/dragonfly/dragonfly
    * Dragonfly Documentation: https://dragonfly-opt.readthedocs.io/

    To use this search algorithm, install Dragonfly:

    .. code-block:: bash

        $ pip install dragonfly-opt


    This interface requires using FunctionCallers and optimizers provided by
    Dragonfly.

    .. code-block:: python

        from ray import tune
        from dragonfly.opt.gp_bandit import EuclideanGPBandit
        from dragonfly.exd.experiment_caller import EuclideanFunctionCaller
        from dragonfly import load_config

        domain_vars = [{
            "name": "LiNO3_vol",
            "type": "float",
            "min": 0,
            "max": 7
        }, {
            "name": "Li2SO4_vol",
            "type": "float",
            "min": 0,
            "max": 7
        }, {
            "name": "NaClO4_vol",
            "type": "float",
            "min": 0,
            "max": 7
        }]

        domain_config = load_config({"domain": domain_vars})
        func_caller = EuclideanFunctionCaller(None,
            domain_config.domain.list_of_domains[0])
        optimizer = EuclideanGPBandit(func_caller, ask_tell_mode=True)

        algo = DragonflySearch(optimizer, metric="objective", mode="max")

        tune.run(my_func, algo=algo)

    Parameters:
        optimizer (dragonfly.opt.BlackboxOptimiser): Optimizer provided
            from dragonfly. Choose an optimiser that extends BlackboxOptimiser.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate (list of lists): A list of points you'd like to run
            first before sampling from the optimiser, e.g. these could be
            parameter configurations you already know work well to help
            the optimiser select good values. Each point is a list of the
            parameters using the order definition given by parameter_names.
        evaluated_rewards (list): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate.

    """

    def __init__(self,
                 optimizer,
                 metric="episode_reward_mean",
                 mode="max",
                 points_to_evaluate=None,
                 evaluated_rewards=None,
                 **kwargs):
        assert dragonfly is not None, """dragonfly must be installed!
            You can install Dragonfly with the command:
            `pip install dragonfly`."""
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        self._initial_points = []
        self._opt = optimizer
        self._opt.initialise()
        if points_to_evaluate and evaluated_rewards:
            self._opt.tell([(points_to_evaluate, evaluated_rewards)])
        elif points_to_evaluate:
            self._initial_points = points_to_evaluate
        # Dragonfly internally maximizes, so "min" => -1
        if mode == "min":
            self._metric_op = -1.
        elif mode == "max":
            self._metric_op = 1.
        self._live_trial_mapping = {}
        super(DragonflySearch, self).__init__(
            metric=metric, mode=mode, **kwargs)

    def suggest(self, trial_id):
        if self._initial_points:
            suggested_config = self._initial_points[0]
            del self._initial_points[0]
        else:
            try:
                suggested_config = self._opt.ask()
            except Exception as exc:
                logger.warning(
                    "Dragonfly errored when querying. This may be due to a "
                    "higher level of parallelism than supported. Try reducing "
                    "parallelism in the experiment: %s", str(exc))
                return None
        self._live_trial_mapping[trial_id] = suggested_config
        return {"point": suggested_config}

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Passes result to Dragonfly unless early terminated or errored."""
        trial_info = self._live_trial_mapping.pop(trial_id)
        if result:
            self._opt.tell([(trial_info,
                             self._metric_op * result[self._metric])])

    def save(self, checkpoint_dir):
        trials_object = (self._initial_points, self._opt)
        with open(checkpoint_dir, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self._initial_points = trials_object[0]
        self._opt = trials_object[1]
