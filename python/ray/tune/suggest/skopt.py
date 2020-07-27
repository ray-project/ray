import logging
import pickle
try:
    import skopt as sko
except ImportError:
    sko = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


def _validate_warmstart(parameter_names, points_to_evaluate,
                        evaluated_rewards):
    if points_to_evaluate:
        if not isinstance(points_to_evaluate, list):
            raise TypeError(
                "points_to_evaluate expected to be a list, got {}.".format(
                    type(points_to_evaluate)))
        for point in points_to_evaluate:
            if not isinstance(point, list):
                raise TypeError(
                    "points_to_evaluate expected to include list, got {}.".
                    format(point))

            if not len(point) == len(parameter_names):
                raise ValueError("Dim of point {}".format(point) +
                                 " and parameter_names {}".format(
                                     parameter_names) + " do not match.")

    if points_to_evaluate and evaluated_rewards:
        if not isinstance(evaluated_rewards, list):
            raise TypeError(
                "evaluated_rewards expected to be a list, got {}.".format(
                    type(evaluated_rewards)))
        if not len(evaluated_rewards) == len(points_to_evaluate):
            raise ValueError(
                "Dim of evaluated_rewards {}".format(evaluated_rewards) +
                " and points_to_evaluate {}".format(points_to_evaluate) +
                " do not match.")


class SkOptSearch(Searcher):
    """Uses Scikit Optimize (skopt) to optimize hyperparameters.

    Scikit-optimize is a black-box optimization library.
    Read more here: https://scikit-optimize.github.io.

    You will need to install Scikit-Optimize to use this module.

    .. code-block:: bash

        pip install scikit-optimize


    This Search Algorithm requires you to pass in a `skopt Optimizer object`_.

    Parameters:
        optimizer (skopt.optimizer.Optimizer): Optimizer provided
            from skopt.
        parameter_names (list): List of parameter names. Should match
            the dimension of the optimizer output.
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
            points_to_evaluate. (See tune/examples/skopt_example.py)
        max_concurrent: Deprecated.
        use_early_stopped_trials: Deprecated.

    Example:

    .. code-block:: python

        from skopt import Optimizer
        optimizer = Optimizer([(0,20),(-100,100)])
        current_best_params = [[10, 0], [15, -20]]

        algo = SkOptSearch(optimizer,
            ["width", "height"],
            metric="mean_loss",
            mode="min",
            points_to_evaluate=current_best_params)
    """

    def __init__(self,
                 optimizer,
                 parameter_names,
                 metric="episode_reward_mean",
                 mode="max",
                 points_to_evaluate=None,
                 evaluated_rewards=None,
                 max_concurrent=None,
                 use_early_stopped_trials=None):
        assert sko is not None, """skopt must be installed!
            You can install Skopt with the command:
            `pip install scikit-optimize`."""
        _validate_warmstart(parameter_names, points_to_evaluate,
                            evaluated_rewards)
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self.max_concurrent = max_concurrent
        super(SkOptSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)

        self._initial_points = []
        if points_to_evaluate and evaluated_rewards:
            optimizer.tell(points_to_evaluate, evaluated_rewards)
        elif points_to_evaluate:
            self._initial_points = points_to_evaluate
        self._parameters = parameter_names
        # Skopt internally minimizes, so "max" => -1
        if mode == "max":
            self._metric_op = -1.
        elif mode == "min":
            self._metric_op = 1.
        self._skopt_opt = optimizer
        self._live_trial_mapping = {}

    def suggest(self, trial_id):
        if self.max_concurrent:
            if len(self._live_trial_mapping) >= self.max_concurrent:
                return None
        if self._initial_points:
            suggested_config = self._initial_points[0]
            del self._initial_points[0]
        else:
            suggested_config = self._skopt_opt.ask()
        self._live_trial_mapping[trial_id] = suggested_config
        return dict(zip(self._parameters, suggested_config))

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        The result is internally negated when interacting with Skopt
        so that Skopt Optimizers can "maximize" this value,
        as it minimizes on default.
        """

        if result:
            self._process_result(trial_id, result)
        self._live_trial_mapping.pop(trial_id)

    def _process_result(self, trial_id, result):
        skopt_trial_info = self._live_trial_mapping[trial_id]
        self._skopt_opt.tell(skopt_trial_info,
                             self._metric_op * result[self._metric])

    def save(self, checkpoint_dir):
        trials_object = (self._initial_points, self._skopt_opt)
        with open(checkpoint_dir, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self._initial_points = trials_object[0]
        self._skopt_opt = trials_object[1]
