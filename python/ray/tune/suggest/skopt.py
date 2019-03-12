from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import numbers
from collections import Iterable

try:
    import skopt
except Exception:
    skopt = None

from ray.tune.suggest.suggestion import SuggestionAlgorithm


class SkOptSearch(SuggestionAlgorithm):
    """A wrapper around skopt to provide trial suggestions.

    Requires skopt to be installed.

    Parameters:
        optimizer (skopt.optimizer.Optimizer): Optimizer provided
            from skopt.
        parameter_names (list): List of parameter names. Should match
            the dimension of the optimizer output.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.
        points_to_evaluate (list of lists): A list of trials you'd like to run
            first before sampling from the optimiser, e.g. these could be
            parameter configurations you already know work well to help
            the optimiser select good values. Each trial is a list of the
            parameters of that trial using the order definition given
            to the optimiser (see example below)
        evaluated_rewards (list): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate. (See skopt_example.py)

    Example:
        >>> from skopt import Optimizer
        >>> optimizer = Optimizer([(0,20),(-100,100)])
        >>> current_best_params = [[10, 0], [15, -20]]
        >>> config = {
        >>>     "my_exp": {
        >>>         "run": "exp",
        >>>         "num_samples": 10,
        >>>         "stop": {
        >>>             "training_iteration": 100
        >>>         },
        >>>     }
        >>> }
        >>> algo = SkOptSearch(optimizer,
        >>>     ["width", "height"], max_concurrent=4,
        >>>     reward_attr="neg_mean_loss", points_to_evaluate=current_best_params)
    """

    def __init__(self,
                 optimizer,
                 parameter_names,
                 max_concurrent=10,
                 reward_attr="episode_reward_mean",
                 points_to_evaluate=None,
                 evaluated_rewards=None,
                 **kwargs):
        assert skopt is not None, """skopt must be installed!
            You can install Skopt with the command:
            `pip install scikit-optimize`."""
        assert type(max_concurrent) is int and max_concurrent > 0
        if points_to_evaluate is None:
            points_to_evaluate = []
        elif not isinstance(points_to_evaluate[0], (list, tuple)):
            points_to_evaluate = [points_to_evaluate]
        if not isinstance(points_to_evaluate, list):
            raise ValueError(
                "`points_to_evaluate` should be a list, but got %s" %
                type(points_to_evaluate))
        if isinstance(evaluated_rewards, Iterable):
            evaluated_rewards = list(evaluated_rewards)
        elif isinstance(evaluated_rewards, numbers.Number):
            evaluated_rewards = [evaluated_rewards]
        self._initial_points = []
        if points_to_evaluate and evaluated_rewards:
            if not (isinstance(evaluated_rewards, Iterable)
                    or isinstance(evaluated_rewards, numbers.Number)):
                raise ValueError(
                    "`evaluated_rewards` should be an iterable or a scalar, got %s"
                    % type(evaluated_rewards))
            if len(points_to_evaluate) != len(evaluated_rewards):
                raise ValueError(
                    "`points_to_evaluate` and `evaluated_rewards` should have the same length"
                )
            optimizer.tell(points_to_evaluate, evaluated_rewards)
        elif points_to_evaluate:
            self._initial_points = points_to_evaluate
        self._max_concurrent = max_concurrent
        self._parameters = parameter_names
        self._reward_attr = reward_attr
        self._skopt_opt = optimizer
        self._live_trial_mapping = {}
        super(SkOptSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        if self._initial_points:
            suggested_config = self._initial_points[0]
            del self._initial_points[0]
        else:
            suggested_config = self._skopt_opt.ask()
        self._live_trial_mapping[trial_id] = suggested_config
        return dict(zip(self._parameters, suggested_config))

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Passes the result to skopt unless early terminated or errored.

        The result is internally negated when interacting with Skopt
        so that Skopt Optimizers can "maximize" this value,
        as it minimizes on default.
        """
        skopt_trial_info = self._live_trial_mapping.pop(trial_id)
        if result:
            self._skopt_opt.tell(skopt_trial_info, -result[self._reward_attr])

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
