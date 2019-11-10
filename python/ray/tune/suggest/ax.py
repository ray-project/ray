from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import ax
except ImportError:
    ax = None
import logging

from ray.tune.suggest.suggestion import SuggestionAlgorithm

logger = logging.getLogger(__name__)


class AxSearch(SuggestionAlgorithm):
    """A wrapper around Ax to provide trial suggestions.

    Requires Ax to be installed. Ax is an open source tool from
    Facebook for configuring and optimizing experiments. More information
    can be found in https://ax.dev/.

    Parameters:
        parameters (list[dict]): Parameters in the experiment search space.
            Required elements in the dictionaries are: "name" (name of
            this parameter, string), "type" (type of the parameter: "range",
            "fixed", or "choice", string), "bounds" for range parameters
            (list of two values, lower bound first), "values" for choice
            parameters (list of values), and "value" for fixed parameters
            (single value).
        objective_name (str): Name of the metric used as objective in this
            experiment. This metric must be present in `raw_data` argument
            to `log_data`. This metric must also be present in the dict
            reported/returned by the Trainable.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        minimize (bool): Whether this experiment represents a minimization
            problem. Defaults to False.
        parameter_constraints (list[str]): Parameter constraints, such as
            "x3 >= x4" or "x3 + x4 >= 2".
        outcome_constraints (list[str]): Outcome constraints of form
            "metric_name >= bound", like "m1 <= 3."
        use_early_stopped_trials (bool): Whether to use early terminated
            trial results in the optimization process.


    Example:
        >>> parameters = [
        >>>     {"name": "x1", "type": "range", "bounds": [0.0, 1.0]},
        >>>     {"name": "x2", "type": "range", "bounds": [0.0, 1.0]},
        >>> ]
        >>> algo = AxSearch(parameters=parameters,
        >>>     objective_name="hartmann6", max_concurrent=4)
    """

    def __init__(self, ax_client, max_concurrent=10, **kwargs):
        assert ax is not None, "Ax must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._ax = ax_client
        exp = self._ax.experiment
        self._objective_name = exp.optimization_config.objective.metric.name
        if self._ax._enforce_sequential_optimization:
            logger.warning("Detected sequential enforcement. Setting max "
                           "concurrency to 1.")
            max_concurrent = 1
        self._max_concurrent = max_concurrent
        self._parameters = list(exp.parameters)
        self._live_index_mapping = {}
        super(AxSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        parameters, trial_index = self._ax.get_next_trial()
        self._live_index_mapping[trial_id] = trial_index
        return parameters

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Notification for the completion of trial.

        Data of form key value dictionary of metric names and values.
        """
        if result:
            self._process_result(trial_id, result, early_terminated)
        self._live_index_mapping.pop(trial_id)

    def _process_result(self, trial_id, result, early_terminated=False):
        if early_terminated and self._use_early_stopped is False:
            return
        ax_trial_index = self._live_index_mapping[trial_id]
        metric_dict = {
            self._objective_name: (result[self._objective_name], 0.0)
        }
        outcome_names = [
            oc.metric.name for oc in
            self._ax.experiment.optimization_config.outcome_constraints
        ]
        metric_dict.update({on: (result[on], 0.0) for on in outcome_names})
        self._ax.complete_trial(
            trial_index=ax_trial_index, raw_data=metric_dict)

    def _num_live_trials(self):
        return len(self._live_index_mapping)
