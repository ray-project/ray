from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import ax.service.ax_client as ax_client
except ImportError:
    ax_client = None

from ray.tune.suggest.suggestion import SuggestionAlgorithm


class AxSearch(SuggestionAlgorithm):
    """A wrapper around Ax to provide trial suggestions.

     Example:
        >>> 
    """

    def __init__(self,
                 parameters,
                 objective_name,
                 max_concurrent=10,
                 minimize=False,
                 param_constraints=None,
                 outcome_constraints=None,
                 **kwargs):
        assert ax_client is not None, "Ax must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._ax = ax_client.AxClient()
        self._ax.create_experiment(
            name="axsearch",
            parameters=parameters,
            objective_name=objective_name,
            minimize=minimize,
            parameter_constraints=param_constraints,
            outcome_constraints=outcome_constraints
        )
        self._max_concurrent = max_concurrent
        self._parameters = [d["name"] for d in parameters]
        self._objective_name = objective_name
        self._live_index_mapping = {}
        super(AxSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        parameters, trial_index = self._ax.get_next_trial()
        suggested_config = list(parameters.values())
        self._live_index_mapping[trial_id] = trial_index
        return dict(zip(self._parameters, suggested_config))

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        ax_trial_index = self._live_index_mapping.pop(trial_id)
        if result:
            evaluation = {self._objective_name: (-result[self._objective_name], 0.0)}
            self._ax.complete_trial(trial_index=ax_trial_index, raw_data=evaluation)

    def _num_live_trials(self):
        return len(self._live_index_mapping)
