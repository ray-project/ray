try:
    import ax
except ImportError:
    ax = None
import logging

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class AxSearch(Searcher):
    """Uses `Ax <https://ax.dev/>`_ to optimize hyperparameters.

    Ax is a platform for understanding, managing, deploying, and
    automating adaptive experiments. Ax provides an easy to use
    interface with BoTorch, a flexible, modern library for Bayesian
    optimization in PyTorch. More information can be found in https://ax.dev/.

    To use this search algorithm, you must install Ax and sqlalchemy:

    .. code-block:: bash

        $ pip install ax-platform sqlalchemy

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
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute. Defaults to "max".
        parameter_constraints (list[str]): Parameter constraints, such as
            "x3 >= x4" or "x3 + x4 >= 2".
        outcome_constraints (list[str]): Outcome constraints of form
            "metric_name >= bound", like "m1 <= 3."
        max_concurrent (int): Deprecated.
        use_early_stopped_trials: Deprecated.

    .. code-block:: python

        from ax.service.ax_client import AxClient
        from ray import tune
        from ray.tune.suggest.ax import AxSearch

        parameters = [
            {"name": "x1", "type": "range", "bounds": [0.0, 1.0]},
            {"name": "x2", "type": "range", "bounds": [0.0, 1.0]},
        ]

        def easy_objective(config):
            for i in range(100):
                intermediate_result = config["x1"] + config["x2"] * i
                tune.track.log(score=intermediate_result)

        client = AxClient(enforce_sequential_optimization=False)
        client.create_experiment(parameters=parameters, objective_name="score")
        algo = AxSearch(client)
        tune.run(easy_objective, search_alg=algo)

    """

    def __init__(self,
                 ax_client,
                 mode="max",
                 use_early_stopped_trials=None,
                 max_concurrent=None):
        assert ax is not None, "Ax must be installed!"
        self._ax = ax_client
        exp = self._ax.experiment
        self._objective_name = exp.optimization_config.objective.metric.name
        self.max_concurrent = max_concurrent
        self._parameters = list(exp.parameters)
        self._live_trial_mapping = {}
        super(AxSearch, self).__init__(
            metric=self._objective_name,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)
        if self._ax._enforce_sequential_optimization:
            logger.warning("Detected sequential enforcement. Be sure to use "
                           "a ConcurrencyLimiter.")

    def suggest(self, trial_id):
        if self.max_concurrent:
            if len(self._live_trial_mapping) >= self.max_concurrent:
                return None
        parameters, trial_index = self._ax.get_next_trial()
        self._live_trial_mapping[trial_id] = trial_index
        return parameters

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        Data of form key value dictionary of metric names and values.
        """
        if result:
            self._process_result(trial_id, result)
        self._live_trial_mapping.pop(trial_id)

    def _process_result(self, trial_id, result):
        ax_trial_index = self._live_trial_mapping[trial_id]
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
