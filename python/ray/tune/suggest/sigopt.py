import copy
import os
import logging
import pickle
import typing
from enum import Enum
try:
    import sigopt as sgo
except ImportError:
    sgo = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class Objective(Enum):
    maximize: 1
    minimize : 2


class SigOptMetric(typing.NamedTuple):
    name: str
    objective: Objective
    stddev_name: typing.Optional[str] = None


class SigOptSearch(Searcher):
    """A wrapper around SigOpt to provide trial suggestions.

    You must install SigOpt and have a SigOpt API key to use this module.
    Store the API token as an environment variable ``SIGOPT_KEY`` as follows:

    .. code-block:: bash

        pip install -U sigopt
        export SIGOPT_KEY= ...

    You will need to use the `SigOpt experiment and space specification
    <https://app.sigopt.com/docs/overview/create>`_.

    This module manages its own concurrency.

    Parameters:
        space (list of dict): SigOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
        name (str): Name of experiment. Required by SigOpt.
        max_concurrent (int): Number of maximum concurrent trials supported
            based on the user's SigOpt plan. Defaults to 1.
        connection (Connection): An existing connection to SigOpt.
        observation_budget (int): Optional, can improve SigOpt performance.
        project (str): Optional, Project name to assign this experiment to.
            SigOpt can group experiments by project
        metric (str or List(SigOptMetric)): If str then the training result
            objective value attribute. If list(SigOptMetric) then a list of
            SigOptMetrics that can be optimized together. SigOpt currently
            supports up to 2 metrics.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute. Will not be used
            if metric is list.

    Example:

    .. code-block:: python

        space = [
            {
                'name': 'width',
                'type': 'int',
                'bounds': {
                    'min': 0,
                    'max': 20
                },
            },
            {
                'name': 'height',
                'type': 'int',
                'bounds': {
                    'min': -100,
                    'max': 100
                },
            },
        ]
        algo = SigOptSearch(
            space, name="SigOpt Example Experiment",
            max_concurrent=1, metric="mean_loss", mode="min")
    """

    def __init__(self,
                 space,
                 name="Default Tune Experiment",
                 max_concurrent=1,
                 reward_attr=None,
                 connection=None,
                 observation_budget=None,
                 project=None,
                 metric="episode_reward_mean",
                 mode="max",
                 **kwargs):
        assert type(max_concurrent) is int and max_concurrent > 0
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        if connection is not None:
            self.conn = connection
        else:
            assert sgo is not None, "SigOpt must be installed!"
            assert "SIGOPT_KEY" in os.environ, \
                "SigOpt API key must be stored as " \
                "environ variable at SIGOPT_KEY"
            # Create a connection with SigOpt API, requires API key
            self.conn = sgo.Connection(client_token=os.environ["SIGOPT_KEY"])

        self._max_concurrent = max_concurrent
        self._metric = metric
        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.
        self._live_trial_mapping = {}

        sigopt_params = dict(
            name=name,
            parameters=space,
            parallel_bandwidth=self._max_concurrent)

        if observation_budget is not None:
            sigopt_params["observation_budget"] = observation_budget

        if project is not None:
            sigopt_params["project"] = project

        if isinstance(metric, list):
            sigopt_params["metric"] = self.serialize_metric(metric)

        self.experiment = self.conn.experiments().create(**sigopt_params)

        super(SigOptSearch, self).__init__(metric=metric, mode=mode, **kwargs)

    def suggest(self, trial_id):
        if self._max_concurrent:
            if len(self._live_trial_mapping) >= self._max_concurrent:
                return None
        # Get new suggestion from SigOpt
        suggestion = self.conn.experiments(
            self.experiment.id).suggestions().create()

        self._live_trial_mapping[trial_id] = suggestion

        return copy.deepcopy(suggestion.assignments)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        If a trial fails, it will be reported as a failed Observation, telling
        the optimizer that the Suggestion led to a metric failure, which
        updates the feasible region and improves parameter recommendation.

        Creates SigOpt Observation object for trial.
        """
        if result:
            payload = dict(suggestion=self._live_trial_mapping[trial_id].id)
            if isinstance(self.metric, list):
                payload["values"] = self.serialize_result(result)
            else:
                payload["value"] = self._metric_op * result[self._metric]

            self.conn.experiments(self.experiment.id).observations().create(**payload)
            # Update the experiment object
            self.experiment = self.conn.experiments(self.experiment.id).fetch()
        elif error:
            # Reports a failed Observation
            self.conn.experiments(self.experiment.id).observations().create(
                failed=True, suggestion=self._live_trial_mapping[trial_id].id)
        del self._live_trial_mapping[trial_id]

    @staticmethod
    def serialize_metric(metrics):
        """
        Converts metrics to https://app.sigopt.com/docs/objects/metric
        """
        return [
            dict(name=metric.name,
                 objective=metric.score.name,
                 strategy="optimize")
            for metric in metrics
        ]

    def serialize_result(self, result):
        """
        Converts results to https://app.sigopt.com/docs/objects/metric_evaluation
        """
        missing_scores = [metric.name for metric in self._metric
                          if metric.name not in result]

        if missing_scores:
            raise ValueError(
                f"Some metrics specified during initialization are missing. "
                f"Missing metrics: {missing_scores}, provided result {result}"
            )

        values = []
        for metric in self._metric:
            value = dict(name=metric.name, value=result[metric.name])
            if metric.stddev_name is not None:
                value["value_stddev"] = result[metric.stddev_name]
            values.append(value)
        return values

    def save(self, checkpoint_path):
        trials_object = (self.conn, self.experiment)
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_path):
        with open(checkpoint_path, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self.conn = trials_object[0]
        self.experiment = trials_object[1]
