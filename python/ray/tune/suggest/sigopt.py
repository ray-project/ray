import copy
import os
import logging
import pickle
from typing import Dict, List, Optional, Union

try:
    import sigopt as sgo
    Connection = sgo.Connection
except ImportError:
    sgo = None
    Connection = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


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
            Not used if existing experiment_id is given
        name (str): Name of experiment. Required by SigOpt.
        max_concurrent (int): Number of maximum concurrent trials supported
            based on the user's SigOpt plan. Defaults to 1.
        connection (Connection): An existing connection to SigOpt.
        experiment_id (str): Optional, if given will connect to an existing
            experiment. This allows for a more interactive experience with
            SigOpt, such as prior beliefs and constraints.
        observation_budget (int): Optional, can improve SigOpt performance.
        project (str): Optional, Project name to assign this experiment to.
            SigOpt can group experiments by project
        metric (str or list(str)): If str then the training result
            objective value attribute. If list(str) then a list of
            metrics that can be optimized together. SigOpt currently
            supports up to 2 metrics.
        mode (str or list(str)): If experiment_id is given then this
            field is ignored, If str then must be one of {min, max}.
            If list then must be comprised of {min, max, obs}. Determines
            whether objective is minimizing or maximizing the metric
            attribute. If metrics is a list then mode must be a list
            of the same length as metric.

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
            space, name="SigOpt Multi Objective Example Experiment",
            max_concurrent=1, metric=["average", "std"], mode=["max", "min"])
    """
    OBJECTIVE_MAP = {
        "max": {
            "objective": "maximize",
            "strategy": "optimize"
        },
        "min": {
            "objective": "minimize",
            "strategy": "optimize"
        },
        "obs": {
            "strategy": "store"
        }
    }

    def __init__(self,
                 space: List[Dict] = None,
                 name: str = "Default Tune Experiment",
                 max_concurrent: int = 1,
                 reward_attr: Optional[str] = None,
                 connection: Optional[Connection] = None,
                 experiment_id: Optional[str] = None,
                 observation_budget: Optional[int] = None,
                 project: Optional[str] = None,
                 metric: Union[None, str, List[str]] = "episode_reward_mean",
                 mode: Union[None, str, List[str]] = "max",
                 points_to_evaluate: Optional[List[Dict]] = None,
                 **kwargs):
        assert (experiment_id is
                None) ^ (space is None), "space xor experiment_id must be set"
        assert type(max_concurrent) is int and max_concurrent > 0

        if connection is not None:
            self.conn = connection
        else:
            assert sgo is not None, """SigOpt must be installed!
                You can install SigOpt with the command:
                `pip install -U sigopt`."""
            assert "SIGOPT_KEY" in os.environ, \
                "SigOpt API key must be stored as " \
                "environ variable at SIGOPT_KEY"
            # Create a connection with SigOpt API, requires API key
            self.conn = sgo.Connection(client_token=os.environ["SIGOPT_KEY"])

        self._max_concurrent = max_concurrent
        if isinstance(metric, str):
            metric = [metric]
            mode = [mode]
        self._metric = metric
        self._live_trial_mapping = {}

        if experiment_id is None:
            sigopt_params = dict(
                name=name,
                parameters=space,
                parallel_bandwidth=self._max_concurrent)

            if observation_budget is not None:
                sigopt_params["observation_budget"] = observation_budget

            if project is not None:
                sigopt_params["project"] = project

            if len(metric) > 1 and observation_budget is None:
                raise ValueError(
                    "observation_budget is required for an"
                    "experiment with more than one optimized metric")
            sigopt_params["metrics"] = self.serialize_metric(metric, mode)

            self.experiment = self.conn.experiments().create(**sigopt_params)
        else:
            self.experiment = self.conn.experiments(experiment_id).fetch()

        self._points_to_evaluate = points_to_evaluate

        super(SigOptSearch, self).__init__(metric=metric, mode=mode, **kwargs)

    def suggest(self, trial_id: str):
        if self._max_concurrent:
            if len(self._live_trial_mapping) >= self._max_concurrent:
                return None

        suggestion_kwargs = {}
        if self._points_to_evaluate:
            config = self._points_to_evaluate.pop(0)
            suggestion_kwargs = {"assignments": config}

        # Get new suggestion from SigOpt
        suggestion = self.conn.experiments(
            self.experiment.id).suggestions().create(**suggestion_kwargs)

        self._live_trial_mapping[trial_id] = suggestion.id

        return copy.deepcopy(suggestion.assignments)

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False):
        """Notification for the completion of trial.

        If a trial fails, it will be reported as a failed Observation, telling
        the optimizer that the Suggestion led to a metric failure, which
        updates the feasible region and improves parameter recommendation.

        Creates SigOpt Observation object for trial.
        """
        if result:
            payload = dict(
                suggestion=self._live_trial_mapping[trial_id],
                values=self.serialize_result(result))
            self.conn.experiments(
                self.experiment.id).observations().create(**payload)
            # Update the experiment object
            self.experiment = self.conn.experiments(self.experiment.id).fetch()
        elif error:
            # Reports a failed Observation
            self.conn.experiments(self.experiment.id).observations().create(
                failed=True, suggestion=self._live_trial_mapping[trial_id])
        del self._live_trial_mapping[trial_id]

    @staticmethod
    def serialize_metric(metrics: List[str], modes: List[str]):
        """
        Converts metrics to https://app.sigopt.com/docs/objects/metric
        """
        serialized_metric = []
        for metric, mode in zip(metrics, modes):
            serialized_metric.append(
                dict(name=metric, **SigOptSearch.OBJECTIVE_MAP[mode].copy()))
        return serialized_metric

    def serialize_result(self, result: Dict):
        """
        Converts experiments results to
        https://app.sigopt.com/docs/objects/metric_evaluation
        """
        missing_scores = [
            metric for metric in self._metric if metric not in result
        ]

        if missing_scores:
            raise ValueError(
                f"Some metrics specified during initialization are missing. "
                f"Missing metrics: {missing_scores}, provided result {result}")

        values = []
        for metric in self._metric:
            value = dict(name=metric, value=result[metric])
            values.append(value)
        return values

    def save(self, checkpoint_path: str):
        trials_object = (self.experiment.id, self._live_trial_mapping,
                         self._points_to_evaluate)
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        experiment_id, self._live_trial_mapping, self._points_to_evaluate = \
            trials_object

        self.experiment = self.conn.experiments(experiment_id).fetch()
