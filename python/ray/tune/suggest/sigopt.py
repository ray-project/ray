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

from ray.tune.result import DEFAULT_METRIC
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

    This searcher manages its own concurrency.
    If this Searcher is used in a ``ConcurrencyLimiter``, the
    ``max_concurrent`` value passed to it will override the value passed
    here.

    Parameters:
        space: SigOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
            Not used if existing experiment_id is given
        name: Name of experiment. Required by SigOpt.
        max_concurrent: Number of maximum concurrent trials supported
            based on the user's SigOpt plan. Defaults to 1.
            If this Searcher is used in a ``ConcurrencyLimiter``, the
            ``max_concurrent`` value passed to it will override the
            value passed here.
        connection: An existing connection to SigOpt.
        experiment_id: Optional, if given will connect to an existing
            experiment. This allows for a more interactive experience with
            SigOpt, such as prior beliefs and constraints.
        observation_budget: Optional, can improve SigOpt performance.
        project: Optional, Project name to assign this experiment to.
            SigOpt can group experiments by project
        metric (str or list(str)): If str then the training result
            objective value attribute. If list(str) then a list of
            metrics that can be optimized together. SigOpt currently
            supports up to 2 metrics.
        mode: If experiment_id is given then this
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
            metric="mean_loss", mode="min")


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
            metric=["average", "std"], mode=["max", "min"])
    """

    OBJECTIVE_MAP = {
        "max": {"objective": "maximize", "strategy": "optimize"},
        "min": {"objective": "minimize", "strategy": "optimize"},
        "obs": {"strategy": "store"},
    }

    def __init__(
        self,
        space: List[Dict] = None,
        name: str = "Default Tune Experiment",
        max_concurrent: int = 1,
        connection: Optional[Connection] = None,
        experiment_id: Optional[str] = None,
        observation_budget: Optional[int] = None,
        project: Optional[str] = None,
        metric: Optional[Union[str, List[str]]] = "episode_reward_mean",
        mode: Optional[Union[str, List[str]]] = "max",
        points_to_evaluate: Optional[List[Dict]] = None,
        **kwargs,
    ):
        assert (experiment_id is None) ^ (
            space is None
        ), "space xor experiment_id must be set"
        assert type(max_concurrent) is int and max_concurrent > 0

        self._experiment_id = experiment_id
        self._name = name
        self._max_concurrent = max_concurrent
        self._connection = connection
        self._observation_budget = observation_budget
        self._project = project
        self._space = space
        self._metric = metric
        self._mode = mode
        self._live_trial_mapping = {}

        self._points_to_evaluate = points_to_evaluate

        self.experiment = None

        super(SigOptSearch, self).__init__(metric=metric, mode=mode, **kwargs)
        self._setup_optimizer()

    def _setup_optimizer(self):
        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        if self._mode is None:
            raise ValueError("`mode` argument passed to SigOptSearch must be set.")

        if isinstance(self._metric, str):
            self._metric = [self._metric]
        if isinstance(self._mode, str):
            self._mode = [self._mode]

        if self._connection is not None:
            self.conn = self._connection
        else:
            assert (
                sgo is not None
            ), """SigOpt must be installed!
                You can install SigOpt with the command:
                `pip install -U sigopt`."""
            assert (
                "SIGOPT_KEY" in os.environ
            ), "SigOpt API key must be stored as environ variable at SIGOPT_KEY"
            # Create a connection with SigOpt API, requires API key
            self.conn = sgo.Connection(client_token=os.environ["SIGOPT_KEY"])

        if self._experiment_id is None:
            sigopt_params = dict(
                name=self._name,
                parameters=self._space,
                parallel_bandwidth=self._max_concurrent,
            )

            if self._observation_budget is not None:
                sigopt_params["observation_budget"] = self._observation_budget

            if self._project is not None:
                sigopt_params["project"] = self._project

            if len(self._metric) > 1 and self._observation_budget is None:
                raise ValueError(
                    "observation_budget is required for an"
                    "experiment with more than one optimized metric"
                )
            sigopt_params["metrics"] = self.serialize_metric(self._metric, self._mode)

            self.experiment = self.conn.experiments().create(**sigopt_params)
        else:
            self.experiment = self.conn.experiments(self._experiment_id).fetch()

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if config or self.experiment:
            # no automatic conversion of search space just yet
            return False

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._setup_optimizer()
        return True

    def set_max_concurrency(self, max_concurrent: int) -> bool:
        self._max_concurrent = max_concurrent
        self.experiment = None
        return True

    def suggest(self, trial_id: str):
        # Required here and not in on __init__
        # to make sure set_max_concurrency works correctly
        if not self.experiment:
            self._setup_optimizer()

        if self._max_concurrent:
            if len(self._live_trial_mapping) >= self._max_concurrent:
                return None

        suggestion_kwargs = {}
        if self._points_to_evaluate:
            config = self._points_to_evaluate.pop(0)
            suggestion_kwargs = {"assignments": config}

        # Get new suggestion from SigOpt
        suggestion = (
            self.conn.experiments(self.experiment.id)
            .suggestions()
            .create(**suggestion_kwargs)
        )

        self._live_trial_mapping[trial_id] = suggestion.id

        return copy.deepcopy(suggestion.assignments)

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        """Notification for the completion of trial.

        If a trial fails, it will be reported as a failed Observation, telling
        the optimizer that the Suggestion led to a metric failure, which
        updates the feasible region and improves parameter recommendation.

        Creates SigOpt Observation object for trial.
        """
        if result:
            payload = dict(
                suggestion=self._live_trial_mapping[trial_id],
                values=self.serialize_result(result),
            )
            self.conn.experiments(self.experiment.id).observations().create(**payload)
            # Update the experiment object
            self.experiment = self.conn.experiments(self.experiment.id).fetch()
        elif error:
            # Reports a failed Observation
            self.conn.experiments(self.experiment.id).observations().create(
                failed=True, suggestion=self._live_trial_mapping[trial_id]
            )
        del self._live_trial_mapping[trial_id]

    @staticmethod
    def serialize_metric(metrics: List[str], modes: List[str]):
        """
        Converts metrics to https://app.sigopt.com/docs/objects/metric
        """
        serialized_metric = []
        for metric, mode in zip(metrics, modes):
            serialized_metric.append(
                dict(name=metric, **SigOptSearch.OBJECTIVE_MAP[mode].copy())
            )
        return serialized_metric

    def serialize_result(self, result: Dict):
        """
        Converts experiments results to
        https://app.sigopt.com/docs/objects/metric_evaluation
        """
        missing_scores = [metric for metric in self._metric if metric not in result]

        if missing_scores:
            raise ValueError(
                f"Some metrics specified during initialization are missing. "
                f"Missing metrics: {missing_scores}, provided result {result}"
            )

        values = []
        for metric in self._metric:
            value = dict(name=metric, value=result[metric])
            values.append(value)
        return values

    def save(self, checkpoint_path: str):
        trials_object = (
            self.experiment.id,
            self._live_trial_mapping,
            self._points_to_evaluate,
        )
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        (
            experiment_id,
            self._live_trial_mapping,
            self._points_to_evaluate,
        ) = trials_object

        self.experiment = self.conn.experiments(experiment_id).fetch()
