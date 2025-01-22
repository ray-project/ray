"""A Vizier Ray Searcher."""

import datetime
import json
import logging
import uuid
from typing import Dict, Optional, Union

from ray.tune.result import DEFAULT_METRIC
from ray.tune.search import UNDEFINED_METRIC_MODE, UNRESOLVED_SEARCH_SPACE, Searcher
from ray.tune.search.variant_generator import parse_spec_vars

# Make sure that importing this file doesn't crash Ray, even if Vizier wasn't installed.
try:
    from vizier import raytune as vzr
    from vizier.service import clients
    from vizier.service import pyvizier as svz

    IMPORT_SUCCESSFUL = True
except ImportError:
    svz = None
    IMPORT_SUCCESSFUL = False


logger = logging.getLogger(__name__)


class VizierSearch(Searcher):
    """A wrapper around OSS Vizier to provide trial suggestions.

    OSS Vizier is a Python-based service for black-box optimization based on
    Google Vizier, one of the first hyperparameter tuning services
    designed to work at scale.

    More info can be found here: https://github.com/google/vizier.

    You will need to install OSS Vizier via the following:

    .. code-block:: bash

        pip install google-vizier[jax]

    Args:
        space: A dict mapping parameter names to Tune search spaces
            or a Vizier SearchSpace object.
        metric: The training result objective value attribute. If None
            but a mode was passed, the anonymous metric `_metric` will be used
            per default.
        mode: One of {"min", "max"}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        algorithm: Specific algorithm from Vizier's library to use.
        "DEFAULT" corresponds to GP-UCB-PE. See https://oss-vizier.readthedocs.io\
/en/latest/guides/user/supported_algorithms.html for more options.

    Tune automatically converts search spaces to Vizier's format:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.bayesopt import VizierSearch

        config = {
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100)
        }

        vizier = VizierSearch(metric="mean_loss", mode="min")
        tuner = tune.Tuner(
            my_func,
            tune_config=tune.TuneConfig(
                search_alg=vizier,
            ),
            param_space=config,
        )
        tuner.fit()

    Alternatively, you can pass a Vizier `SearchSpace` object manually to the
    Searcher:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.vizier import VizierSearch
        from vizier import pyvizier as vz

        search_space = vz.SearchSpace()
        search_space.root.add_float_param('w', 0.0, 5.0)

        vizier = VizierSearch(search_space, metric="mean_loss", mode="min")
        tuner = tune.Tuner(
            trainable_function,
            tune_config=tune.TuneConfig(
                search_alg=vizier
            )
        )
        tuner.fit()

    """

    def __init__(
        self,
        space: Optional[Union[Dict, "svz.SearchSpace"]] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        algorithm: Optional[str] = "DEFAULT",
    ):
        assert (
            IMPORT_SUCCESSFUL
        ), "Vizier must be installed with `pip install google-vizier[jax]`."
        super(VizierSearch, self).__init__(metric=metric, mode=mode)
        self._algorithm = algorithm

        # For Vizier to identify the unique study.
        self._study_id = f"ray_vizier_{uuid.uuid1()}"

        # Mapping from Ray trial id to Vizier Trial client.
        self._active_trials: Dict[str, clients.Trial] = {}

        # Vizier service client.
        self._study_client: Optional[clients.Study] = None

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(par="space", cls=type(self))
                )
                space = self.convert_search_space(space)
            self._space = space
        elif isinstance(space, svz.SearchSpace):
            self._space = space
        else:
            raise TypeError(
                "Space must be Tune space or Vizier SearchSpace. Got {}.".format(
                    type(space)
                )
            )

        if self._space:
            self._setup_vizier()

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self._study_client:  # The study is already configured.
            return False

        self._metric = metric or DEFAULT_METRIC
        self._mode = mode
        self._space = vzr.SearchSpaceConverter.to_vizier(config)

        self._setup_vizier()
        return True

    def _setup_vizier(self) -> None:
        if self._mode == "max":
            vizier_goal = svz.ObjectiveMetricGoal.MAXIMIZE
        elif self._mode == "min":
            vizier_goal = svz.ObjectiveMetricGoal.MINIMIZE
        else:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__, metric=self._metric, mode=self._mode
                )
            )

        study_config = svz.StudyConfig(
            search_space=self._space,
            algorithm=self._algorithm,
            metric_information=[svz.MetricInformation(self._metric, goal=vizier_goal)],
        )
        self._study_client = clients.Study.from_study_config(
            study_config, owner="raytune", study_id=self._study_id
        )

    def on_trial_result(self, trial_id: str, result: Dict) -> None:
        if trial_id not in self._active_trials:
            raise RuntimeError(f"No active trial for {trial_id}")
        if self._study_client is None:
            raise RuntimeError(
                "VizierSearch not initialized! Set a search space first."
            )
        trial_client = self._active_trials[trial_id]
        elapsed_secs = (
            datetime.datetime.now().astimezone()
            - trial_client.materialize().creation_time
        )
        trial_client.add_measurement(
            svz.Measurement(
                {k: v for k, v in result.items() if isinstance(v, float)},
                elapsed_secs=elapsed_secs.total_seconds(),
            )
        )

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ) -> None:
        if trial_id not in self._active_trials:
            raise RuntimeError(f"No active trial for {trial_id}")
        if self.study_client is None:
            raise RuntimeError(
                "VizierSearch not initialized! Set a search space first."
            )
        trial_client = self._active_trials[trial_id]

        if error:
            # Mark the trial as infeasible.
            trial_client.complete(
                infeasible_reason=f"Trial {trial_id} failed: {result}"
            )
        else:
            measurement = None
            if result:
                elapsed_secs = (
                    datetime.datetime.now().astimezone()
                    - trial_client.materialize().creation_time
                )
                measurement = svz.Measurement(
                    {k: v for k, v in result.items() if isinstance(v, float)},
                    elapsed_secs=elapsed_secs.total_seconds(),
                )
            trial_client.complete(measurement=measurement)
        self._active_trials.pop(trial_id)

    def suggest(self, trial_id: str) -> Optional[Dict]:
        suggestions = self._study_client.suggest(count=1, client_id=trial_id)
        if not suggestions:
            return Searcher.FINISHED

        self._active_trials[trial_id] = suggestions[0]
        return self._active_trials[trial_id].parameters

    def save(self, checkpoint_path: str) -> None:
        # We assume that the Vizier service continues running, so the only
        # information needed to restore this searcher is the mapping from the Ray
        # to Vizier trial ids. All other information can become stale and is best
        # restored from the Vizier service in restore().
        ray_to_vizier_trial_ids = {}
        for trial_id, trial_client in self._active_trials.items():
            ray_to_vizier_trial_ids[trial_id] = trial_client.id
        with open(checkpoint_path, "w") as f:
            info = {
                "study_id": self._study_id,
                "ray_to_vizier_trial_ids": ray_to_vizier_trial_ids,
            }
            json.dump(info, f)

    def restore(self, checkpoint_path: str) -> None:
        with open(checkpoint_path, "r") as f:
            obj = json.load(f)

        self._study_id = obj["study_id"]
        # TODO: This will return a `ResourceNotFoundError` if study doesn't exist.
        # Consider adding more explicit error handling later on if this is opaque.
        self._study_client = clients.Study.from_owner_and_id("raytune", self._study_id)
        self._metric = (
            self._study_client.materialize_study_config().metric_information.item()
        )
        self._active_trials = {}
        for ray_id, vizier_trial_id in obj["ray_to_vizier_trial_ids"].items():
            self._active_trials[ray_id] = self._study_client.get_trial(vizier_trial_id)

    @staticmethod
    def convert_search_space(spec: Dict) -> "svz.SearchSpace":
        return vzr.SearchSpaceConverter.to_vizier(spec)
