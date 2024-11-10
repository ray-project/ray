"""A Vizier Ray Searcher."""

import datetime
import json
from typing import Dict, Optional
import uuid

from ray import tune
from ray.tune import search

# Make sure that importing this file doesn't crash Ray, even if Vizier wasn't installed.
try:
    from vizier import raytune as vzr
    from vizier.service import clients
    from vizier.service import pyvizier as svz

    StudyConfig = svz.StudyConfig
    IMPORT_SUCCESSFUL = True
except ImportError:
    IMPORT_SUCCESSFUL = False
    StudyConfig = None


class VizierSearch(search.Searcher):
    """An OSS Vizier Searcher for Ray."""

    def __init__(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        study_id: Optional[str] = None,
        algorithm: Optional[str] = 'DEFAULT',
    ):
        """Initialize a Searcher via ProblemStatement.

        Args:
            study_id: The study id in the Vizier service.
            algorithm: The Vizier algorithm to use.
            **kwargs:
        """
        assert IMPORT_SUCCESSFUL, 'Vizier must be installed with `pip install google-vizier[jax]`.'
      
        super(VizierSearch, self).__init__(metric=metric, mode=mode)

        if study_id:
            self._study_id = study_id
        else:
            self._study_id = f'ray_vizier_{uuid.uuid1()}'

        self._algorithm = algorithm

        # Mapping from Ray trial id to Vizier Trial client.
        self._active_trials: Dict[str, clients.Trial] = {}

        # The name of the metric being optimized, for single objective studies.
        self._metric = None

        # Vizier service client.
        self._study_client: Optional[clients.Study] = None

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self._study_client:  # The study is already configured.
            return False

        if mode not in ['min', 'max']:
            raise ValueError("'mode' must be one of ['min', 'max']")

        self._metric = metric or tune.result.DEFAULT_METRIC

        vizier_goal = (
            svz.ObjectiveMetricGoal.MAXIMIZE
            if mode == 'max'
            else svz.ObjectiveMetricGoal.MINIMIZE
        )
        study_config = svz.StudyConfig(
            search_space=vzr.SearchSpaceConverter.to_vizier(config),
            algorithm=self._algorithm,
            metric_information=[
                svz.MetricInformation(self._metric, goal=vizier_goal)
            ],
        )
        self._study_client = clients.Study.from_study_config(
            study_config, owner='raytune', study_id=self._study_id
        )
        return True

    def on_trial_result(self, trial_id: str, result: Dict) -> None:
        if trial_id not in self._active_trials:
            raise RuntimeError(f'No active trial for {trial_id}')
        if self._study_client is None:
            raise RuntimeError(
                'VizierSearch not initialized! Set a search space first.'
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
            raise RuntimeError(f'No active trial for {trial_id}')
        if self.study_client is None:
            raise RuntimeError(
                'VizierSearch not initialized! Set a search space first.'
            )
        trial_client = self._active_trials[trial_id]

        if error:
            # Mark the trial as infeasible.
            trial_client.complete(
                infeasible_reason=f'Trial {trial_id} failed: {result}'
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
            return search.Searcher.FINISHED

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
        with open(checkpoint_path, 'w') as f:
            info = {'study_id': self._study_id, 'ray_to_vizier_trial_ids': ray_to_vizier_trial_ids}
            json.dump(info, f)

    def restore(self, checkpoint_path: str) -> None:
        with open(checkpoint_path, 'r') as f:
            obj = json.load(f)

        self._study_id = obj['study_id']
        self._study_client = clients.Study.from_owner_and_id('raytune', self.study_id)
        self._metric = (
            self._study_client.materialize_study_config().metric_information.item()
        )
        self._active_trials = {}
        for ray_id, vizier_trial_id in obj['ray_to_vizier_trial_ids'].items():
            self._active_trials[ray_id] = self._study_client.get_trial(vizier_trial_id)
