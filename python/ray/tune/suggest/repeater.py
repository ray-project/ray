import numpy as np
import copy

from ray.tune.suggest.search import SearchAlgorithm

class _TrialGroup:
    """Class for grouping trials of same parameters.

    This is used when repeating trials for reducing training variance. See
    https://github.com/ray-project/ray/issues/6994.

    Args:
        primary_trial_id (str): Trial ID of the "primary trial".
            This trial is the one that the Searcher is aware of.
        trial_ids (List[str]): List of trial ids of the trials in the group.
            All trials should be using the same hyperparameters.
        config (dict): Suggested configuration shared across all trials
            in the trial group.
        metric (str): The training result objective value attribute.
        use_clipped_trials (bool): Whether to use early terminated trials
            in the calculation of the group aggregate score.

    """

    def __init__(self,
                 primary_trial_id,
                 config,
                 count=1):
        assert type(config) is dict, (
            "config is not a dict, got {}".format(config))
        self.primary_trial_id = primary_trial_id
        self.config = config
        self._trials = {primary_trial_id: None}
        self.count = count

    def add(self, trial_id):
        assert len(self._trials) < self.count
        self._trials[trial_id] = None

    def full(self):
        return len(self._trials) == self.count

    def report(self, trial_id, score):
        assert trial_id in self._trials
        self._trials[trial_id] = score

    def scores(self):
        return self._trials.values()


class Repeater(SearchAlgorithm):
    def __init__(self, search_alg, num_repeat):
        self.search_alg = search_alg
        self.num_repeat = num_repeat
        self._groups = []
        self._trial_id_to_group = {}
        self._current_group = None

    def suggest(self, trial_id):
        if self._current_group is None or self._current_group.full():
            config = self.search_alg.suggest(trial_id)
            if config is None:
                return config
            self._current_group = _TrialGroup(trial_id, copy.deepcopy(config))
            self._groups.append(self._current_group)
        else:
            self._current_group.add(trial_id)

        self._trial_id_to_group[trial_id] = self._current_group
        return self._current_group.config.copy()

    def on_trial_complete(self, trial_id, result=None, **kwargs):
        """Stores the score for and keeps track of a completed trial.

        Stores the metric of a trial as nan if any of the following conditions
        are met:
            1. ``result`` is empty or not provided.
            2. ``result`` is provided but no metric was provided.
        """
        if trial_id not in self._trial_id_to_group:
            logger.error("Trial {} not seen before; cannot report score".format(trial_id))
        trial_group = self._trial_id_to_group[trial_id]
        if not result or self.search_alg.metric not in result:
            score = np.nan
        trial_group.report(trial_id, score)
        scores = trial_group.scores()
        if not any(value is None for value in scores):
            self.search_alg.on_trial_complete(
                trial_group.primary_trial_id,
                result=np.nanmean(scores),
                **kwargs)

    def add_configurations(self, *args, **kwargs):
        """Calls the underlying SearchAlgorithm function."""
        return self.search_alg.add_configurations(*args, **kwargs)

    def next_trials(self):
        """Calls the underlying SearchAlgorithm function."""
        return self.search_alg.next_trials()

    def is_finished(self):
        """Calls the underlying SearchAlgorithm function."""
        return self.search_alg.is_finished()

    def save(self, *args, **kwargs):
        """Calls the underlying SearchAlgorithm function."""
        return self.search_alg.save(*args, **kwargs)

    def restore(self, *args, **kwargs):
        """Calls the underlying SearchAlgorithm function."""
        return self.search_alg.restore(*args, **kwargs)
