import copy
import itertools
import logging
import numpy as np

from ray.tune.suggest.suggestion import SuggestionAlgorithm
from ray.tune.experiment import convert_to_experiment_list

logger = logging.getLogger(__name__)

TRIAL_INDEX = "__trial_index__"
"""str: A constant value representing the repeat index of the trial."""


class _TrialGroup:
    """Internal class for grouping trials of same parameters.

    This is used when repeating trials for reducing training variance.

    Args:
        primary_trial_id (str): Trial ID of the "primary trial".
            This trial is the one that the Searcher is aware of.
        config (dict): Suggested configuration shared across all trials
            in the trial group.
        max_trials (int): Max number of trials to execute within this group.

    """

    def __init__(self, primary_trial_id, config, max_trials=1):
        assert type(config) is dict, (
            "config is not a dict, got {}".format(config))
        self.primary_trial_id = primary_trial_id
        self.config = config
        self._trials = {primary_trial_id: None}
        self.max_trials = max_trials

    def add(self, trial_id):
        assert len(self._trials) < self.max_trials
        self._trials[trial_id] = None

    def full(self):
        return len(self._trials) == self.max_trials

    def report(self, trial_id, score):
        assert trial_id in self._trials
        if score is None:
            raise ValueError("Internal Error: Score cannot be None.")
        self._trials[trial_id] = score

    def finished_reporting(self):
        return None not in self._trials.values()

    def scores(self):
        return list(self._trials.values())

    def count(self):
        return len(self._trials)


class Repeater(SuggestionAlgorithm):
    """A wrapper algorithm for repeating trials of same parameters.

    It is recommended that you do not run an early-stopping TrialScheduler
    simultaneously.

    Args:
        search_alg (SearchAlgorithm): SearchAlgorithm object that the
            Repeater will optimize. Note that the SearchAlgorithm
            will only see 1 trial among multiple repeated trials.
            The result/metric passed to the SearchAlgorithm upon
            trial completion will be averaged among all repeats.
        repeat (int): Number of times to generate a trial with a repeated
            configuration. Defaults to 1.
        set_index (bool): Sets a tune.suggest.repeater.TRIAL_INDEX in
            Trainable/Function config which corresponds to the index of the
            repeated trial. This can be used for seeds. Defaults to True.

    """

    def __init__(self, search_alg, repeat=1, set_index=True):
        self.search_alg = search_alg
        self._repeat = repeat
        self._set_index = set_index
        self._groups = []
        self._trial_id_to_group = {}
        self._current_group = None
        super(Repeater, self).__init__(
            metric=self.search_alg.metric,
            mode=self.search_alg.mode,
            use_early_stopped_trials=self.search_alg._use_early_stopped)

    def add_configurations(self, experiments):
        """Chains generator given experiment specifications.

        Multiplies the number of trials by the repeat factor.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        for experiment in experiment_list:
            self._trial_generator = itertools.chain(
                self._trial_generator,
                self._generate_trials(
                    experiment.spec.get("num_samples", 1) * self._repeat,
                    experiment.spec, experiment.name))

    def suggest(self, trial_id):
        if self._current_group is None or self._current_group.full():
            config = self.search_alg.suggest(trial_id)
            if config is None:
                return config
            self._current_group = _TrialGroup(
                trial_id, copy.deepcopy(config), max_trials=self._repeat)
            self._groups.append(self._current_group)
            index_in_group = 0
        else:
            index_in_group = self._current_group.count()
            self._current_group.add(trial_id)

        config = self._current_group.config.copy()
        if self._set_index:
            config[TRIAL_INDEX] = index_in_group
        self._trial_id_to_group[trial_id] = self._current_group
        return config

    def on_trial_complete(self, trial_id, result=None, **kwargs):
        """Stores the score for and keeps track of a completed trial.

        Stores the metric of a trial as nan if any of the following conditions
        are met:

        1. ``result`` is empty or not provided.
        2. ``result`` is provided but no metric was provided.

        """
        if trial_id not in self._trial_id_to_group:
            logger.error("Trial {} not in group; cannot report score. "
                         "Seen trials: {}".format(
                             trial_id, list(self._trial_id_to_group)))
        trial_group = self._trial_id_to_group[trial_id]
        if not result or self.search_alg.metric not in result:
            score = np.nan
        else:
            score = result[self.search_alg.metric]
        trial_group.report(trial_id, score)

        if trial_group.finished_reporting():
            scores = trial_group.scores()
            self.search_alg.on_trial_complete(
                trial_group.primary_trial_id,
                result={self.search_alg.metric: np.nanmean(scores)},
                **kwargs)
