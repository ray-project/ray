import copy
import logging
from typing import Dict, List, Optional

import numpy as np

from ray.tune.search import Searcher
from ray.tune.search.util import set_search_properties_backwards_compatible
from ray.util import PublicAPI

logger = logging.getLogger(__name__)

TRIAL_INDEX = "__trial_index__"
"""str: A constant value representing the repeat index of the trial."""


def _warn_num_samples(searcher: Searcher, num_samples: int):
    if isinstance(searcher, Repeater) and num_samples % searcher.repeat:
        logger.warning(
            "`num_samples` is now expected to be the total number of trials, "
            "including the repeat trials. For example, set num_samples=15 if "
            "you intend to obtain 3 search algorithm suggestions and repeat "
            "each suggestion 5 times. Any leftover trials "
            "(num_samples mod repeat) will be ignored."
        )


class _TrialGroup:
    """Internal class for grouping trials of same parameters.

    This is used when repeating trials for reducing training variance.

    Args:
        primary_trial_id: Trial ID of the "primary trial".
            This trial is the one that the Searcher is aware of.
        config: Suggested configuration shared across all trials
            in the trial group.
        max_trials: Max number of trials to execute within this group.

    """

    def __init__(self, primary_trial_id: str, config: Dict, max_trials: int = 1):
        assert type(config) is dict, "config is not a dict, got {}".format(config)
        self.primary_trial_id = primary_trial_id
        self.config = config
        self._trials = {primary_trial_id: None}
        self.max_trials = max_trials

    def add(self, trial_id: str):
        assert len(self._trials) < self.max_trials
        self._trials.setdefault(trial_id, None)

    def full(self) -> bool:
        return len(self._trials) == self.max_trials

    def report(self, trial_id: str, score: float):
        assert trial_id in self._trials
        if score is None:
            raise ValueError("Internal Error: Score cannot be None.")
        self._trials[trial_id] = score

    def finished_reporting(self) -> bool:
        return (
            None not in self._trials.values() and len(self._trials) == self.max_trials
        )

    def scores(self) -> List[Optional[float]]:
        return list(self._trials.values())

    def count(self) -> int:
        return len(self._trials)


@PublicAPI
class Repeater(Searcher):
    """A wrapper algorithm for repeating trials of same parameters.

    Set tune.run(num_samples=...) to be a multiple of `repeat`. For example,
    set num_samples=15 if you intend to obtain 3 search algorithm suggestions
    and repeat each suggestion 5 times. Any leftover trials
    (num_samples mod repeat) will be ignored.

    It is recommended that you do not run an early-stopping TrialScheduler
    simultaneously.

    Args:
        searcher: Searcher object that the
            Repeater will optimize. Note that the Searcher
            will only see 1 trial among multiple repeated trials.
            The result/metric passed to the Searcher upon
            trial completion will be averaged among all repeats.
        repeat: Number of times to generate a trial with a repeated
            configuration. Defaults to 1.
        set_index: Sets a tune.search.repeater.TRIAL_INDEX in
            Trainable/Function config which corresponds to the index of the
            repeated trial. This can be used for seeds. Defaults to True.

    Example:

    .. code-block:: python

        from ray.tune.search import Repeater

        search_alg = BayesOptSearch(...)
        re_search_alg = Repeater(search_alg, repeat=10)

        # Repeat 2 samples 10 times each.
        tune.run(trainable, num_samples=20, search_alg=re_search_alg)

    """

    def __init__(self, searcher: Searcher, repeat: int = 1, set_index: bool = True):
        self.searcher = searcher
        self.repeat = repeat
        self._set_index = set_index
        self._groups = []
        self._trial_id_to_group = {}
        self._current_group = None
        super(Repeater, self).__init__(
            metric=self.searcher.metric, mode=self.searcher.mode
        )

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if self._current_group is None or self._current_group.full():
            config = self.searcher.suggest(trial_id)
            if config is None:
                return config
            self._current_group = _TrialGroup(
                trial_id, copy.deepcopy(config), max_trials=self.repeat
            )
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

    def on_trial_complete(self, trial_id: str, result: Optional[Dict] = None, **kwargs):
        """Stores the score for and keeps track of a completed trial.

        Stores the metric of a trial as nan if any of the following conditions
        are met:

        1. ``result`` is empty or not provided.
        2. ``result`` is provided but no metric was provided.

        """
        if trial_id not in self._trial_id_to_group:
            logger.error(
                "Trial {} not in group; cannot report score. "
                "Seen trials: {}".format(trial_id, list(self._trial_id_to_group))
            )
        trial_group = self._trial_id_to_group[trial_id]
        if not result or self.searcher.metric not in result:
            score = np.nan
        else:
            score = result[self.searcher.metric]
        trial_group.report(trial_id, score)

        if trial_group.finished_reporting():
            scores = trial_group.scores()
            self.searcher.on_trial_complete(
                trial_group.primary_trial_id,
                result={self.searcher.metric: np.nanmean(scores)},
                **kwargs
            )

    def get_state(self) -> Dict:
        self_state = self.__dict__.copy()
        del self_state["searcher"]
        return self_state

    def set_state(self, state: Dict):
        self.__dict__.update(state)

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        return set_search_properties_backwards_compatible(
            self.searcher.set_search_properties, metric, mode, config, **spec
        )
