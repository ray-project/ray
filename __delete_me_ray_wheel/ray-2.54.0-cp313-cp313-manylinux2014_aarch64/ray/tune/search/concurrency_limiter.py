import copy
import logging
from typing import Dict, List, Optional

from ray.tune.search.searcher import Searcher
from ray.tune.search.util import _set_search_properties_backwards_compatible
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
class ConcurrencyLimiter(Searcher):
    """A wrapper algorithm for limiting the number of concurrent trials.

    Certain Searchers have their own internal logic for limiting
    the number of concurrent trials. If such a Searcher is passed to a
    ``ConcurrencyLimiter``, the ``max_concurrent`` of the
    ``ConcurrencyLimiter`` will override the ``max_concurrent`` value
    of the Searcher. The ``ConcurrencyLimiter`` will then let the
    Searcher's internal logic take over.

    Args:
        searcher: Searcher object that the
            ConcurrencyLimiter will manage.
        max_concurrent: Maximum concurrent samples from the underlying
            searcher.
        batch: Whether to wait for all concurrent samples
            to finish before updating the underlying searcher.

    Example:

    .. code-block:: python

        from ray.tune.search import ConcurrencyLimiter
        search_alg = HyperOptSearch(metric="accuracy")
        search_alg = ConcurrencyLimiter(search_alg, max_concurrent=2)
        tuner = tune.Tuner(
            trainable,
            tune_config=tune.TuneConfig(
                search_alg=search_alg
            ),
        )
        tuner.fit()
    """

    def __init__(self, searcher: Searcher, max_concurrent: int, batch: bool = False):
        assert type(max_concurrent) is int and max_concurrent > 0
        self.searcher = searcher
        self.max_concurrent = max_concurrent
        self.batch = batch
        self.live_trials = set()
        self.num_unfinished_live_trials = 0
        self.cached_results = {}
        self._limit_concurrency = True

        if not isinstance(searcher, Searcher):
            raise RuntimeError(
                f"The `ConcurrencyLimiter` only works with `Searcher` "
                f"objects (got {type(searcher)}). Please try to pass "
                f"`max_concurrent` to the search generator directly."
            )

        self._set_searcher_max_concurrency()

        super(ConcurrencyLimiter, self).__init__(
            metric=self.searcher.metric, mode=self.searcher.mode
        )

    def _set_searcher_max_concurrency(self):
        # If the searcher has special logic for handling max concurrency,
        # we do not do anything inside the ConcurrencyLimiter
        self._limit_concurrency = not self.searcher.set_max_concurrency(
            self.max_concurrent
        )

    def set_max_concurrency(self, max_concurrent: int) -> bool:
        # Determine if this behavior is acceptable, or if it should
        # raise an exception.
        self.max_concurrent = max_concurrent
        return True

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        self._set_searcher_max_concurrency()
        return _set_search_properties_backwards_compatible(
            self.searcher.set_search_properties, metric, mode, config, **spec
        )

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._limit_concurrency:
            return self.searcher.suggest(trial_id)

        assert (
            trial_id not in self.live_trials
        ), f"Trial ID {trial_id} must be unique: already found in set."
        if len(self.live_trials) >= self.max_concurrent:
            logger.debug(
                f"Not providing a suggestion for {trial_id} due to "
                "concurrency limit: %s/%s.",
                len(self.live_trials),
                self.max_concurrent,
            )
            return

        suggestion = self.searcher.suggest(trial_id)
        if suggestion not in (None, Searcher.FINISHED):
            self.live_trials.add(trial_id)
            self.num_unfinished_live_trials += 1
        return suggestion

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        if not self._limit_concurrency:
            return self.searcher.on_trial_complete(trial_id, result=result, error=error)

        if trial_id not in self.live_trials:
            return
        elif self.batch:
            self.cached_results[trial_id] = (result, error)
            self.num_unfinished_live_trials -= 1
            if self.num_unfinished_live_trials <= 0:
                # Update the underlying searcher once the
                # full batch is completed.
                for trial_id, (result, error) in self.cached_results.items():
                    self.searcher.on_trial_complete(
                        trial_id, result=result, error=error
                    )
                    self.live_trials.remove(trial_id)
                self.cached_results = {}
                self.num_unfinished_live_trials = 0
            else:
                return
        else:
            self.searcher.on_trial_complete(trial_id, result=result, error=error)
            self.live_trials.remove(trial_id)
            self.num_unfinished_live_trials -= 1

    def on_trial_result(self, trial_id: str, result: Dict) -> None:
        self.searcher.on_trial_result(trial_id, result)

    def add_evaluated_point(
        self,
        parameters: Dict,
        value: float,
        error: bool = False,
        pruned: bool = False,
        intermediate_values: Optional[List[float]] = None,
    ):
        return self.searcher.add_evaluated_point(
            parameters, value, error, pruned, intermediate_values
        )

    def get_state(self) -> Dict:
        state = self.__dict__.copy()
        del state["searcher"]
        return copy.deepcopy(state)

    def set_state(self, state: Dict):
        self.__dict__.update(state)

    def save(self, checkpoint_path: str):
        self.searcher.save(checkpoint_path)

    def restore(self, checkpoint_path: str):
        self.searcher.restore(checkpoint_path)

    # BOHB Specific.
    # TODO(team-ml): Refactor alongside HyperBandForBOHB
    def on_pause(self, trial_id: str):
        self.searcher.on_pause(trial_id)

    def on_unpause(self, trial_id: str):
        self.searcher.on_unpause(trial_id)
