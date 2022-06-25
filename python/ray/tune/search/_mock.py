from typing import Dict, List, Optional

from ray.tune.search import Searcher, ConcurrencyLimiter
from ray.tune.search.search_generator import SearchGenerator
from ray.tune.experiment import Trial


class _MockSearcher(Searcher):
    def __init__(self, **kwargs):
        self.live_trials = {}
        self.counter = {"result": 0, "complete": 0}
        self.final_results = []
        self.stall = False
        self.results = []
        super(_MockSearcher, self).__init__(**kwargs)

    def suggest(self, trial_id: str):
        if not self.stall:
            self.live_trials[trial_id] = 1
            return {"test_variable": 2}
        return None

    def on_trial_result(self, trial_id: str, result: Dict):
        self.counter["result"] += 1
        self.results += [result]

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        self.counter["complete"] += 1
        if result:
            self._process_result(result)
        if trial_id in self.live_trials:
            del self.live_trials[trial_id]

    def _process_result(self, result: Dict):
        self.final_results += [result]


class _MockSuggestionAlgorithm(SearchGenerator):
    def __init__(self, max_concurrent: Optional[int] = None, **kwargs):
        self.searcher = _MockSearcher(**kwargs)
        if max_concurrent:
            self.searcher = ConcurrencyLimiter(
                self.searcher, max_concurrent=max_concurrent
            )
        super(_MockSuggestionAlgorithm, self).__init__(self.searcher)

    @property
    def live_trials(self) -> List[Trial]:
        return self.searcher.live_trials

    @property
    def results(self) -> List[Dict]:
        return self.searcher.results
