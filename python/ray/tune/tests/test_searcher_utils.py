import pytest

from ray.tune.search import BasicVariantGenerator, ConcurrencyLimiter, Searcher
from ray.tune.search.repeater import Repeater
from ray.tune.search.search_generator import SearchGenerator


def test_nested_suggestion():
    class TestSuggestion(Searcher):
        def suggest(self, trial_id):
            return {"a": {"b": {"c": {"d": 4, "e": 5}}}}

    searcher = TestSuggestion()
    alg = SearchGenerator(searcher)
    alg.add_configurations({"test": {"run": "__fake"}})
    trial = alg.next_trial()
    assert "e=5" in trial.experiment_tag
    assert "d=4" in trial.experiment_tag


def _repeat_trials(num_samples: int, repeat: int):
    class TestSuggestion(Searcher):
        index = 0

        def suggest(self, trial_id):
            self.index += 1
            return {"test_variable": 5 + self.index}

        def on_trial_complete(self, *args, **kwargs):
            return

    searcher = TestSuggestion(metric="episode_reward_mean")
    repeat_searcher = Repeater(searcher, repeat=repeat, set_index=False)
    alg = SearchGenerator(repeat_searcher)

    alg.add_configurations(
        {
            "test": {
                "run": "__fake",
                "num_samples": num_samples,
                "stop": {"training_iteration": 1},
            }
        }
    )

    trials = []
    while not alg.is_finished():
        trials.append(alg.next_trial())

    return trials


def test_repeat_1():
    trials = _repeat_trials(num_samples=2, repeat=1)
    assert len(trials) == 2
    parameter_set = {t.evaluated_params["test_variable"] for t in trials}
    assert len(parameter_set) == 2


def test_repeat_4():
    trials = _repeat_trials(num_samples=12, repeat=4)
    assert len(trials) == 12
    parameter_set = {t.evaluated_params["test_variable"] for t in trials}
    assert len(parameter_set) == 3


def test_odd_repeat():
    trials = _repeat_trials(num_samples=11, repeat=5)
    assert len(trials) == 11
    parameter_set = {t.evaluated_params["test_variable"] for t in trials}
    assert len(parameter_set) == 3


def test_set_get_repeater():
    class TestSuggestion(Searcher):
        def __init__(self, index):
            self.index = index
            self.returned_result = []
            super().__init__(metric="result", mode="max")

        def suggest(self, trial_id):
            self.index += 1
            return {"score": self.index}

        def on_trial_complete(self, trial_id, result=None, **kwargs):
            self.returned_result.append(result)

    searcher = TestSuggestion(0)
    repeater1 = Repeater(searcher, repeat=3, set_index=False)
    for i in range(3):
        assert repeater1.suggest(f"test_{i}")["score"] == 1
    for i in range(2):  # An incomplete set of results
        assert repeater1.suggest(f"test_{i}_2")["score"] == 2

    # Restore a new one
    state = repeater1.get_state()
    del repeater1
    new_repeater = Repeater(searcher, repeat=1, set_index=True)
    new_repeater.set_state(state)
    assert new_repeater.repeat == 3
    assert new_repeater.suggest("test_2_2")["score"] == 2
    assert new_repeater.suggest("test_x")["score"] == 3

    # Report results
    for i in range(3):
        new_repeater.on_trial_complete(f"test_{i}", {"result": 2})

    for i in range(3):
        new_repeater.on_trial_complete(f"test_{i}_2", {"result": -i * 10})

    assert len(new_repeater.searcher.returned_result) == 2
    assert new_repeater.searcher.returned_result[-1] == {"result": -10}

    # Finish the rest of the last trial group
    new_repeater.on_trial_complete("test_x", {"result": 3})
    assert new_repeater.suggest("test_y")["score"] == 3
    new_repeater.on_trial_complete("test_y", {"result": 3})
    assert len(new_repeater.searcher.returned_result) == 2
    assert new_repeater.suggest("test_z")["score"] == 3
    new_repeater.on_trial_complete("test_z", {"result": 3})
    assert len(new_repeater.searcher.returned_result) == 3
    assert new_repeater.searcher.returned_result[-1] == {"result": 3}


def test_set_get_limiter():
    class TestSuggestion(Searcher):
        def __init__(self, index):
            self.index = index
            self.returned_result = []
            super().__init__(metric="result", mode="max")

        def suggest(self, trial_id):
            self.index += 1
            return {"score": self.index}

        def on_trial_complete(self, trial_id, result=None, **kwargs):
            self.returned_result.append(result)

    searcher = TestSuggestion(0)
    limiter = ConcurrencyLimiter(searcher, max_concurrent=2)
    assert limiter.suggest("test_1")["score"] == 1
    assert limiter.suggest("test_2")["score"] == 2
    assert limiter.suggest("test_3") is None

    state = limiter.get_state()
    del limiter
    limiter2 = ConcurrencyLimiter(searcher, max_concurrent=3)
    limiter2.set_state(state)
    assert limiter2.suggest("test_4") is None
    assert limiter2.suggest("test_5") is None
    limiter2.on_trial_complete("test_1", {"result": 3})
    limiter2.on_trial_complete("test_2", {"result": 3})
    assert limiter2.suggest("test_3")["score"] == 3


def test_basic_variant_limiter():
    search_alg = BasicVariantGenerator(max_concurrent=2)

    experiment_spec = {
        "run": "__fake",
        "num_samples": 5,
        "stop": {"training_iteration": 1},
    }
    search_alg.add_configurations({"test": experiment_spec})

    trial1 = search_alg.next_trial()
    assert trial1

    trial2 = search_alg.next_trial()
    assert trial2

    # Returns None because of limiting
    trial3 = search_alg.next_trial()
    assert not trial3

    # Finish trial, now trial 3 should be created
    search_alg.on_trial_complete(trial1.trial_id, None, False)
    trial3 = search_alg.next_trial()
    assert trial3

    trial4 = search_alg.next_trial()
    assert not trial4

    search_alg.on_trial_complete(trial2.trial_id, None, False)
    search_alg.on_trial_complete(trial3.trial_id, None, False)

    trial4 = search_alg.next_trial()
    assert trial4

    trial5 = search_alg.next_trial()
    assert trial5

    search_alg.on_trial_complete(trial4.trial_id, None, False)

    # Should also be None because search is finished
    trial6 = search_alg.next_trial()
    assert not trial6


def test_batch_limiter():
    class TestSuggestion(Searcher):
        def __init__(self, index):
            self.index = index
            self.returned_result = []
            super().__init__(metric="result", mode="max")

        def suggest(self, trial_id):
            self.index += 1
            return {"score": self.index}

        def on_trial_complete(self, trial_id, result=None, **kwargs):
            self.returned_result.append(result)

    searcher = TestSuggestion(0)
    limiter = ConcurrencyLimiter(searcher, max_concurrent=2, batch=True)
    assert limiter.suggest("test_1")["score"] == 1
    assert limiter.suggest("test_2")["score"] == 2
    assert limiter.suggest("test_3") is None

    limiter.on_trial_complete("test_1", {"result": 3})
    assert limiter.suggest("test_3") is None
    limiter.on_trial_complete("test_2", {"result": 3})
    assert limiter.suggest("test_3") is not None


def test_batch_limiter_infinite_loop():
    """Check whether an infinite loop when less than max_concurrent trials
    are suggested with batch mode is avoided.
    """

    class TestSuggestion(Searcher):
        def __init__(self, index, max_suggestions=10):
            self.index = index
            self.max_suggestions = max_suggestions
            self.returned_result = []
            super().__init__(metric="result", mode="max")

        def suggest(self, trial_id):
            self.index += 1
            if self.index > self.max_suggestions:
                return None
            return {"score": self.index}

        def on_trial_complete(self, trial_id, result=None, **kwargs):
            self.returned_result.append(result)
            self.index = 0

    searcher = TestSuggestion(0, 2)
    limiter = ConcurrencyLimiter(searcher, max_concurrent=5, batch=True)
    limiter.suggest("test_1")
    limiter.suggest("test_2")
    limiter.suggest("test_3")  # TestSuggestion return None

    limiter.on_trial_complete("test_1", {"result": 3})
    limiter.on_trial_complete("test_2", {"result": 3})
    assert limiter.searcher.returned_result

    searcher = TestSuggestion(0, 10)
    limiter = ConcurrencyLimiter(searcher, max_concurrent=5, batch=True)
    limiter.suggest("test_1")
    limiter.suggest("test_2")
    limiter.suggest("test_3")

    limiter.on_trial_complete("test_1", {"result": 3})
    limiter.on_trial_complete("test_2", {"result": 3})
    assert not limiter.searcher.returned_result


def test_set_max_concurrency():
    """Test whether ``set_max_concurrency`` is called by the
    ``ConcurrencyLimiter`` and works correctly.
    """

    class TestSuggestion(Searcher):
        def __init__(self, index):
            self.index = index
            self.returned_result = []
            self._max_concurrent = 1
            super().__init__(metric="result", mode="max")

        def suggest(self, trial_id):
            self.index += 1
            return {"score": self.index}

        def on_trial_complete(self, trial_id, result=None, **kwargs):
            self.returned_result.append(result)

        def set_max_concurrency(self, max_concurrent: int) -> bool:
            self._max_concurrent = max_concurrent
            return True

    searcher = TestSuggestion(0)
    limiter_max_concurrent = 2
    limiter = ConcurrencyLimiter(
        searcher, max_concurrent=limiter_max_concurrent, batch=True
    )
    assert limiter.searcher._max_concurrent == limiter_max_concurrent
    # Since set_max_concurrency returns True, ConcurrencyLimiter should not
    # be limiting concurrency itself
    assert not limiter._limit_concurrency
    assert limiter.suggest("test_1")["score"] == 1
    assert limiter.suggest("test_2")["score"] == 2
    assert limiter.suggest("test_3")["score"] == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
