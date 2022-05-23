import random
import unittest

from ray.tune import register_trainable
from ray.tune.automl import SearchSpace, DiscreteSpace, GridSearch


def next_trials(searcher):
    trials = []
    while not searcher.is_finished():
        trial = searcher.next_trial()
        if not trial:
            break
        trials.append(trial)
    return trials


class AutoMLSearcherTest(unittest.TestCase):
    def setUp(self):
        def dummy_train(config, reporter):
            reporter(timesteps_total=100, done=True)

        register_trainable("f1", dummy_train)

    def testExpandSearchSpace(self):
        exp = {"test-exp": {"run": "f1", "config": {"a": {"d": "dummy"}}}}
        space = SearchSpace(
            [
                DiscreteSpace("a.b.c", [1, 2]),
                DiscreteSpace("a.d", ["a", "b"]),
            ]
        )
        searcher = GridSearch(space, "reward")
        searcher.add_configurations(exp)
        trials = next_trials(searcher)

        self.assertEqual(len(trials), 4)
        self.assertTrue(trials[0].config["a"]["b"]["c"] in [1, 2])
        self.assertTrue(trials[1].config["a"]["d"] in ["a", "b"])

    def testSearchRound(self):
        exp = {"test-exp": {"run": "f1", "config": {"a": {"d": "dummy"}}}}
        space = SearchSpace(
            [
                DiscreteSpace("a.b.c", [1, 2]),
                DiscreteSpace("a.d", ["a", "b"]),
            ]
        )
        searcher = GridSearch(space, "reward")
        searcher.add_configurations(exp)
        trials = next_trials(searcher)

        self.assertEqual(searcher.next_trial(), None)
        for trial in trials[1:]:
            searcher.on_trial_complete(trial.trial_id)
        searcher.on_trial_complete(trials[0].trial_id, error=True)

        self.assertTrue(searcher.is_finished())

    def testBestTrial(self):
        exp = {"test-exp": {"run": "f1", "config": {"a": {"d": "dummy"}}}}
        space = SearchSpace(
            [
                DiscreteSpace("a.b.c", [1, 2]),
                DiscreteSpace("a.d", ["a", "b"]),
            ]
        )
        searcher = GridSearch(space, "reward")
        searcher.add_configurations(exp)
        trials = next_trials(searcher)

        self.assertEqual(searcher.next_trial(), None)
        for i, trial in enumerate(trials):
            print("TRIAL {}".format(trial))
            rewards = list(range(i, i + 10))
            random.shuffle(rewards)
            for reward in rewards:
                searcher.on_trial_result(trial.trial_id, {"reward": reward})

        best_trial = searcher.get_best_trial()
        self.assertEqual(best_trial, trials[-1])
        self.assertEqual(best_trial.best_result["reward"], 3 + 10 - 1)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
