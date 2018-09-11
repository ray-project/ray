from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import unittest

from ray.tune import register_trainable
from ray.tune.automl import SearchSpace, DiscreteSpace, GridSearch


class AutoMLSearcherTest(unittest.TestCase):
    def setUp(self):
        def dummy_train(config, reporter):
            reporter(timesteps_total=100, done=True)

        register_trainable("f1", dummy_train)

    def testExpandSearchSpace(self):
        exp = {"test-exp": {"run": "f1", "config": {"a": {'d': 'dummy'}}}}
        space = SearchSpace([
            DiscreteSpace('a.b.c', [1, 2]),
            DiscreteSpace('a.d', ['a', 'b']),
        ])
        searcher = GridSearch(space, 'reward')
        searcher.add_configurations(exp)
        trials = searcher.next_trials()

        self.assertEqual(len(trials), 4)
        self.assertTrue(trials[0].config['a']['b']['c'] in [1, 2])
        self.assertTrue(trials[1].config['a']['d'] in ['a', 'b'])

    def testSearchRound(self):
        exp = {"test-exp": {"run": "f1", "config": {"a": {'d': 'dummy'}}}}
        space = SearchSpace([
            DiscreteSpace('a.b.c', [1, 2]),
            DiscreteSpace('a.d', ['a', 'b']),
        ])
        searcher = GridSearch(space, 'reward')
        searcher.add_configurations(exp)
        trials = searcher.next_trials()

        self.assertEqual(len(searcher.next_trials()), 0)
        for trial in trials[1:]:
            searcher.on_trial_complete(trial.trial_id)
        searcher.on_trial_complete(trials[0].trial_id, error=True)

        self.assertTrue(searcher.is_finished())

    def testBestTrial(self):
        exp = {"test-exp": {"run": "f1", "config": {"a": {'d': 'dummy'}}}}
        space = SearchSpace([
            DiscreteSpace('a.b.c', [1, 2]),
            DiscreteSpace('a.d', ['a', 'b']),
        ])
        searcher = GridSearch(space, 'reward')
        searcher.add_configurations(exp)
        trials = searcher.next_trials()

        self.assertEqual(len(searcher.next_trials()), 0)
        for i, trial in enumerate(trials):
            rewards = [x for x in range(i, i + 10)]
            random.shuffle(rewards)
            for reward in rewards:
                searcher.on_trial_result(trial.trial_id, {"reward": reward})

        best_trial = searcher.get_best_trial()
        self.assertEqual(best_trial, trials[-1])
        self.assertEqual(best_trial.best_result['reward'], 3 + 10 - 1)
