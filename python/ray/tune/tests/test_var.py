from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import unittest

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.experiment import Experiment
from ray.tune.suggest import grid_search, BasicVariantGenerator
from ray.tune.suggest.suggestion import _MockSuggestionAlgorithm
from ray.tune.suggest.variant_generator import (RecursiveDependencyError,
                                                resolve_nested_dict)


class VariantGeneratorTest(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def generate_trials(self, spec, name):
        suggester = BasicVariantGenerator()
        suggester.add_configurations({name: spec})
        return suggester.next_trials()

    def testParseToTrials(self):
        trials = self.generate_trials({
            "run": "PPO",
            "num_samples": 2,
            "max_failures": 5,
            "config": {
                "env": "Pong-v0",
                "foo": "bar"
            },
        }, "tune-pong")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertTrue("PPO_Pong-v0" in str(trials[0]))
        self.assertEqual(trials[0].config, {"foo": "bar", "env": "Pong-v0"})
        self.assertEqual(trials[0].trainable_name, "PPO")
        self.assertEqual(trials[0].experiment_tag, "0")
        self.assertEqual(trials[0].max_failures, 5)
        self.assertEqual(trials[0].evaluated_params, {})
        self.assertEqual(trials[0].local_dir,
                         os.path.join(DEFAULT_RESULTS_DIR, "tune-pong"))
        self.assertEqual(trials[1].experiment_tag, "1")

    def testEval(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "foo": {
                    "eval": "2 + 2"
                },
            },
        }, "eval")
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"foo": 4})
        self.assertEqual(trials[0].evaluated_params, {"foo": 4})
        self.assertEqual(trials[0].experiment_tag, "0_foo=4")

    def testGridSearch(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "bar": {
                    "grid_search": [True, False]
                },
                "foo": {
                    "grid_search": [1, 2, 3]
                },
                "baz": "asd",
            },
        }, "grid_search")
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {
            "bar": True,
            "foo": 1,
            "baz": "asd",
        })
        self.assertEqual(trials[0].evaluated_params, {
            "bar": True,
            "foo": 1,
        })
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1")

        self.assertEqual(trials[1].config, {
            "bar": False,
            "foo": 1,
            "baz": "asd",
        })
        self.assertEqual(trials[1].evaluated_params, {
            "bar": False,
            "foo": 1,
        })
        self.assertEqual(trials[1].experiment_tag, "1_bar=False,foo=1")

        self.assertEqual(trials[2].config, {
            "bar": True,
            "foo": 2,
            "baz": "asd",
        })
        self.assertEqual(trials[2].evaluated_params, {
            "bar": True,
            "foo": 2,
        })

        self.assertEqual(trials[3].config, {
            "bar": False,
            "foo": 2,
            "baz": "asd",
        })
        self.assertEqual(trials[3].evaluated_params, {
            "bar": False,
            "foo": 2,
        })

        self.assertEqual(trials[4].config, {
            "bar": True,
            "foo": 3,
            "baz": "asd",
        })
        self.assertEqual(trials[4].evaluated_params, {
            "bar": True,
            "foo": 3,
        })

        self.assertEqual(trials[5].config, {
            "bar": False,
            "foo": 3,
            "baz": "asd",
        })
        self.assertEqual(trials[5].evaluated_params, {
            "bar": False,
            "foo": 3,
        })

    def testGridSearchAndEval(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "qux": tune.sample_from(lambda spec: 2 + 2),
                "bar": grid_search([True, False]),
                "foo": grid_search([1, 2, 3]),
                "baz": "asd",
            },
        }, "grid_eval")
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {
            "bar": True,
            "foo": 1,
            "qux": 4,
            "baz": "asd",
        })
        self.assertEqual(trials[0].evaluated_params, {
            "bar": True,
            "foo": 1,
            "qux": 4,
        })
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1,qux=4")

    def testConditionResolution(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": 1,
                "y": tune.sample_from(lambda spec: spec.config.x + 1),
                "z": tune.sample_from(lambda spec: spec.config.y + 1),
            },
        }, "condition_resolution")
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"x": 1, "y": 2, "z": 3})
        self.assertEqual(trials[0].evaluated_params, {"y": 2, "z": 3})
        self.assertEqual(trials[0].experiment_tag, "0_y=2,z=3")

    def testDependentLambda(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": grid_search([1, 2]),
                "y": tune.sample_from(lambda spec: spec.config.x * 100),
            },
        }, "dependent_lambda")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].config, {"x": 1, "y": 100})
        self.assertEqual(trials[1].config, {"x": 2, "y": 200})

    def testDependentGridSearch(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": grid_search([
                    tune.sample_from(lambda spec: spec.config.y * 100),
                    tune.sample_from(lambda spec: spec.config.y * 200)
                ]),
                "y": tune.sample_from(lambda spec: 1),
            },
        }, "dependent_grid_search")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].config, {"x": 100, "y": 1})
        self.assertEqual(trials[1].config, {"x": 200, "y": 1})

    def testNestedValues(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": {
                    "y": {
                        "z": tune.sample_from(lambda spec: 1)
                    }
                },
                "y": tune.sample_from(lambda spec: 12),
                "z": tune.sample_from(lambda spec: spec.config.x.y.z * 100),
            },
        }, "nested_values")
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {
            "x": {
                "y": {
                    "z": 1
                }
            },
            "y": 12,
            "z": 100
        })
        self.assertEqual(trials[0].evaluated_params, {
            "x/y/z": 1,
            "y": 12,
            "z": 100
        })

    def testLogUniform(self):
        sampler = tune.loguniform(1e-10, 1e-1).func
        results = [sampler(None) for i in range(1000)]
        assert abs(np.log(min(results)) / np.log(10) - -10) < 0.1
        assert abs(np.log(max(results)) / np.log(10) - -1) < 0.1

        sampler_e = tune.loguniform(np.e**-4, np.e, base=np.e).func
        results_e = [sampler_e(None) for i in range(1000)]
        assert abs(np.log(min(results_e)) - -4) < 0.1
        assert abs(np.log(max(results_e)) - 1) < 0.1

    def test_resolve_dict(self):
        config = {
            "a": {
                "b": 1,
                "c": 2,
            },
            "b": {
                "a": 3
            }
        }
        resolved = resolve_nested_dict(config)
        for k, v in [(("a", "b"), 1), (("a", "c"), 2), (("b", "a"), 3)]:
            self.assertEqual(resolved.get(k), v)

    def testRecursiveDep(self):
        try:
            list(
                self.generate_trials({
                    "run": "PPO",
                    "config": {
                        "foo": tune.sample_from(lambda spec: spec.config.foo),
                    },
                }, "recursive_dep"))
        except RecursiveDependencyError as e:
            assert "`foo` recursively depends on" in str(e), e
        else:
            assert False

    def testMaxConcurrentSuggestions(self):
        """Checks that next_trials() supports throttling."""
        experiment_spec = {
            "run": "PPO",
            "num_samples": 6,
        }
        experiments = [Experiment.from_json("test", experiment_spec)]

        searcher = _MockSuggestionAlgorithm(max_concurrent=4)
        searcher.add_configurations(experiments)
        trials = searcher.next_trials()
        self.assertEqual(len(trials), 4)
        self.assertEqual(searcher.next_trials(), [])

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)
        self.assertEqual(len(searcher.next_trials()), 1)

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)

        finished_trial = trials.pop()
        searcher.on_trial_complete(finished_trial.trial_id)
        self.assertEqual(len(searcher.next_trials()), 1)
        self.assertEqual(len(searcher.next_trials()), 0)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
