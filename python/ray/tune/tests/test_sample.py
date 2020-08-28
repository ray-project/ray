import numpy as np
import unittest

from ray import tune


class SearchSpaceTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testBoundedFloat(self):
        bounded = tune.sample.Float(-4.2, 8.3)

        # Don't allow to specify more than one sampler
        with self.assertRaises(ValueError):
            bounded.normal().uniform()

        # Normal
        samples = bounded.normal(-4, 2).sample(size=1000)
        self.assertTrue(any(-4.2 < s < 8.3 for s in samples))
        self.assertTrue(np.mean(samples) < -2)

        # Uniform
        samples = bounded.uniform().sample(size=1000)
        self.assertTrue(any(-4.2 < s < 8.3 for s in samples))
        self.assertFalse(np.mean(samples) < -2)

        # Loguniform
        with self.assertRaises(ValueError):
            bounded.loguniform().sample(size=1000)

        bounded_positive = tune.sample.Float(1e-4, 1e-1)
        samples = bounded_positive.loguniform().sample(size=1000)
        self.assertTrue(any(1e-4 < s < 1e-1 for s in samples))

    def testUnboundedFloat(self):
        unbounded = tune.sample.Float()

        # Require min and max bounds for loguniform
        with self.assertRaises(ValueError):
            unbounded.loguniform()

    def testBoundedInt(self):
        bounded = tune.sample.Integer(-3, 12)

        samples = bounded.uniform().sample(size=1000)
        self.assertTrue(any(-3 <= s < 12 for s in samples))
        self.assertFalse(np.mean(samples) < 2)

    def testCategorical(self):
        categories = [-2, -1, 0, 1, 2]
        cat = tune.sample.Categorical(categories)

        samples = cat.uniform().sample(size=1000)
        self.assertTrue(any([-2 <= s <= 2 for s in samples]))
        self.assertTrue(all([c in samples for c in categories]))

    def testIterative(self):
        categories = [-2, -1, 0, 1, 2]

        def test_iter():
            for i in categories:
                yield i

        itr = tune.sample.Iterative(test_iter())
        samples = itr.uniform().sample(size=5)
        self.assertTrue(any([-2 <= s <= 2 for s in samples]))
        self.assertTrue(all([c in samples for c in categories]))

        itr = tune.sample.Iterative(iter(categories))
        samples = itr.uniform().sample(size=5)
        self.assertTrue(any([-2 <= s <= 2 for s in samples]))
        self.assertTrue(all([c in samples for c in categories]))

    def testFunction(self):
        def sample(spec):
            return np.random.uniform(-4, 4)

        fnc = tune.sample.Function(sample)

        samples = fnc.uniform().sample(size=1000)
        self.assertTrue(any([-4 < s < 4 for s in samples]))
        self.assertTrue(-2 < np.mean(samples) < 2)

    def testQuantized(self):
        bounded_positive = tune.sample.Float(1e-4, 1e-1)
        samples = bounded_positive.loguniform().quantized(5e-4).sample(size=10)

        for sample in samples:
            factor = sample / 5e-4
            self.assertAlmostEqual(factor, round(factor), places=10)

    def testConvertAx(self):
        from ray.tune.suggest.ax import AxSearch
        from ax.service.ax_client import AxClient

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = AxSearch.convert_search_space(config)
        ax_config = [
            {
                "name": "a",
                "type": "choice",
                "values": [2, 3, 4]
            },
            {
                "name": "b/x",
                "type": "range",
                "bounds": [0, 5],
                "value_type": "int"
            },
            {
                "name": "b/y",
                "type": "fixed",
                "value": 4
            },
            {
                "name": "b/z",
                "type": "range",
                "bounds": [1e-4, 1e-2],
                "value_type": "float",
                "log_scale": True
            },
        ]

        client1 = AxClient(random_seed=1234)
        client1.create_experiment(parameters=converted_config)
        searcher1 = AxSearch(client1)

        client2 = AxClient(random_seed=1234)
        client2.create_experiment(parameters=ax_config)
        searcher2 = AxSearch(client2)

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertEqual(config1["b"]["y"], 4)
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

    def testConvertHyperOpt(self):
        from ray.tune.suggest.hyperopt import HyperOptSearch
        from hyperopt import hp

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = HyperOptSearch.convert_search_space(config)
        hyperopt_config = {
            "a": hp.choice("a", [2, 3, 4]),
            "b": {
                "x": hp.randint("x", 5),
                "y": 4,
                "z": hp.loguniform("z", np.log(1e-4), np.log(1e-2))
            }
        }

        searcher1 = HyperOptSearch(
            space=converted_config, random_state_seed=1234)
        searcher2 = HyperOptSearch(
            space=hyperopt_config, random_state_seed=1234)

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertEqual(config1["b"]["y"], 4)
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

    def testConvertOptuna(self):
        from ray.tune.suggest.optuna import OptunaSearch, param
        from optuna.samplers import RandomSampler

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = OptunaSearch.convert_search_space(config)
        optuna_config = [
            param.suggest_categorical("a", [2, 3, 4]),
            param.suggest_int("b/x", 0, 5, 2),
            param.suggest_loguniform("b/z", 1e-4, 1e-2)
        ]

        sampler1 = RandomSampler(seed=1234)
        searcher1 = OptunaSearch(
            space=converted_config, sampler=sampler1, config=config)

        sampler2 = RandomSampler(seed=1234)
        searcher2 = OptunaSearch(
            space=optuna_config, sampler=sampler2, config=config)

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertEqual(config1["b"]["y"], 4)
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
