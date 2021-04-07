import numpy as np
import unittest

from ray import tune
from ray.tune import Experiment
from ray.tune.suggest.variant_generator import generate_variants


def _mock_objective(config):
    tune.report(**config)


class SearchSpaceTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testTuneSampleAPI(self):
        config = {
            "func": tune.sample_from(lambda spec: spec.config.uniform * 0.01),
            "uniform": tune.uniform(-5, -1),
            "quniform": tune.quniform(3.2, 5.4, 0.2),
            "loguniform": tune.loguniform(1e-4, 1e-2),
            "qloguniform": tune.qloguniform(1e-4, 1e-1, 5e-5),
            "choice": tune.choice([2, 3, 4]),
            "randint": tune.randint(-9, 15),
            "lograndint": tune.lograndint(1, 10),
            "qrandint": tune.qrandint(-21, 12, 3),
            "qlograndint": tune.qlograndint(2, 20, 2),
            "randn": tune.randn(10, 2),
            "qrandn": tune.qrandn(10, 2, 0.2),
        }
        for _, (_, generated) in zip(
                range(1000), generate_variants({
                    "config": config
                })):
            out = generated["config"]

            self.assertAlmostEqual(out["func"], out["uniform"] * 0.01)

            self.assertGreaterEqual(out["uniform"], -5)
            self.assertLess(out["uniform"], -1)

            self.assertGreaterEqual(out["quniform"], 3.2)
            self.assertLessEqual(out["quniform"], 5.4)
            self.assertAlmostEqual(out["quniform"] / 0.2,
                                   round(out["quniform"] / 0.2))

            self.assertGreaterEqual(out["loguniform"], 1e-4)
            self.assertLess(out["loguniform"], 1e-2)

            self.assertGreaterEqual(out["qloguniform"], 1e-4)
            self.assertLessEqual(out["qloguniform"], 1e-1)
            self.assertAlmostEqual(out["qloguniform"] / 5e-5,
                                   round(out["qloguniform"] / 5e-5))

            self.assertIn(out["choice"], [2, 3, 4])

            self.assertGreaterEqual(out["randint"], -9)
            self.assertLess(out["randint"], 15)
            self.assertTrue(isinstance(out["randint"], int))

            self.assertGreaterEqual(out["lograndint"], 1)
            self.assertLess(out["lograndint"], 10)
            self.assertTrue(isinstance(out["lograndint"], int))

            self.assertGreaterEqual(out["qrandint"], -21)
            self.assertLessEqual(out["qrandint"], 12)
            self.assertEqual(out["qrandint"] % 3, 0)
            self.assertTrue(isinstance(out["qrandint"], int))

            self.assertGreaterEqual(out["qlograndint"], 2)
            self.assertLessEqual(out["qlograndint"], 20)
            self.assertEqual(out["qlograndint"] % 2, 0)
            self.assertTrue(isinstance(out["qlograndint"], int))

            # Very improbable
            self.assertGreater(out["randn"], 0)
            self.assertLess(out["randn"], 20)

            self.assertGreater(out["qrandn"], 0)
            self.assertLess(out["qrandn"], 20)
            self.assertAlmostEqual(out["qrandn"] / 0.2,
                                   round(out["qrandn"] / 0.2))

    def testBoundedFloat(self):
        bounded = tune.sample.Float(-4.2, 8.3)

        # Don't allow to specify more than one sampler
        with self.assertRaises(ValueError):
            bounded.normal().uniform()

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
        unbounded = tune.sample.Float(None, None)

        # Require min and max bounds for loguniform
        with self.assertRaises(ValueError):
            unbounded.loguniform()

        # Normal
        samples = tune.sample.Float(None, None).normal().sample(size=1000)
        self.assertTrue(any(-5 < s < 5 for s in samples))
        self.assertTrue(-1 < np.mean(samples) < 1)

    def testBoundedInt(self):
        bounded = tune.sample.Integer(-3, 12)

        samples = bounded.uniform().sample(size=1000)
        self.assertTrue(any(-3 <= s < 12 for s in samples))
        self.assertFalse(np.mean(samples) < 2)

    def testCategorical(self):
        categories = [-2, -1, 0, 1, 2]
        cat = tune.sample.Categorical(categories)

        samples = cat.uniform().sample(size=1000)
        self.assertTrue(any(-2 <= s <= 2 for s in samples))
        self.assertTrue(all(c in samples for c in categories))

    def testFunction(self):
        def sample(spec):
            return np.random.uniform(-4, 4)

        fnc = tune.sample.Function(sample)

        samples = fnc.sample(size=1000)
        self.assertTrue(any(-4 < s < 4 for s in samples))
        self.assertTrue(-2 < np.mean(samples) < 2)

    def testFunctionSignature(self):
        from functools import partial

        def sample_a():
            return 0

        def sample_b(spec):
            return 1

        def sample_c(spec, b="ok"):
            return 2

        def sample_d_invalid(spec, b):
            return 3

        sample_d_valid = partial(sample_d_invalid, b="ok")

        for sample_fn in [sample_a, sample_b, sample_c, sample_d_valid]:
            fn = tune.sample_from(sample_fn)
            sample = fn.sample(None)
            self.assertIsNotNone(sample)

        with self.assertRaises(ValueError):
            fn = tune.sample_from(sample_d_invalid)
            print(fn.sample(None))

    def testQuantized(self):
        bounded_positive = tune.sample.Float(1e-4, 1e-1)

        bounded = tune.sample.Float(1e-4, 1e-1)
        with self.assertRaises(ValueError):
            # Granularity too high
            bounded.quantized(5e-4)

        with self.assertRaises(ValueError):
            tune.sample.Float(-1e-1, -1e-4).quantized(5e-4)

        samples = bounded_positive.loguniform().quantized(5e-5).sample(
            size=1000)

        for sample in samples:
            factor = sample / 5e-5
            assert 1e-4 <= sample <= 1e-1
            self.assertAlmostEqual(factor, round(factor), places=10)

        with self.assertRaises(ValueError):
            tune.sample.Float(0, 32).quantized(3)

        samples = tune.sample.Float(0, 33).quantized(3).sample(size=1000)
        self.assertTrue(all(0 <= s <= 33 for s in samples))

    def testCategoricalSeedInTrainingLoop(self):
        def train(config):
            return 0

        config = {
            "integer": tune.randint(0, 100_000),
            "choice": tune.choice(list(range(100_000)))
        }

        np.random.seed(1000)

        out_1 = tune.run(train, config=config, num_samples=8, verbose=0)

        integers_1 = [t.config["integer"] for t in out_1.trials]
        choices_1 = [t.config["choice"] for t in out_1.trials]

        np.random.seed(1000)

        out_2 = tune.run(train, config=config, num_samples=8, verbose=0)

        integers_2 = [t.config["integer"] for t in out_2.trials]
        choices_2 = [t.config["choice"] for t in out_2.trials]

        self.assertSequenceEqual(integers_1, integers_2)
        self.assertSequenceEqual(choices_1, choices_2)

    def testConvertAx(self):
        from ray.tune.suggest.ax import AxSearch
        from ax.service.ax_client import AxClient

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            AxSearch.convert_search_space({"grid": tune.grid_search([0, 1])})

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
        client1.create_experiment(
            parameters=converted_config, objective_name="a", minimize=False)
        searcher1 = AxSearch(ax_client=client1)

        client2 = AxClient(random_seed=1234)
        client2.create_experiment(
            parameters=ax_config, objective_name="a", minimize=False)
        searcher2 = AxSearch(ax_client=client2)

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertEqual(config1["b"]["y"], 4)
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = AxSearch(metric="a", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        assert trial.config["a"] in [2, 3, 4]

        mixed_config = {"a": tune.uniform(5, 6), "b": tune.uniform(8, 9)}
        searcher = AxSearch(space=mixed_config, metric="a", mode="max")
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertBayesOpt(self):
        from ray.tune.suggest.bayesopt import BayesOptSearch

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            BayesOptSearch.convert_search_space({
                "grid": tune.grid_search([0, 1])
            })

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        with self.assertRaises(ValueError):
            converted_config = BayesOptSearch.convert_search_space(config)

        config = {"b": {"z": tune.sample.Float(1e-4, 1e-2).loguniform()}}
        bayesopt_config = {"b/z": (1e-4, 1e-2)}
        converted_config = BayesOptSearch.convert_search_space(config)

        searcher1 = BayesOptSearch(
            space=converted_config, metric="none", mode="max")
        searcher2 = BayesOptSearch(
            space=bayesopt_config, metric="none", mode="max")

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = BayesOptSearch()

        invalid_config = {"a/b": tune.uniform(4.0, 8.0)}

        with self.assertRaises(ValueError):
            searcher.set_search_properties("none", "max", invalid_config)

        invalid_config = {"a": {"b/c": tune.uniform(4.0, 8.0)}}

        with self.assertRaises(ValueError):
            searcher.set_search_properties("none", "max", invalid_config)

        searcher = BayesOptSearch(metric="b/z", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        self.assertLess(trial.config["b"]["z"], 1e-2)

        mixed_config = {"a": tune.uniform(5, 6), "b": (8., 9.)}
        searcher = BayesOptSearch(space=mixed_config, metric="a", mode="max")
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertBOHB(self):
        from ray.tune.suggest.bohb import TuneBOHB
        import ConfigSpace

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            TuneBOHB.convert_search_space({"grid": tune.grid_search([0, 1])})

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = TuneBOHB.convert_search_space(config)
        bohb_config = ConfigSpace.ConfigurationSpace()
        bohb_config.add_hyperparameters([
            ConfigSpace.CategoricalHyperparameter("a", [2, 3, 4]),
            ConfigSpace.UniformIntegerHyperparameter(
                "b/x", lower=0, upper=4, q=2),
            ConfigSpace.UniformFloatHyperparameter(
                "b/z", lower=1e-4, upper=1e-2, log=True)
        ])

        converted_config.seed(1234)
        bohb_config.seed(1234)

        searcher1 = TuneBOHB(space=converted_config, metric="a", mode="max")
        searcher2 = TuneBOHB(space=bohb_config, metric="a", mode="max")

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = TuneBOHB(metric="a", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        self.assertIn(trial.config["a"], [2, 3, 4])
        self.assertEqual(trial.config["b"]["y"], 4)

        mixed_config = {
            "a": tune.uniform(5, 6),
            "b": tune.uniform(8, 9)  # Cannot mix ConfigSpace and Dict
        }
        searcher = TuneBOHB(space=mixed_config, metric="a", mode="max")
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertDragonfly(self):
        from ray.tune.suggest.dragonfly import DragonflySearch

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            DragonflySearch.convert_search_space({
                "grid": tune.grid_search([0, 1])
            })

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        with self.assertRaises(ValueError):
            converted_config = DragonflySearch.convert_search_space(config)

        config = {
            "a": 4,
            "b": {
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        dragonfly_config = [{
            "name": "b/z",
            "type": "float",
            "min": 1e-4,
            "max": 1e-2
        }]
        converted_config = DragonflySearch.convert_search_space(config)

        np.random.seed(1234)
        searcher1 = DragonflySearch(
            optimizer="bandit",
            domain="euclidean",
            space=converted_config,
            metric="none",
            mode="max")

        config1 = searcher1.suggest("0")

        np.random.seed(1234)
        searcher2 = DragonflySearch(
            optimizer="bandit",
            domain="euclidean",
            space=dragonfly_config,
            metric="none",
            mode="max")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertLess(config2["b"]["z"], 1e-2)

        searcher = DragonflySearch()
        invalid_config = {"a/b": tune.uniform(4.0, 8.0)}
        with self.assertRaises(ValueError):
            searcher.set_search_properties("none", "max", invalid_config)
        invalid_config = {"a": {"b/c": tune.uniform(4.0, 8.0)}}
        with self.assertRaises(ValueError):
            searcher.set_search_properties("none", "max", invalid_config)

        searcher = DragonflySearch(
            optimizer="bandit", domain="euclidean", metric="a", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        self.assertLess(trial.config["b"]["z"], 1e-2)

        mixed_config = {
            "a": tune.uniform(5, 6),
            "b": tune.uniform(8, 9)  # Cannot mix List and Dict
        }
        searcher = DragonflySearch(
            space=mixed_config,
            optimizer="bandit",
            domain="euclidean",
            metric="a",
            mode="max")
        config = searcher.suggest("0")

        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertHEBO(self):
        from ray.tune.suggest.hebo import HEBOSearch
        from hebo.design_space.design_space import DesignSpace
        import torch

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            HEBOSearch.convert_search_space({"grid": tune.grid_search([0, 1])})

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = HEBOSearch.convert_search_space(config)
        hebo_space_config = [
            {
                "name": "a",
                "type": "cat",
                "categories": [2, 3, 4]
            },
            {
                "name": "b/x",
                "type": "int",
                "lb": 0,
                "ub": 5
            },
            {
                "name": "b/z",
                "type": "pow",
                "lb": 1e-4,
                "ub": 1e-2
            },
        ]
        hebo_space = DesignSpace().parse(hebo_space_config)

        searcher1 = HEBOSearch(
            space=converted_config,
            metric="a",
            mode="max",
            random_state_seed=123)
        searcher2 = HEBOSearch(
            space=hebo_space, metric="a", mode="max", random_state_seed=123)

        np.random.seed(1234)
        torch.manual_seed(1234)
        config1 = searcher1.suggest("0")
        np.random.seed(1234)
        torch.manual_seed(1234)
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = HEBOSearch(metric="a", mode="max", random_state_seed=123)
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        self.assertIn(trial.config["a"], [2, 3, 4])
        self.assertEqual(trial.config["b"]["y"], 4)

        # Mixed configs are not supported

    def testConvertHyperOpt(self):
        from ray.tune.suggest.hyperopt import HyperOptSearch
        from hyperopt import hp

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            HyperOptSearch.convert_search_space({
                "grid": tune.grid_search([0, 1])
            })

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(-15, -10),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = HyperOptSearch.convert_search_space(config)
        hyperopt_config = {
            "a": hp.choice("a", [2, 3, 4]),
            "b": {
                "x": hp.uniformint("x", -15, -10),
                "y": 4,
                "z": hp.loguniform("z", np.log(1e-4), np.log(1e-2))
            }
        }

        searcher1 = HyperOptSearch(
            space=converted_config,
            random_state_seed=1234,
            metric="a",
            mode="max")
        searcher2 = HyperOptSearch(
            space=hyperopt_config,
            random_state_seed=1234,
            metric="a",
            mode="max")

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(-15, -10)))
        self.assertEqual(config1["b"]["y"], 4)
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = HyperOptSearch(metric="a", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        assert trial.config["a"] in [2, 3, 4]

        mixed_config = {"a": tune.uniform(5, 6), "b": hp.uniform("b", 8, 9)}
        searcher = HyperOptSearch(space=mixed_config, metric="a", mode="max")
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertHyperOptNested(self):
        from ray.tune.suggest.hyperopt import HyperOptSearch

        config = {
            "a": 1,
            "dict_nested": tune.sample.Categorical([{
                "a": tune.sample.Categorical(["M", "N"]),
                "b": tune.sample.Categorical(["O", "P"])
            }]).uniform(),
            "list_nested": tune.sample.Categorical([
                [
                    tune.sample.Categorical(["M", "N"]),
                    tune.sample.Categorical(["O", "P"])
                ],
                [
                    tune.sample.Categorical(["Q", "R"]),
                    tune.sample.Categorical(["S", "T"])
                ],
            ]).uniform(),
            "domain_nested": tune.sample.Categorical([
                tune.sample.Categorical(["M", "N"]),
                tune.sample.Categorical(["O", "P"])
            ]).uniform(),
        }

        searcher = HyperOptSearch(metric="a", mode="max")
        analysis = tune.run(
            _mock_objective,
            config=config,
            search_alg=searcher,
            num_samples=10)

        for trial in analysis.trials:
            config = trial.config

            self.assertIn(config["dict_nested"]["a"], ["M", "N"])
            self.assertIn(config["dict_nested"]["b"], ["O", "P"])

            if config["list_nested"][0] in ["M", "N"]:
                self.assertIn(config["list_nested"][1], ["O", "P"])
            else:
                self.assertIn(config["list_nested"][0], ["Q", "R"])
                self.assertIn(config["list_nested"][1], ["S", "T"])

            self.assertIn(config["domain_nested"], ["M", "N", "O", "P"])

    def testConvertNevergrad(self):
        from ray.tune.suggest.nevergrad import NevergradSearch
        import nevergrad as ng

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            NevergradSearch.convert_search_space({
                "grid": tune.grid_search([0, 1])
            })

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = NevergradSearch.convert_search_space(config)
        nevergrad_config = ng.p.Dict(
            a=ng.p.Choice([2, 3, 4]),
            b=ng.p.Dict(
                x=ng.p.Scalar(lower=0, upper=5).set_integer_casting(),
                z=ng.p.Log(lower=1e-4, upper=1e-2)))

        searcher1 = NevergradSearch(
            optimizer=ng.optimizers.OnePlusOne,
            space=converted_config,
            metric="a",
            mode="max")
        searcher2 = NevergradSearch(
            optimizer=ng.optimizers.OnePlusOne,
            space=nevergrad_config,
            metric="a",
            mode="max")

        np.random.seed(1234)
        config1 = searcher1.suggest("0")
        np.random.seed(1234)
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = NevergradSearch(
            optimizer=ng.optimizers.OnePlusOne, metric="a", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        assert trial.config["a"] in [2, 3, 4]

        mixed_config = {
            "a": tune.uniform(5, 6),
            "b": tune.uniform(8, 9)  # Cannot mix Nevergrad cfg and tune
        }
        searcher = NevergradSearch(
            space=mixed_config,
            optimizer=ng.optimizers.OnePlusOne,
            metric="a",
            mode="max")
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertOptuna(self):
        from ray.tune.suggest.optuna import OptunaSearch, param
        import optuna
        from optuna.samplers import RandomSampler

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            OptunaSearch.convert_search_space({
                "grid": tune.grid_search([0, 1])
            })

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = OptunaSearch.convert_search_space(config)
        optuna_config = {
            "a": optuna.distributions.CategoricalDistribution([2, 3, 4]),
            "b": {
                "x": optuna.distributions.IntUniformDistribution(0, 5, step=2),
                "z": optuna.distributions.LogUniformDistribution(1e-4, 1e-2)
            }
        }
        legacy_optuna_config = [
            param.suggest_categorical("a", [2, 3, 4]),
            param.suggest_int("b/x", 0, 5, 2),
            param.suggest_loguniform("b/z", 1e-4, 1e-2)
        ]

        sampler1 = RandomSampler(seed=1234)
        searcher1 = OptunaSearch(
            space=converted_config, sampler=sampler1, metric="a", mode="max")

        sampler2 = RandomSampler(seed=1234)
        searcher2 = OptunaSearch(
            space=optuna_config, sampler=sampler2, metric="a", mode="max")

        sampler3 = RandomSampler(seed=1234)
        searcher3 = OptunaSearch(
            space=legacy_optuna_config,
            sampler=sampler3,
            metric="a",
            mode="max")

        config1 = searcher1.suggest("0")
        config2 = searcher2.suggest("0")
        config3 = searcher3.suggest("0")

        self.assertEqual(config1, config2)
        self.assertEqual(config1, config3)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = OptunaSearch(metric="a", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        assert trial.config["a"] in [2, 3, 4]

        mixed_config = {
            "a": tune.uniform(5, 6),
            "b": tune.uniform(8, 9)  # Cannot mix List and Dict
        }
        searcher = OptunaSearch(space=mixed_config, metric="a", mode="max")
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertSkOpt(self):
        from ray.tune.suggest.skopt import SkOptSearch
        from skopt.space import Real, Integer

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            SkOptSearch.convert_search_space({
                "grid": tune.grid_search([0, 1])
            })

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5),
                "y": 4,
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        converted_config = SkOptSearch.convert_search_space(config)
        skopt_config = {
            "a": [2, 3, 4],
            "b/x": Integer(0, 5),
            "b/z": Real(1e-4, 1e-2, prior="log-uniform")
        }

        searcher1 = SkOptSearch(space=converted_config, metric="a", mode="max")
        searcher2 = SkOptSearch(space=skopt_config, metric="a", mode="max")

        np.random.seed(1234)
        config1 = searcher1.suggest("0")
        np.random.seed(1234)
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["a"], [2, 3, 4])
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertLess(1e-4, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 1e-2)

        searcher = SkOptSearch(metric="a", mode="max")
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        self.assertIn(trial.config["a"], [2, 3, 4])
        self.assertEqual(trial.config["b"]["y"], 4)

        mixed_config = {"a": tune.uniform(5, 6), "b": (8, 9)}
        searcher = SkOptSearch(space=mixed_config, metric="a", mode="max")
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def testConvertZOOpt(self):
        from ray.tune.suggest.zoopt import ZOOptSearch
        from zoopt import ValueType

        # Grid search not supported, should raise ValueError
        with self.assertRaises(ValueError):
            ZOOptSearch.convert_search_space({
                "grid": tune.grid_search([0, 1])
            })

        config = {
            "a": tune.sample.Categorical([2, 3, 4]).uniform(),
            "b": {
                "x": tune.sample.Integer(0, 5).quantized(2),
                "y": tune.sample.Categorical([2, 4, 6, 8]).uniform(),
                "z": tune.sample.Float(1e-4, 1e-2).loguniform()
            }
        }
        # Does not support categorical variables
        with self.assertRaises(ValueError):
            converted_config = ZOOptSearch.convert_search_space(config)
        config = {
            "a": 2,
            "b": {
                "x": tune.sample.Integer(0, 5).uniform(),
                "y": tune.sample.Categorical([2, 4, 6, 8]).uniform(),
                "z": tune.sample.Float(-3, 7).uniform().quantized(1e-4)
            }
        }
        converted_config = ZOOptSearch.convert_search_space(config)

        zoopt_config = {
            "b/x": (ValueType.DISCRETE, [0, 5], True),
            "b/y": (ValueType.GRID, [2, 4, 6, 8]),
            "b/z": (ValueType.CONTINUOUS, [-3, 7], 1e-4),
        }

        zoopt_search_config = {"parallel_num": 4}

        searcher1 = ZOOptSearch(
            dim_dict=converted_config,
            budget=5,
            metric="a",
            mode="max",
            **zoopt_search_config)
        searcher2 = ZOOptSearch(
            dim_dict=zoopt_config,
            budget=5,
            metric="a",
            mode="max",
            **zoopt_search_config)

        np.random.seed(1234)
        config1 = searcher1.suggest("0")
        np.random.seed(1234)
        config2 = searcher2.suggest("0")

        self.assertEqual(config1, config2)
        self.assertIn(config1["b"]["x"], list(range(5)))
        self.assertIn(config1["b"]["y"], [2, 4, 6, 8])
        self.assertLess(-3, config1["b"]["z"])
        self.assertLess(config1["b"]["z"], 7)

        searcher = ZOOptSearch(
            budget=5, metric="a", mode="max", **zoopt_search_config)
        analysis = tune.run(
            _mock_objective, config=config, search_alg=searcher, num_samples=1)
        trial = analysis.trials[0]
        self.assertIn(trial.config["b"]["y"], [2, 4, 6, 8])

        mixed_config = {
            "a": tune.uniform(5, 6),
            "b": (ValueType.CONTINUOUS, [8, 9], 1e-4)
        }
        searcher = ZOOptSearch(
            dim_dict=mixed_config,
            budget=5,
            metric="a",
            mode="max",
            **zoopt_search_config)
        config = searcher.suggest("0")
        self.assertTrue(5 <= config["a"] <= 6)
        self.assertTrue(8 <= config["b"] <= 9)

    def _testPointsToEvaluate(self, cls, config, **kwargs):
        points_to_evaluate = [{k: v.sample()
                               for k, v in config.items()} for _ in range(2)]
        print(f"Points to evaluate: {points_to_evaluate}")
        searcher = cls(points_to_evaluate=points_to_evaluate, **kwargs)

        analysis = tune.run(
            _mock_objective,
            config=config,
            metric="metric",
            mode="max",
            search_alg=searcher,
            num_samples=5)

        for i in range(len(points_to_evaluate)):
            trial_config = analysis.trials[i].config
            trial_config_dict = {
                "metric": trial_config["metric"],
                "a": trial_config["a"],
                "b": trial_config["b"],
                "c": trial_config["c"]
            }
            self.assertDictEqual(trial_config_dict, points_to_evaluate[i])

    def testPointsToEvaluateAx(self):
        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.ax import AxSearch
        return self._testPointsToEvaluate(AxSearch, config)

    def testPointsToEvaluateBayesOpt(self):
        config = {
            "metric": tune.sample.Float(10, 20).uniform(),
            "a": tune.sample.Float(-30, -20).uniform(),
            "b": tune.sample.Float(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.bayesopt import BayesOptSearch
        return self._testPointsToEvaluate(BayesOptSearch, config)

    def testPointsToEvaluateBOHB(self):
        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.bohb import TuneBOHB
        return self._testPointsToEvaluate(TuneBOHB, config)

    def testPointsToEvaluateDragonfly(self):
        config = {
            "metric": tune.sample.Float(10, 20).uniform(),
            "a": tune.sample.Float(-30, -20).uniform(),
            "b": tune.sample.Float(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.dragonfly import DragonflySearch
        return self._testPointsToEvaluate(
            DragonflySearch, config, domain="euclidean", optimizer="bandit")

    def testPointsToEvaluateHyperOpt(self):
        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.hyperopt import HyperOptSearch
        return self._testPointsToEvaluate(HyperOptSearch, config)

    def testPointsToEvaluateNevergrad(self):
        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.nevergrad import NevergradSearch
        import nevergrad as ng
        return self._testPointsToEvaluate(
            NevergradSearch, config, optimizer=ng.optimizers.OnePlusOne)

    def testPointsToEvaluateOptuna(self):
        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.optuna import OptunaSearch
        return self._testPointsToEvaluate(OptunaSearch, config)

    def testPointsToEvaluateSkOpt(self):
        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.skopt import SkOptSearch
        return self._testPointsToEvaluate(SkOptSearch, config)

    def testPointsToEvaluateZoOpt(self):
        self.skipTest(
            "ZOOpt's latest release (0.4.1) does not support sampling "
            "initial points. Please re-enable this test after the next "
            "release.")

        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).uniform()
        }

        from ray.tune.suggest.zoopt import ZOOptSearch
        return self._testPointsToEvaluate(
            ZOOptSearch, config, budget=10, parallel_num=8)

    def testPointsToEvaluateBasicVariant(self):
        config = {
            "metric": tune.sample.Categorical([1, 2, 3, 4]).uniform(),
            "a": tune.sample.Categorical(["t1", "t2", "t3", "t4"]).uniform(),
            "b": tune.sample.Integer(0, 5),
            "c": tune.sample.Float(1e-4, 1e-1).loguniform()
        }

        from ray.tune.suggest.basic_variant import BasicVariantGenerator
        return self._testPointsToEvaluate(BasicVariantGenerator, config)

    def testPointsToEvaluateBasicVariantAdvanced(self):
        config = {
            "grid_1": tune.grid_search(["a", "b", "c", "d"]),
            "grid_2": tune.grid_search(["x", "y", "z"]),
            "nested": {
                "random": tune.uniform(2., 10.),
                "dependent": tune.sample_from(
                    lambda spec: -1. * spec.config.nested.random)
            }
        }

        points = [
            {
                "grid_1": "b"
            },
            {
                "grid_2": "z"
            },
            {
                "grid_1": "a",
                "grid_2": "y"
            },
            {
                "nested": {
                    "random": 8.0
                }
            },
        ]

        from ray.tune.suggest.basic_variant import BasicVariantGenerator

        # grid_1 * grid_2 are 3 * 4 = 12 variants per complete grid search
        # However if one grid var is set by preset variables, that run
        # is excluded from grid search.

        # Point 1 overwrites grid_1, so the first trial only grid searches
        # over grid_2 (3 trials).
        # The remaining 5 trials search over the whole space (5 * 12 trials)
        searcher = BasicVariantGenerator(points_to_evaluate=[points[0]])
        exp = Experiment(
            run=_mock_objective, name="test", config=config, num_samples=6)
        searcher.add_configurations(exp)
        self.assertEqual(searcher.total_samples, 1 * 3 + 5 * 12)

        # Point 2 overwrites grid_2, so the first trial only grid searches
        # over grid_1 (4 trials).
        # The remaining 5 trials search over the whole space (5 * 12 trials)
        searcher = BasicVariantGenerator(points_to_evaluate=[points[1]])
        exp = Experiment(
            run=_mock_objective, name="test", config=config, num_samples=6)
        searcher.add_configurations(exp)
        self.assertEqual(searcher.total_samples, 1 * 4 + 5 * 12)

        # Point 3 overwrites grid_1 and grid_2, so the first trial does not
        # grid search.
        # The remaining 5 trials search over the whole space (5 * 12 trials)
        searcher = BasicVariantGenerator(points_to_evaluate=[points[2]])
        exp = Experiment(
            run=_mock_objective, name="test", config=config, num_samples=6)
        searcher.add_configurations(exp)
        self.assertEqual(searcher.total_samples, 1 + 5 * 12)

        # When initialized with all points, the first three trials are
        # defined by the logic above. Only 3 trials are grid searched
        # compeletely.
        searcher = BasicVariantGenerator(points_to_evaluate=points)
        exp = Experiment(
            run=_mock_objective, name="test", config=config, num_samples=6)
        searcher.add_configurations(exp)
        self.assertEqual(searcher.total_samples, 1 * 3 + 1 * 4 + 1 + 3 * 12)

        # Run this and confirm results
        analysis = tune.run(exp, search_alg=searcher)
        configs = [trial.config for trial in analysis.trials]

        self.assertEqual(len(configs), searcher.total_samples)
        self.assertTrue(
            all(config["grid_1"] == "b" for config in configs[0:3]))
        self.assertTrue(
            all(config["grid_2"] == "z" for config in configs[3:7]))
        self.assertTrue(configs[7]["grid_1"] == "a"
                        and configs[7]["grid_2"] == "y")
        self.assertTrue(configs[8]["nested"]["random"] == 8.0)
        self.assertTrue(configs[8]["nested"]["dependent"] == -8.0)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
