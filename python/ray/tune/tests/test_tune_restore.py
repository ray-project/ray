# coding: utf-8
import os
import shutil
import tempfile
import unittest
import skopt
import numpy as np
from hyperopt import hp
from nevergrad.optimization import optimizerlib
from zoopt import ValueType

import ray
from ray import tune
from ray.test_utils import recursive_fnmatch
from ray.rllib import _register_all
from ray.tune.suggest import ConcurrencyLimiter, Searcher
from ray.tune.suggest.hyperopt import HyperOptSearch
from ray.tune.suggest.dragonfly import DragonflySearch
from ray.tune.suggest.bayesopt import BayesOptSearch
from ray.tune.suggest.skopt import SkOptSearch
from ray.tune.suggest.nevergrad import NevergradSearch
from ray.tune.suggest.optuna import OptunaSearch, param as ot_param
from ray.tune.suggest.sigopt import SigOptSearch
from ray.tune.suggest.zoopt import ZOOptSearch
from ray.tune.utils import validate_save_restore


class TuneRestoreTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0, local_mode=True)
        tmpdir = tempfile.mkdtemp()
        test_name = "TuneRestoreTest"
        tune.run(
            "PG",
            name=test_name,
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir=tmpdir,
            config={
                "env": "CartPole-v0",
                "framework": "tf",
            },
        )

        logdir = os.path.expanduser(os.path.join(tmpdir, test_name))
        self.logdir = logdir
        self.checkpoint_path = recursive_fnmatch(logdir, "checkpoint-1")[0]

    def tearDown(self):
        shutil.rmtree(self.logdir)
        ray.shutdown()
        _register_all()

    def testTuneRestore(self):
        self.assertTrue(os.path.isfile(self.checkpoint_path))
        tune.run(
            "PG",
            name="TuneRestoreTest",
            stop={"training_iteration": 2},  # train one more iteration.
            checkpoint_freq=1,
            restore=self.checkpoint_path,  # Restore the checkpoint
            config={
                "env": "CartPole-v0",
                "framework": "tf",
            },
        )

    def testPostRestoreCheckpointExistence(self):
        """Tests that checkpoint restored from is not deleted post-restore."""
        self.assertTrue(os.path.isfile(self.checkpoint_path))
        tune.run(
            "PG",
            name="TuneRestoreTest",
            stop={"training_iteration": 2},
            checkpoint_freq=1,
            keep_checkpoints_num=1,
            restore=self.checkpoint_path,
            config={
                "env": "CartPole-v0",
                "framework": "tf",
            },
        )
        self.assertTrue(os.path.isfile(self.checkpoint_path))


class TuneExampleTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=2)

    def tearDown(self):
        ray.shutdown()
        _register_all()

    def testPBTKeras(self):
        from ray.tune.examples.pbt_tune_cifar10_with_keras import Cifar10Model
        from tensorflow.python.keras.datasets import cifar10
        cifar10.load_data()
        validate_save_restore(Cifar10Model)
        validate_save_restore(Cifar10Model, use_object_store=True)

    def testPyTorchMNIST(self):
        from ray.tune.examples.mnist_pytorch_trainable import TrainMNIST
        from torchvision import datasets
        datasets.MNIST("~/data", train=True, download=True)
        validate_save_restore(TrainMNIST)
        validate_save_restore(TrainMNIST, use_object_store=True)

    def testLogging(self):
        from ray.tune.examples.logging_example import MyTrainableClass
        validate_save_restore(MyTrainableClass)
        validate_save_restore(MyTrainableClass, use_object_store=True)

    def testHyperbandExample(self):
        from ray.tune.examples.hyperband_example import MyTrainableClass
        validate_save_restore(MyTrainableClass)
        validate_save_restore(MyTrainableClass, use_object_store=True)

    def testAsyncHyperbandExample(self):
        from ray.tune.utils.mock import MyTrainableClass
        validate_save_restore(MyTrainableClass)
        validate_save_restore(MyTrainableClass, use_object_store=True)


class AutoInitTest(unittest.TestCase):
    def testTuneRestore(self):
        self.assertFalse(ray.is_initialized())
        tune.run("__fake", name="TestAutoInit", stop={"training_iteration": 1})
        self.assertTrue(ray.is_initialized())

    def tearDown(self):
        ray.shutdown()
        _register_all()


class AbstractWarmStartTest:
    def setUp(self):
        ray.init(num_cpus=1, local_mode=True)
        self.tmpdir = tempfile.mkdtemp()
        self.experiment_name = "results"

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        ray.shutdown()
        _register_all()

    def set_basic_conf(self):
        raise NotImplementedError()

    def run_part_from_scratch(self):
        np.random.seed(162)
        search_alg, cost = self.set_basic_conf()
        search_alg = ConcurrencyLimiter(search_alg, 1)
        results_exp_1 = tune.run(
            cost,
            num_samples=5,
            search_alg=search_alg,
            verbose=0,
            name=self.experiment_name,
            local_dir=self.tmpdir)
        checkpoint_path = os.path.join(self.tmpdir, "warmStartTest.pkl")
        search_alg.save(checkpoint_path)
        return results_exp_1, np.random.get_state(), checkpoint_path

    def run_from_experiment_restore(self, random_state):
        search_alg, cost = self.set_basic_conf()
        search_alg = ConcurrencyLimiter(search_alg, 1)
        search_alg.restore_from_dir(
            os.path.join(self.tmpdir, self.experiment_name))
        results = tune.run(
            cost,
            num_samples=5,
            search_alg=search_alg,
            verbose=0,
            name=self.experiment_name,
            local_dir=self.tmpdir)
        return results

    def run_explicit_restore(self, random_state, checkpoint_path):
        np.random.set_state(random_state)
        search_alg2, cost = self.set_basic_conf()
        search_alg2 = ConcurrencyLimiter(search_alg2, 1)
        search_alg2.restore(checkpoint_path)
        return tune.run(cost, num_samples=5, search_alg=search_alg2, verbose=0)

    def run_full(self):
        np.random.seed(162)
        search_alg3, cost = self.set_basic_conf()
        search_alg3 = ConcurrencyLimiter(search_alg3, 1)
        return tune.run(
            cost, num_samples=10, search_alg=search_alg3, verbose=0)

    def testWarmStart(self):
        results_exp_1, r_state, checkpoint_path = self.run_part_from_scratch()
        results_exp_2 = self.run_explicit_restore(r_state, checkpoint_path)
        results_exp_3 = self.run_full()
        trials_1_config = [trial.config for trial in results_exp_1.trials]
        trials_2_config = [trial.config for trial in results_exp_2.trials]
        trials_3_config = [trial.config for trial in results_exp_3.trials]
        self.assertEqual(trials_1_config + trials_2_config, trials_3_config)

    def testRestore(self):
        results_exp_1, r_state, checkpoint_path = self.run_part_from_scratch()
        results_exp_2 = self.run_from_experiment_restore(r_state)
        results_exp_3 = self.run_full()

        trials_1_config = [trial.config for trial in results_exp_1.trials]
        trials_2_config = [trial.config for trial in results_exp_2.trials]
        trials_3_config = [trial.config for trial in results_exp_3.trials]
        self.assertEqual(trials_1_config + trials_2_config, trials_3_config)


class HyperoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = {
            "x": hp.uniform("x", 0, 10),
            "y": hp.uniform("y", -10, 10),
            "z": hp.uniform("z", -10, 0)
        }

        def cost(space, reporter):
            loss = space["x"]**2 + space["y"]**2 + space["z"]**2
            reporter(loss=loss)

        search_alg = HyperOptSearch(
            space,
            metric="loss",
            mode="min",
            random_state_seed=5,
            n_initial_points=1,
            max_concurrent=1000  # Here to avoid breaking back-compat.
        )
        return search_alg, cost


class BayesoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self, analysis=None):
        space = {"width": (0, 20), "height": (-100, 100)}

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14)**2 - abs(space["width"] - 3))

        search_alg = BayesOptSearch(
            space, metric="loss", mode="min", analysis=analysis)
        return search_alg, cost

    def testBootStrapAnalysis(self):
        analysis = self.run_full()
        search_alg3, cost = self.set_basic_conf(analysis)
        search_alg3 = ConcurrencyLimiter(search_alg3, 1)
        tune.run(cost, num_samples=10, search_alg=search_alg3, verbose=0)


class SkoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        optimizer = skopt.Optimizer([(0, 20), (-100, 100)])
        previously_run_params = [[10, 0], [15, -20]]
        known_rewards = [-189, -1144]

        def cost(space, reporter):
            reporter(loss=(space["height"]**2 + space["width"]**2))

        search_alg = SkOptSearch(
            optimizer,
            ["width", "height"],
            metric="loss",
            mode="min",
            max_concurrent=1000,  # Here to avoid breaking back-compat.
            points_to_evaluate=previously_run_params,
            evaluated_rewards=known_rewards)
        return search_alg, cost


class NevergradWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        instrumentation = 2
        parameter_names = ["height", "width"]
        optimizer = optimizerlib.OnePlusOne(instrumentation)

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14)**2 - abs(space["width"] - 3))

        search_alg = NevergradSearch(
            optimizer,
            parameter_names,
            metric="loss",
            mode="min",
            max_concurrent=1000,  # Here to avoid breaking back-compat.
        )
        return search_alg, cost


class OptunaWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        from optuna.samplers import TPESampler
        space = [
            ot_param.suggest_uniform("width", 0, 20),
            ot_param.suggest_uniform("height", -100, 100)
        ]

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14)**2 - abs(space["width"] - 3))

        search_alg = OptunaSearch(
            space, sampler=TPESampler(seed=10), metric="loss", mode="min")
        return search_alg, cost


class DragonflyWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        from dragonfly.opt.gp_bandit import EuclideanGPBandit
        from dragonfly.exd.experiment_caller import EuclideanFunctionCaller
        from dragonfly import load_config

        def cost(space, reporter):
            height, width = space["point"]
            reporter(loss=(height - 14)**2 - abs(width - 3))

        domain_vars = [{
            "name": "height",
            "type": "float",
            "min": -10,
            "max": 10
        }, {
            "name": "width",
            "type": "float",
            "min": 0,
            "max": 20
        }]

        domain_config = load_config({"domain": domain_vars})

        func_caller = EuclideanFunctionCaller(
            None, domain_config.domain.list_of_domains[0])
        optimizer = EuclideanGPBandit(func_caller, ask_tell_mode=True)
        search_alg = DragonflySearch(
            optimizer,
            metric="loss",
            mode="min",
            max_concurrent=1000,  # Here to avoid breaking back-compat.
        )
        return search_alg, cost

    @unittest.skip("Skip because this doesn't seem to work.")
    def testWarmStart(self):
        pass

    @unittest.skip("Skip because this doesn't seem to work.")
    def testRestore(self):
        pass


class SigOptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = [
            {
                "name": "width",
                "type": "int",
                "bounds": {
                    "min": 0,
                    "max": 20
                },
            },
            {
                "name": "height",
                "type": "int",
                "bounds": {
                    "min": -100,
                    "max": 100
                },
            },
        ]

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14)**2 - abs(space["width"] - 3))

        search_alg = SigOptSearch(
            space,
            name="SigOpt Example Experiment",
            max_concurrent=1,
            metric="loss",
            mode="min")
        return search_alg, cost

    def testWarmStart(self):
        if ("SIGOPT_KEY" not in os.environ):
            return

        super().testWarmStart()

    def testRestore(self):
        if ("SIGOPT_KEY" not in os.environ):
            return
        super().testRestore()


class ZOOptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        dim_dict = {
            "height": (ValueType.CONTINUOUS, [-100, 100], 1e-2),
            "width": (ValueType.DISCRETE, [0, 20], False)
        }

        def cost(param, reporter):
            reporter(loss=(param["height"] - 14)**2 - abs(param["width"] - 3))

        search_alg = ZOOptSearch(
            algo="Asracos",  # only support ASRacos currently
            budget=200,
            dim_dict=dim_dict,
            metric="loss",
            mode="min")

        return search_alg, cost

    @unittest.skip("Skip because this seems to have leaking state.")
    def testRestore(self):
        pass


class SearcherTest(unittest.TestCase):
    class MockSearcher(Searcher):
        def __init__(self, data):
            self.data = data

        def save(self, path):
            with open(path, "w") as f:
                f.write(self.data)

        def restore(self, path):
            with open(path, "r") as f:
                self.data = f.read()

    def testSaveRestoreDir(self):
        tmpdir = tempfile.mkdtemp()
        original_data = "hello-its-me"
        searcher = self.MockSearcher(original_data)
        searcher.save_to_dir(tmpdir)
        searcher_2 = self.MockSearcher("no-its-not-me")
        searcher_2.restore_from_dir(tmpdir)
        assert searcher_2.data == original_data


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
