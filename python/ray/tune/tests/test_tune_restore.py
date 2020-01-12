# coding: utf-8
import os
import shutil
import tempfile
import unittest
import skopt
import numpy as np
from hyperopt import hp
from nevergrad.optimization import optimizerlib

import ray
from ray import tune
from ray.test_utils import recursive_fnmatch
from ray.rllib import _register_all
from ray.tune.suggest.hyperopt import HyperOptSearch
from ray.tune.suggest.bayesopt import BayesOptSearch
from ray.tune.suggest.skopt import SkOptSearch
from ray.tune.suggest.nevergrad import NevergradSearch
from ray.tune.suggest.sigopt import SigOptSearch
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
            },
        )


class TuneExampleTest(unittest.TestCase):
    def setUp(self):
        ray.init()

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
        from ray.tune.examples.async_hyperband_example import MyTrainableClass
        validate_save_restore(MyTrainableClass)
        validate_save_restore(MyTrainableClass, use_object_store=True)


class AutoInitTest(unittest.TestCase):
    def testTuneRestore(self):
        self.assertFalse(ray.is_initialized())
        tune.run(
            "__fake",
            name="TestAutoInit",
            stop={"training_iteration": 1},
            ray_auto_init=True)
        self.assertTrue(ray.is_initialized())

    def tearDown(self):
        ray.shutdown()
        _register_all()


class AbstractWarmStartTest:
    def setUp(self):
        ray.init(local_mode=True)
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        ray.shutdown()
        _register_all()

    def set_basic_conf(self):
        raise NotImplementedError()

    def run_exp_1(self):
        np.random.seed(162)
        search_alg, cost = self.set_basic_conf()
        results_exp_1 = tune.run(cost, num_samples=15, search_alg=search_alg)
        self.log_dir = os.path.join(self.tmpdir, "warmStartTest.pkl")
        search_alg.save(self.log_dir)
        return results_exp_1

    def run_exp_2(self):
        search_alg2, cost = self.set_basic_conf()
        search_alg2.restore(self.log_dir)
        return tune.run(cost, num_samples=15, search_alg=search_alg2)

    def run_exp_3(self):
        np.random.seed(162)
        search_alg3, cost = self.set_basic_conf()
        return tune.run(cost, num_samples=30, search_alg=search_alg3)

    def testWarmStart(self):
        results_exp_1 = self.run_exp_1()
        results_exp_2 = self.run_exp_2()
        results_exp_3 = self.run_exp_3()
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
            max_concurrent=1,
            metric="loss",
            mode="min",
            random_state_seed=5)
        return search_alg, cost


class BayesoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = {"width": (0, 20), "height": (-100, 100)}

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14)**2 - abs(space["width"] - 3))

        search_alg = BayesOptSearch(
            space,
            max_concurrent=1,
            metric="loss",
            mode="min",
            utility_kwargs={
                "kind": "ucb",
                "kappa": 2.5,
                "xi": 0.0
            })
        return search_alg, cost


class SkoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        optimizer = skopt.Optimizer([(0, 20), (-100, 100)])
        previously_run_params = [[10, 0], [15, -20]]
        known_rewards = [-189, -1144]

        def cost(space, reporter):
            reporter(loss=(space["height"]**2 + space["width"]**2))

        search_alg = SkOptSearch(
            optimizer, ["width", "height"],
            max_concurrent=1,
            metric="loss",
            mode="min",
            points_to_evaluate=previously_run_params,
            evaluated_rewards=known_rewards)
        return search_alg, cost


class NevergradWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        instrumentation = 2
        parameter_names = ["height", "width"]
        optimizer = optimizerlib.OnePlusOne(instrumentation)

        def cost(space, reporter):
            reporter(
                mean_loss=(space["height"] - 14)**2 - abs(space["width"] - 3))

        search_alg = NevergradSearch(
            optimizer,
            parameter_names,
            max_concurrent=1,
            metric="mean_loss",
            mode="min")
        return search_alg, cost


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
            reporter(
                mean_loss=(space["height"] - 14)**2 - abs(space["width"] - 3))

        search_alg = SigOptSearch(
            space,
            name="SigOpt Example Experiment",
            max_concurrent=1,
            metric="mean_loss",
            mode="min")
        return search_alg, cost

    def testWarmStart(self):
        if ("SIGOPT_KEY" not in os.environ):
            return

        super().testWarmStart()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
