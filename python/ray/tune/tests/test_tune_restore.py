# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import tempfile
import unittest

import ray
from ray import tune
from ray.tune.util import recursive_fnmatch, validate_save_restore
from ray.rllib import _register_all
from hyperopt import hp
from ray.tune.suggest.hyperopt import HyperOptSearch


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

    def testTensorFlowMNIST(self):
        from ray.tune.examples.tune_mnist_ray_hyperband import TrainMNIST
        validate_save_restore(TrainMNIST)
        validate_save_restore(TrainMNIST, use_object_store=True)

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


class HyperoptWarmStartTest(unittest.TestCase):
    def setUp(self):
        ray.init(local_mode=True)
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        ray.shutdown()
        _register_all()

    def set_basic_conf(self):
        space = {
            "x": hp.uniform("x", 0, 10),
            "y": hp.uniform("y", -10, 10),
            "z": hp.uniform("z", -10, 0)
        }

        def cost(space, reporter):
            loss = space["x"]**2 + space["y"]**2 + space["z"]**2
            reporter(loss=loss)

        return space, cost

    def run_exp_1(self):
        space, cost = self.set_basic_conf()
        algo_1 = HyperOptSearch(
            space,
            max_concurrent=1,
            metric="loss",
            mode="min",
            random_state_seed=5)
        results_exp_1 = tune.run(cost, num_samples=15, search_alg=algo_1)
        self.log_dir = os.path.join(self.tmpdir, "trials_algo1.pkl")
        algo_1.save(self.log_dir)
        return results_exp_1

    def run_exp_2(self):
        space, cost = self.set_basic_conf()
        algo_2 = HyperOptSearch(
            space,
            max_concurrent=1,
            metric="loss",
            mode="min",
            random_state_seed=5)
        algo_2.restore(self.log_dir)
        return tune.run(cost, num_samples=15, search_alg=algo_2)

    def run_exp_3(self):
        space, cost = self.set_basic_conf()
        algo_3 = HyperOptSearch(
            space,
            max_concurrent=1,
            metric="loss",
            mode="min",
            random_state_seed=5)
        return tune.run(cost, num_samples=30, search_alg=algo_3)

    def testHyperoptWarmStart(self):
        results_exp_1 = self.run_exp_1()
        results_exp_2 = self.run_exp_2()
        results_exp_3 = self.run_exp_3()
        trials_1_config = [trial.config for trial in results_exp_1.trials]
        trials_2_config = [trial.config for trial in results_exp_2.trials]
        trials_3_config = [trial.config for trial in results_exp_3.trials]
        self.assertEqual(trials_1_config + trials_2_config, trials_3_config)


if __name__ == "__main__":
    unittest.main(verbosity=2)
