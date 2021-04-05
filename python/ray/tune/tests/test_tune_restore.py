# coding: utf-8
import signal
from collections import Counter
import os
import shutil
import tempfile
import time
import unittest

import skopt
import numpy as np
from hyperopt import hp
from nevergrad.optimization import optimizerlib
from zoopt import ValueType
from hebo.design_space.design_space import DesignSpace as HEBODesignSpace

import ray
from ray import tune
from ray.test_utils import recursive_fnmatch
from ray.rllib import _register_all
from ray.tune.callback import Callback
from ray.tune.suggest.basic_variant import BasicVariantGenerator
from ray.tune.suggest import ConcurrencyLimiter, Searcher
from ray.tune.suggest.hyperopt import HyperOptSearch
from ray.tune.suggest.dragonfly import DragonflySearch
from ray.tune.suggest.bayesopt import BayesOptSearch
from ray.tune.suggest.skopt import SkOptSearch
from ray.tune.suggest.nevergrad import NevergradSearch
from ray.tune.suggest.optuna import OptunaSearch, param as ot_param
from ray.tune.suggest.sigopt import SigOptSearch
from ray.tune.suggest.zoopt import ZOOptSearch
from ray.tune.suggest.hebo import HEBOSearch
from ray.tune.utils import validate_save_restore
from ray.tune.utils._mock_trainable import MyTrainableClass


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


class TuneInterruptionTest(unittest.TestCase):
    def setUp(self) -> None:
        # Wait up to five seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"

    def testExperimentInterrupted(self):
        import multiprocessing

        trainer_semaphore = multiprocessing.Semaphore()
        driver_semaphore = multiprocessing.Semaphore()

        class SteppingCallback(Callback):
            def on_step_end(self, iteration, trials, **info):
                driver_semaphore.release()  # Driver should continue
                trainer_semaphore.acquire()  # Wait until released

        def _run(local_dir):
            def _train(config):
                for i in range(7):
                    tune.report(val=i)

            tune.run(
                _train,
                local_dir=local_dir,
                name="interrupt",
                callbacks=[SteppingCallback()])

        local_dir = tempfile.mkdtemp()
        process = multiprocessing.Process(target=_run, args=(local_dir, ))
        process.daemon = False
        process.start()

        exp_dir = os.path.join(local_dir, "interrupt")

        # Skip first five steps
        for i in range(5):
            driver_semaphore.acquire()  # Wait for callback
            trainer_semaphore.release()  # Continue training

        driver_semaphore.acquire()

        experiment_state_file = None
        for file in os.listdir(exp_dir):
            if file.startswith("experiment_state"):
                experiment_state_file = os.path.join(exp_dir, file)
                break

        self.assertTrue(experiment_state_file)
        last_mtime = os.path.getmtime(experiment_state_file)

        # Now send kill signal
        os.kill(process.pid, signal.SIGINT)
        # Release trainer. It should handle the signal and try to
        # checkpoint the experiment
        trainer_semaphore.release()

        time.sleep(2)  # Wait for checkpoint
        new_mtime = os.path.getmtime(experiment_state_file)

        self.assertNotEqual(last_mtime, new_mtime)

        shutil.rmtree(local_dir)


class TuneFailResumeGridTest(unittest.TestCase):
    class FailureInjectorCallback(Callback):
        """Adds random failure injection to the TrialExecutor."""

        def __init__(self, steps=20):
            self._step = 0
            self.steps = steps

        def on_trial_start(self, trials, **info):
            self._step += 1
            if self._step >= self.steps:
                print(f"Failing after step {self._step} with "
                      f"{len(trials)} trials")
                raise RuntimeError

    class CheckStateCallback(Callback):
        """Checks state for the experiment initialization."""

        def __init__(self, expected_trials=20):
            self.expected_trials = expected_trials
            self._checked = False

        def on_step_begin(self, iteration, trials, **kwargs):
            if not self._checked:
                assert len(trials) == self.expected_trials
                self._checked = True

    def setUp(self):
        self.logdir = tempfile.mkdtemp()
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"
        # Wait up to 1.5 seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "1.5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"

        # Change back to local_mode=True after this is resolved:
        # https://github.com/ray-project/ray/issues/13932
        ray.init(local_mode=False, num_cpus=2)

        from ray.tune import register_trainable
        register_trainable("trainable", MyTrainableClass)

    def tearDown(self):
        os.environ.pop("TUNE_GLOBAL_CHECKPOINT_S")
        shutil.rmtree(self.logdir)
        ray.shutdown()

    def testFailResumeGridSearch(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        config = dict(
            num_samples=3,
            fail_fast=True,
            config={
                "test": tune.grid_search([1, 2, 3]),
                "test2": tune.grid_search([1, 2, 3]),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1)

        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[self.FailureInjectorCallback()],
                **config)

        analysis = tune.run(
            "trainable",
            resume=True,
            callbacks=[self.CheckStateCallback()],
            **config)
        assert len(analysis.trials) == 27
        test_counter = Counter([t.config["test"] for t in analysis.trials])
        assert all(v == 9 for v in test_counter.values())
        test2_counter = Counter([t.config["test2"] for t in analysis.trials])
        assert all(v == 9 for v in test2_counter.values())

    def testFailResumeWithPreset(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        search_alg = BasicVariantGenerator(points_to_evaluate=[{
            "test": -1,
            "test2": -1
        }, {
            "test": -1
        }, {
            "test2": -1
        }])

        config = dict(
            num_samples=3 + 3,  # 3 preset, 3 samples
            fail_fast=True,
            config={
                "test": tune.grid_search([1, 2, 3]),
                "test2": tune.grid_search([1, 2, 3]),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1)
        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[self.FailureInjectorCallback(5)],
                search_alg=search_alg,
                **config)

        analysis = tune.run(
            "trainable",
            resume=True,
            callbacks=[self.CheckStateCallback(expected_trials=5)],
            search_alg=search_alg,
            **config)
        assert len(analysis.trials) == 34
        test_counter = Counter([t.config["test"] for t in analysis.trials])
        assert test_counter.pop(-1) == 4
        assert all(v == 10 for v in test_counter.values())
        test2_counter = Counter([t.config["test2"] for t in analysis.trials])
        assert test2_counter.pop(-1) == 4
        assert all(v == 10 for v in test2_counter.values())

    def testFailResumeAfterPreset(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        search_alg = BasicVariantGenerator(points_to_evaluate=[{
            "test": -1,
            "test2": -1
        }, {
            "test": -1
        }, {
            "test2": -1
        }])

        config = dict(
            num_samples=3 + 3,  # 3 preset, 3 samples
            fail_fast=True,
            config={
                "test": tune.grid_search([1, 2, 3]),
                "test2": tune.grid_search([1, 2, 3]),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1)

        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[self.FailureInjectorCallback(15)],
                search_alg=search_alg,
                **config)

        analysis = tune.run(
            "trainable",
            resume=True,
            callbacks=[self.CheckStateCallback(expected_trials=15)],
            search_alg=search_alg,
            **config)
        assert len(analysis.trials) == 34
        test_counter = Counter([t.config["test"] for t in analysis.trials])
        assert test_counter.pop(-1) == 4
        assert all(v == 10 for v in test_counter.values())
        test2_counter = Counter([t.config["test2"] for t in analysis.trials])
        assert test2_counter.pop(-1) == 4
        assert all(v == 10 for v in test2_counter.values())

    def testMultiExperimentFail(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        experiments = []
        for i in range(3):
            experiments.append(
                tune.Experiment(
                    run=MyTrainableClass,
                    name="trainable",
                    num_samples=2,
                    config={
                        "test": tune.grid_search([1, 2, 3]),
                    },
                    stop={"training_iteration": 1},
                    local_dir=self.logdir))

        with self.assertRaises(RuntimeError):
            tune.run(
                experiments,
                callbacks=[self.FailureInjectorCallback(10)],
                fail_fast=True)

        analysis = tune.run(
            experiments,
            resume=True,
            callbacks=[self.CheckStateCallback(expected_trials=10)],
            fail_fast=True)
        assert len(analysis.trials) == 18

    def testWarningLargeGrid(self):
        config = dict(
            num_samples=3,
            fail_fast=True,
            config={
                "test": tune.grid_search(list(range(20))),
                "test2": tune.grid_search(list(range(20))),
                "test3": tune.grid_search(list(range(20))),
                "test4": tune.grid_search(list(range(20))),
                "test5": tune.grid_search(list(range(20))),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1)
        with self.assertWarnsRegex(UserWarning,
                                   "exceeds the serialization threshold"):
            with self.assertRaises(RuntimeError):
                tune.run(
                    "trainable",
                    callbacks=[self.FailureInjectorCallback(10)],
                    **config)


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

    # def testPyTorchMNIST(self):
    #     from ray.tune.examples.mnist_pytorch_trainable import TrainMNIST
    #     from torchvision import datasets
    #     datasets.MNIST("~/data", train=True, download=True)
    #     validate_save_restore(TrainMNIST)
    #     validate_save_restore(TrainMNIST, use_object_store=True)

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

        # Unfortunately, SigOpt doesn't allow setting of random state. Thus,
        # we always end up with different suggestions, which is unsuitable
        # for the warm start test. Here we make do with points_to_evaluate,
        # and ensure that state is preserved over checkpoints and restarts.
        points = [
            {
                "width": 5,
                "height": 20
            },
            {
                "width": 10,
                "height": -20
            },
            {
                "width": 15,
                "height": 30
            },
            {
                "width": 5,
                "height": -30
            },
            {
                "width": 10,
                "height": 40
            },
            {
                "width": 15,
                "height": -40
            },
            {
                "width": 5,
                "height": 50
            },
            {
                "width": 10,
                "height": -50
            },
            {
                "width": 15,
                "height": 60
            },
            {
                "width": 12,
                "height": -60
            },
        ]

        search_alg = SigOptSearch(
            space,
            name="SigOpt Example Experiment",
            max_concurrent=1,
            metric="loss",
            mode="min",
            points_to_evaluate=points)
        return search_alg, cost

    def testWarmStart(self):
        if "SIGOPT_KEY" not in os.environ:
            self.skipTest("No SigOpt API key found in environment.")
            return

        super().testWarmStart()

    def testRestore(self):
        if "SIGOPT_KEY" not in os.environ:
            self.skipTest("No SigOpt API key found in environment.")
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


class HEBOWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space_config = [
            {
                "name": "width",
                "type": "num",
                "lb": 0,
                "ub": 20
            },
            {
                "name": "height",
                "type": "num",
                "lb": -100,
                "ub": 100
            },
        ]
        space = HEBODesignSpace().parse(space_config)

        def cost(param, reporter):
            reporter(loss=(param["height"] - 14)**2 - abs(param["width"] - 3))

        search_alg = HEBOSearch(
            space=space, metric="loss", mode="min", random_state_seed=5)

        return search_alg, cost


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
    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
