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
from hebo.design_space.design_space import DesignSpace as HEBODesignSpace

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune.suggest import ConcurrencyLimiter
from ray.tune.suggest.hyperopt import HyperOptSearch
from ray.tune.suggest.dragonfly import DragonflySearch
from ray.tune.suggest.bayesopt import BayesOptSearch
from ray.tune.suggest.flaml import CFO, BlendSearch
from ray.tune.suggest.skopt import SkOptSearch
from ray.tune.suggest.nevergrad import NevergradSearch
from ray.tune.suggest.optuna import OptunaSearch
from ray.tune.suggest.sigopt import SigOptSearch
from ray.tune.suggest.zoopt import ZOOptSearch
from ray.tune.suggest.hebo import HEBOSearch
from ray.tune.suggest.ax import AxSearch
from ray.tune.suggest.bohb import TuneBOHB
from ray.tune.schedulers.hb_bohb import HyperBandForBOHB


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

    def get_scheduler(self):
        return None

    def treat_trial_config(self, trial_config):
        return trial_config

    def run_part_from_scratch(self):
        np.random.seed(162)
        search_alg, cost = self.set_basic_conf()
        if not isinstance(search_alg, ConcurrencyLimiter):
            search_alg = ConcurrencyLimiter(search_alg, 1)
        results_exp_1 = tune.run(
            cost,
            num_samples=5,
            search_alg=search_alg,
            scheduler=self.get_scheduler(),
            verbose=0,
            name=self.experiment_name,
            local_dir=self.tmpdir,
            reuse_actors=True,
        )
        checkpoint_path = os.path.join(self.tmpdir, "warmStartTest.pkl")
        search_alg.save(checkpoint_path)
        return results_exp_1, np.random.get_state(), checkpoint_path

    def run_from_experiment_restore(self, random_state):
        search_alg, cost = self.set_basic_conf()
        if not isinstance(search_alg, ConcurrencyLimiter):
            search_alg = ConcurrencyLimiter(search_alg, 1)
        search_alg.restore_from_dir(os.path.join(self.tmpdir, self.experiment_name))
        results = tune.run(
            cost,
            num_samples=5,
            search_alg=search_alg,
            scheduler=self.get_scheduler(),
            verbose=0,
            name=self.experiment_name,
            local_dir=self.tmpdir,
            reuse_actors=True,
        )
        return results

    def run_explicit_restore(self, random_state, checkpoint_path):
        np.random.set_state(random_state)
        search_alg2, cost = self.set_basic_conf()
        if not isinstance(search_alg2, ConcurrencyLimiter):
            search_alg2 = ConcurrencyLimiter(search_alg2, 1)
        search_alg2.restore(checkpoint_path)
        return tune.run(
            cost,
            num_samples=5,
            search_alg=search_alg2,
            scheduler=self.get_scheduler(),
            verbose=0,
            reuse_actors=True,
        )

    def run_full(self):
        np.random.seed(162)
        search_alg3, cost = self.set_basic_conf()
        if not isinstance(search_alg3, ConcurrencyLimiter):
            search_alg3 = ConcurrencyLimiter(search_alg3, 1)
        return tune.run(
            cost,
            num_samples=10,
            search_alg=search_alg3,
            scheduler=self.get_scheduler(),
            verbose=0,
            reuse_actors=True,
        )

    def testWarmStart(self):
        results_exp_1, r_state, checkpoint_path = self.run_part_from_scratch()
        results_exp_2 = self.run_explicit_restore(r_state, checkpoint_path)
        results_exp_3 = self.run_full()
        trials_1_config = self.treat_trial_config(
            [trial.config for trial in results_exp_1.trials]
        )
        trials_2_config = self.treat_trial_config(
            [trial.config for trial in results_exp_2.trials]
        )
        trials_3_config = self.treat_trial_config(
            [trial.config for trial in results_exp_3.trials]
        )
        self.assertEqual(trials_1_config + trials_2_config, trials_3_config)

    def testRestore(self):
        results_exp_1, r_state, checkpoint_path = self.run_part_from_scratch()
        results_exp_2 = self.run_from_experiment_restore(r_state)
        results_exp_3 = self.run_full()

        trials_1_config = self.treat_trial_config(
            [trial.config for trial in results_exp_1.trials]
        )
        trials_2_config = self.treat_trial_config(
            [trial.config for trial in results_exp_2.trials]
        )
        trials_3_config = self.treat_trial_config(
            [trial.config for trial in results_exp_3.trials]
        )
        self.assertEqual(trials_1_config + trials_2_config, trials_3_config)


class HyperoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = {
            "x": hp.uniform("x", 0, 10),
            "y": hp.uniform("y", -10, 10),
            "z": hp.uniform("z", -10, 0),
        }

        def cost(space, reporter):
            loss = space["x"] ** 2 + space["y"] ** 2 + space["z"] ** 2
            reporter(loss=loss)

        search_alg = HyperOptSearch(
            space,
            metric="loss",
            mode="min",
            random_state_seed=5,
            n_initial_points=1,
        )
        return search_alg, cost


class BayesoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self, analysis=None):
        space = {"width": (0, 20), "height": (-100, 100)}

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14) ** 2 - abs(space["width"] - 3))

        search_alg = BayesOptSearch(space, metric="loss", mode="min", analysis=analysis)
        return search_alg, cost

    def testBootStrapAnalysis(self):
        analysis = self.run_full()
        search_alg3, cost = self.set_basic_conf(analysis)
        if not isinstance(search_alg3, ConcurrencyLimiter):
            search_alg3 = ConcurrencyLimiter(search_alg3, 1)
        tune.run(
            cost, num_samples=10, search_alg=search_alg3, verbose=0, reuse_actors=True
        )


class CFOWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = {
            "height": tune.uniform(-100, 100),
            "width": tune.randint(0, 100),
        }

        def cost(param, reporter):
            reporter(loss=(param["height"] - 14) ** 2 - abs(param["width"] - 3))

        search_alg = CFO(
            space=space,
            metric="loss",
            mode="min",
            seed=20,
        )

        return search_alg, cost


class BlendSearchWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = {
            "height": tune.uniform(-100, 100),
            "width": tune.randint(0, 100),
            "time_budget_s": 10,
        }

        def cost(param, reporter):
            reporter(loss=(param["height"] - 14) ** 2 - abs(param["width"] - 3))

        search_alg = BlendSearch(
            space=space,
            metric="loss",
            mode="min",
            seed=20,
        )

        return search_alg, cost


class SkoptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        optimizer = skopt.Optimizer([(0, 20), (-100, 100)])
        previously_run_params = [[10, 0], [15, -20]]
        known_rewards = [-189, -1144]

        def cost(space, reporter):
            reporter(loss=(space["height"] ** 2 + space["width"] ** 2))

        search_alg = SkOptSearch(
            optimizer,
            ["width", "height"],
            metric="loss",
            mode="min",
            points_to_evaluate=previously_run_params,
            evaluated_rewards=known_rewards,
        )
        return search_alg, cost


class NevergradWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        instrumentation = 2
        parameter_names = ["height", "width"]
        optimizer = optimizerlib.OnePlusOne(instrumentation)

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14) ** 2 - abs(space["width"] - 3))

        search_alg = NevergradSearch(
            optimizer,
            parameter_names,
            metric="loss",
            mode="min",
        )
        return search_alg, cost


class OptunaWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        from optuna.samplers import TPESampler

        space = OptunaSearch.convert_search_space(
            {"width": tune.uniform(0, 20), "height": tune.uniform(-100, 100)}
        )

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14) ** 2 - abs(space["width"] - 3))

        search_alg = OptunaSearch(
            space, sampler=TPESampler(seed=10), metric="loss", mode="min"
        )
        return search_alg, cost


class DragonflyWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        from dragonfly.opt.gp_bandit import EuclideanGPBandit
        from dragonfly.exd.experiment_caller import EuclideanFunctionCaller
        from dragonfly import load_config

        def cost(space, reporter):
            height, width = space["point"]
            reporter(loss=(height - 14) ** 2 - abs(width - 3))

        domain_vars = [
            {"name": "height", "type": "float", "min": -10, "max": 10},
            {"name": "width", "type": "float", "min": 0, "max": 20},
        ]

        domain_config = load_config({"domain": domain_vars})

        func_caller = EuclideanFunctionCaller(
            None, domain_config.domain.list_of_domains[0]
        )
        optimizer = EuclideanGPBandit(func_caller, ask_tell_mode=True)
        search_alg = DragonflySearch(
            optimizer, metric="loss", mode="min", random_state_seed=162
        )
        return search_alg, cost

    def treat_trial_config(self, trial_config):
        return [list(x["point"]) for x in trial_config]


class SigOptWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = [
            {
                "name": "width",
                "type": "int",
                "bounds": {"min": 0, "max": 20},
            },
            {
                "name": "height",
                "type": "int",
                "bounds": {"min": -100, "max": 100},
            },
        ]

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14) ** 2 - abs(space["width"] - 3))

        # Unfortunately, SigOpt doesn't allow setting of random state. Thus,
        # we always end up with different suggestions, which is unsuitable
        # for the warm start test. Here we make do with points_to_evaluate,
        # and ensure that state is preserved over checkpoints and restarts.
        points = [
            {"width": 5, "height": 20},
            {"width": 10, "height": -20},
            {"width": 15, "height": 30},
            {"width": 5, "height": -30},
            {"width": 10, "height": 40},
            {"width": 15, "height": -40},
            {"width": 5, "height": 50},
            {"width": 10, "height": -50},
            {"width": 15, "height": 60},
            {"width": 12, "height": -60},
        ]

        search_alg = SigOptSearch(
            space,
            name="SigOpt Example Experiment",
            metric="loss",
            mode="min",
            points_to_evaluate=points,
        )
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
            "width": (ValueType.DISCRETE, [0, 20], False),
        }

        def cost(param, reporter):
            reporter(loss=(param["height"] - 14) ** 2 - abs(param["width"] - 3))

        search_alg = ZOOptSearch(
            algo="Asracos",  # only support ASRacos currently
            budget=200,
            dim_dict=dim_dict,
            metric="loss",
            mode="min",
        )

        return search_alg, cost


class HEBOWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space_config = [
            {"name": "width", "type": "num", "lb": 0, "ub": 20},
            {"name": "height", "type": "num", "lb": -100, "ub": 100},
        ]
        space = HEBODesignSpace().parse(space_config)

        def cost(param, reporter):
            reporter(loss=(param["height"] - 14) ** 2 - abs(param["width"] - 3))

        search_alg = HEBOSearch(
            space=space, metric="loss", mode="min", random_state_seed=5
        )
        # This is done on purpose to speed up the test, as HEBO will
        # cache suggestions
        search_alg = ConcurrencyLimiter(search_alg, max_concurrent=10)
        return search_alg, cost


class AxWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        from ax.service.ax_client import AxClient

        space = AxSearch.convert_search_space(
            {"width": tune.uniform(0, 20), "height": tune.uniform(-100, 100)}
        )

        from ax.modelbridge.generation_strategy import (
            GenerationStep,
            GenerationStrategy,
        )
        from ax.modelbridge.registry import Models

        # set generation strategy to sobol to ensure reproductibility
        try:
            # ax-platform>=0.2.0
            gs = GenerationStrategy(
                steps=[
                    GenerationStep(
                        model=Models.SOBOL,
                        num_trials=-1,
                        model_kwargs={"seed": 4321},
                    ),
                ]
            )
        except TypeError:
            # ax-platform<0.2.0
            gs = GenerationStrategy(
                steps=[
                    GenerationStep(
                        model=Models.SOBOL,
                        num_arms=-1,
                        model_kwargs={"seed": 4321},
                    ),
                ]
            )

        client = AxClient(random_seed=4321, generation_strategy=gs)
        client.create_experiment(parameters=space, objective_name="loss", minimize=True)

        def cost(space, reporter):
            reporter(loss=(space["height"] - 14) ** 2 - abs(space["width"] - 3))

        search_alg = AxSearch(ax_client=client)
        return search_alg, cost


class BOHBWarmStartTest(AbstractWarmStartTest, unittest.TestCase):
    def set_basic_conf(self):
        space = {"width": tune.uniform(0, 20), "height": tune.uniform(-100, 100)}

        def cost(space, reporter):
            for i in range(10):
                reporter(loss=(space["height"] - 14) ** 2 - abs(space["width"] - 3 - i))

        search_alg = TuneBOHB(space=space, metric="loss", mode="min", seed=1)

        return search_alg, cost

    def get_scheduler(self):
        return HyperBandForBOHB(max_t=10, metric="loss", mode="min")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
