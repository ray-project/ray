from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time
import unittest

import ray
from ray.rllib import _register_all

from ray.tune import Trainable, TuneError
from ray.tune import register_env, register_trainable, run_experiments
from ray.tune.trial_scheduler import TrialScheduler, FIFOScheduler
from ray.tune.registry import _global_registry, TRAINABLE_CLASS
from ray.tune.result import DEFAULT_RESULTS_DIR, TIMESTEPS_TOTAL, DONE
from ray.tune.util import pin_in_object_store, get_pinned_object
from ray.tune.experiment import Experiment
from ray.tune.trial import Trial, Resources
from ray.tune.trial_runner import TrialRunner
from ray.tune.suggest import grid_search, BasicVariantGenerator
from ray.tune.suggest.suggestion import _MockSuggestionAlgorithm
from ray.tune.suggest.variant_generator import RecursiveDependencyError


class TrainableFunctionApiTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, num_gpus=0)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testPinObject(self):
        X = pin_in_object_store("hello")

        @ray.remote
        def f():
            return get_pinned_object(X)

        self.assertEqual(ray.get(f.remote()), "hello")

    def testFetchPinned(self):
        X = pin_in_object_store("hello")

        def train(config, reporter):
            get_pinned_object(X)
            reporter(timesteps_total=100, done=True)

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0,
                },
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 100)

    def testRegisterEnv(self):
        register_env("foo", lambda: None)
        self.assertRaises(TypeError, lambda: register_env("foo", 2))

    def testRegisterEnvOverwrite(self):
        def train(config, reporter):
            reporter(timesteps_total=100, done=True)

        def train2(config, reporter):
            reporter(timesteps_total=200, done=True)

        register_trainable("f1", train)
        register_trainable("f1", train2)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0,
                },
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 200)

    def testRegisterTrainable(self):
        def train(config, reporter):
            pass

        class A(object):
            pass

        class B(Trainable):
            pass

        register_trainable("foo", train)
        register_trainable("foo", B)
        self.assertRaises(TypeError, lambda: register_trainable("foo", B()))
        self.assertRaises(TypeError, lambda: register_trainable("foo", A))

    def testBuiltInTrainableResources(self):
        class B(Trainable):
            @classmethod
            def default_resource_request(cls, config):
                return Resources(cpu=config["cpu"], gpu=config["gpu"])

            def _train(self):
                return dict(timesteps_this_iter=1, done=True)

        register_trainable("B", B)

        def f(cpus, gpus, queue_trials):
            return run_experiments(
                {
                    "foo": {
                        "run": "B",
                        "config": {
                            "cpu": cpus,
                            "gpu": gpus,
                        },
                    }
                },
                queue_trials=queue_trials)[0]

        # Should all succeed
        self.assertEqual(f(0, 0, False).status, Trial.TERMINATED)
        self.assertEqual(f(1, 0, True).status, Trial.TERMINATED)
        self.assertEqual(f(1, 0, True).status, Trial.TERMINATED)

        # Infeasible even with queueing enabled (no gpus)
        self.assertRaises(TuneError, lambda: f(1, 1, True))

        # Too large resource request
        self.assertRaises(TuneError, lambda: f(100, 100, False))
        self.assertRaises(TuneError, lambda: f(0, 100, False))
        self.assertRaises(TuneError, lambda: f(100, 0, False))

        # TODO(ekl) how can we test this is queued (hangs)?
        # f(100, 0, True)

    def testRewriteEnv(self):
        def train(config, reporter):
            reporter(timesteps_total=1)

        register_trainable("f1", train)

        [trial] = run_experiments({
            "foo": {
                "run": "f1",
                "env": "CartPole-v0",
            }
        })
        self.assertEqual(trial.config["env"], "CartPole-v0")

    def testConfigPurity(self):
        def train(config, reporter):
            assert config == {"a": "b"}, config
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "a": "b"
                },
            }
        })

    def testLogdir(self):
        def train(config, reporter):
            assert "/tmp/logdir/foo" in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": "/tmp/logdir",
                "config": {
                    "a": "b"
                },
            }
        })

    def testLogdirStartingWithTilde(self):
        local_dir = '~/ray_results/local_dir'

        def train(config, reporter):
            cwd = os.getcwd()
            assert cwd.startswith(os.path.expanduser(local_dir)), cwd
            assert not cwd.startswith('~'), cwd
            reporter(timesteps_total=1)

        register_trainable('f1', train)
        run_experiments({
            'foo': {
                'run': 'f1',
                'local_dir': local_dir,
                'config': {
                    'a': 'b'
                },
            }
        })

    def testLongFilename(self):
        def train(config, reporter):
            assert "/tmp/logdir/foo" in os.getcwd(), os.getcwd()
            reporter(timesteps_total=1)

        register_trainable("f1", train)
        run_experiments({
            "foo": {
                "run": "f1",
                "local_dir": "/tmp/logdir",
                "config": {
                    "a" * 50: lambda spec: 5.0 / 7,
                    "b" * 50: lambda spec: "long" * 40
                },
            }
        })

    def testBadParams(self):
        def f():
            run_experiments({"foo": {}})

        self.assertRaises(TuneError, f)

    def testBadParams2(self):
        def f():
            run_experiments({
                "foo": {
                    "run": "asdf",
                    "bah": "this param is not allowed",
                }
            })

        self.assertRaises(TuneError, f)

    def testBadParams3(self):
        def f():
            run_experiments({
                "foo": {
                    "run": grid_search("invalid grid search"),
                }
            })

        self.assertRaises(TuneError, f)

    def testBadParams4(self):
        def f():
            run_experiments({
                "foo": {
                    "run": "asdf",
                }
            })

        self.assertRaises(TuneError, f)

    def testBadParams5(self):
        def f():
            run_experiments({"foo": {"run": "PPO", "stop": {"asdf": 1}}})

        self.assertRaises(TuneError, f)

    def testBadParams6(self):
        def f():
            run_experiments({
                "foo": {
                    "run": "PPO",
                    "trial_resources": {
                        "asdf": 1
                    }
                }
            })

        self.assertRaises(TuneError, f)

    def testBadStoppingReturn(self):
        def train(config, reporter):
            reporter()

        register_trainable("f1", train)

        def f():
            run_experiments({
                "foo": {
                    "run": "f1",
                    "stop": {
                        "time": 10
                    },
                    "config": {
                        "script_min_iter_time_s": 0,
                    },
                }
            })

        self.assertRaises(TuneError, f)

    def testEarlyReturn(self):
        def train(config, reporter):
            reporter(timesteps_total=100, done=True)
            time.sleep(99999)

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0,
                },
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 100)

    def testAbruptReturn(self):
        def train(config, reporter):
            reporter(timesteps_total=100)

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0,
                },
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 100)

    def testErrorReturn(self):
        def train(config, reporter):
            raise Exception("uh oh")

        register_trainable("f1", train)

        def f():
            run_experiments({
                "foo": {
                    "run": "f1",
                    "config": {
                        "script_min_iter_time_s": 0,
                    },
                }
            })

        self.assertRaises(TuneError, f)

    def testSuccess(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        [trial] = run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0,
                },
            }
        })
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)


class RunExperimentTest(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testDict(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        trials = run_experiments({
            "foo": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0
                }
            },
            "bar": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0
                }
            }
        })
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testExperiment(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        exp1 = Experiment(**{
            "name": "foo",
            "run": "f1",
            "config": {
                "script_min_iter_time_s": 0
            }
        })
        [trial] = run_experiments(exp1)
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testExperimentList(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        exp1 = Experiment(**{
            "name": "foo",
            "run": "f1",
            "config": {
                "script_min_iter_time_s": 0
            }
        })
        exp2 = Experiment(**{
            "name": "bar",
            "run": "f1",
            "config": {
                "script_min_iter_time_s": 0
            }
        })
        trials = run_experiments([exp1, exp2])
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testSpecifyAlgorithm(self):
        """Tests run_experiments works without specifying experiment."""

        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)

        alg = BasicVariantGenerator({
            "foo": {
                "run": "f1",
                "config": {
                    "script_min_iter_time_s": 0
                }
            }
        })
        trials = run_experiments(search_alg=alg)
        for trial in trials:
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)


class VariantGeneratorTest(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def generate_trials(self, spec, name):
        suggester = BasicVariantGenerator({name: spec})
        return suggester.next_trials()

    def testParseToTrials(self):
        trials = self.generate_trials({
            "run": "PPO",
            "repeat": 2,
            "max_failures": 5,
            "config": {
                "env": "Pong-v0",
                "foo": "bar"
            },
        }, "tune-pong")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(str(trials[0]), "PPO_Pong-v0_0")
        self.assertEqual(trials[0].config, {"foo": "bar", "env": "Pong-v0"})
        self.assertEqual(trials[0].trainable_name, "PPO")
        self.assertEqual(trials[0].experiment_tag, "0")
        self.assertEqual(trials[0].max_failures, 5)
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
            },
        }, "grid_search")
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1})
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1")
        self.assertEqual(trials[1].config, {"bar": False, "foo": 1})
        self.assertEqual(trials[1].experiment_tag, "1_bar=False,foo=1")
        self.assertEqual(trials[2].config, {"bar": True, "foo": 2})
        self.assertEqual(trials[3].config, {"bar": False, "foo": 2})
        self.assertEqual(trials[4].config, {"bar": True, "foo": 3})
        self.assertEqual(trials[5].config, {"bar": False, "foo": 3})

    def testGridSearchAndEval(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "qux": lambda spec: 2 + 2,
                "bar": grid_search([True, False]),
                "foo": grid_search([1, 2, 3]),
            },
        }, "grid_eval")
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1, "qux": 4})
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1,qux=4")

    def testConditionResolution(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": 1,
                "y": lambda spec: spec.config.x + 1,
                "z": lambda spec: spec.config.y + 1,
            },
        }, "condition_resolution")
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"x": 1, "y": 2, "z": 3})

    def testDependentLambda(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "x": grid_search([1, 2]),
                "y": lambda spec: spec.config.x * 100,
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
                    lambda spec: spec.config.y * 100,
                    lambda spec: spec.config.y * 200
                ]),
                "y": lambda spec: 1,
            },
        }, "dependent_grid_search")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].config, {"x": 100, "y": 1})
        self.assertEqual(trials[1].config, {"x": 200, "y": 1})

    def testRecursiveDep(self):
        try:
            list(
                self.generate_trials({
                    "run": "PPO",
                    "config": {
                        "foo": lambda spec: spec.config.foo,
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
            "repeat": 6,
        }
        experiments = [Experiment.from_json("test", experiment_spec)]

        searcher = _MockSuggestionAlgorithm(experiments, max_concurrent=4)
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


class TrialRunnerTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testTrialStatus(self):
        ray.init()
        trial = Trial("__fake")
        self.assertEqual(trial.status, Trial.PENDING)
        trial.start()
        self.assertEqual(trial.status, Trial.RUNNING)
        trial.stop()
        self.assertEqual(trial.status, Trial.TERMINATED)
        trial.stop(error=True)
        self.assertEqual(trial.status, Trial.ERROR)

    def testExperimentTagTruncation(self):
        ray.init()

        def train(config, reporter):
            reporter(timesteps_total=1)

        register_trainable("f1", train)

        experiments = {
            "foo": {
                "run": "f1",
                "config": {
                    "a" * 50: lambda spec: 5.0 / 7,
                    "b" * 50: lambda spec: "long" * 40
                },
            }
        }

        for name, spec in experiments.items():
            trial_generator = BasicVariantGenerator({name: spec})
            for trial in trial_generator.next_trials():
                trial.start()
                self.assertLessEqual(len(trial.logdir), 200)
                trial.stop()

    def testTrialErrorOnStart(self):
        ray.init()
        _global_registry.register(TRAINABLE_CLASS, "asdf", None)
        trial = Trial("asdf", resources=Resources(1, 0))
        try:
            trial.start()
        except Exception as e:
            self.assertIn("a class", str(e))

    def testExtraResources(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=0, extra_cpu=3, extra_gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testResourceScheduler(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.TERMINATED)

    def testMultiStepRun(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 5
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testErrorHandling(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        _global_registry.register(TRAINABLE_CLASS, "asdf", None)
        trials = [Trial("asdf", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testFailureRecoveryDisabled(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 0,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[0].num_failures, 1)

    def testFailureRecoveryEnabled(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 1,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 1)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

    def testFailureRecoveryMaxFailures(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 2,
            "config": {
                "mock_error": True,
                "persistent_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 1)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 2)
        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[0].num_failures, 3)

    def testCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)

        path = trials[0].checkpoint()
        kwargs["restore_path"] = path

        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[1].runner.get_info.remote()), 1)
        self.addCleanup(os.remove, path)

    def testResultDone(self):
        """Tests that last_result is marked `done` after trial is complete."""
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertNotEqual(trials[0].last_result[DONE], True)
        runner.step()
        self.assertEqual(trials[0].last_result[DONE], True)

    def testPauseThenResume(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), None)

        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)

        trials[0].pause()
        self.assertEqual(trials[0].status, Trial.PAUSED)

        trials[0].resume()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), 1)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

    def testStopTrial(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner(BasicVariantGenerator())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 5
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)
        ]
        for t in trials:
            runner.add_trial(t)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        # Stop trial while running
        runner.stop_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.PENDING)

        # Stop trial while pending
        runner.stop_trial(trials[-1])
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.TERMINATED)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.TERMINATED)

    def testSearchAlgNotification(self):
        """Checks notification of trial to the Search Algorithm."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(experiments, max_concurrent=10)
        runner = TrialRunner(search_alg=searcher)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        self.assertEqual(searcher.counter["result"], 1)
        self.assertEqual(searcher.counter["complete"], 1)

    def testSearchAlgFinished(self):
        """Checks that SearchAlg is Finished before all trials are done."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 1}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(experiments, max_concurrent=10)
        runner = TrialRunner(search_alg=searcher)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertTrue(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgSchedulerInteraction(self):
        """Checks that TrialScheduler killing trial will notify SearchAlg."""

        class _MockScheduler(FIFOScheduler):
            def on_trial_result(self, *args, **kwargs):
                return TrialScheduler.STOP

        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(experiments, max_concurrent=10)
        runner = TrialRunner(search_alg=searcher, scheduler=_MockScheduler())
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertTrue(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgStalled(self):
        """Checks that runner and searcher state is maintained when stalled."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {
            "run": "__fake",
            "repeat": 3,
            "stop": {
                "training_iteration": 1
            }
        }
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(experiments, max_concurrent=1)
        runner = TrialRunner(search_alg=searcher)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        trials = runner.get_trials()
        runner.step()
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        searcher.stall = True

        runner.step()
        self.assertEqual(trials[1].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)

        self.assertTrue(all(trial.is_finished() for trial in trials))
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        searcher.stall = False

        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        runner.step()
        self.assertEqual(trials[2].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())


if __name__ == "__main__":
    unittest.main(verbosity=2)
