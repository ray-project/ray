from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import os

import ray
from ray.tune.trial import Trial, Resources
from ray.tune.trial_runner import TrialRunner
from ray.tune.config_parser import parse_to_trials


class ConfigParserTest(unittest.TestCase):
    def testParseToTrials(self):
        trials = parse_to_trials({
            "tune-pong": {
                "env": "Pong-v0",
                "alg": "PPO",
                "num_trials": 2,
                "config": {
                    "foo": "bar"
                },
            },
        })
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].env_name, "Pong-v0")
        self.assertEqual(trials[0].config, {"foo": "bar"})
        self.assertEqual(trials[0].alg, "PPO")
        self.assertEqual(trials[0].agent_id, "0")
        self.assertEqual(trials[0].local_dir, "/tmp/ray/tune-pong")
        self.assertEqual(trials[1].agent_id, "1")

    def testEval(self):
        trials = parse_to_trials({
            "tune-pong": {
                "env": "Pong-v0",
                "config": {
                    "foo": {
                        "eval": "2 + 2"
                    },
                },
            },
        })
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"foo": 4})
        self.assertEqual(trials[0].agent_id, "0_foo=4")

    def testGridSearch(self):
        trials = parse_to_trials({
            "tune-pong": {
                "env": "Pong-v0",
                "num_trials": 6,
                "config": {
                    "bar": {
                        "grid_search": [True, False]
                    },
                    "foo": {
                        "grid_search": [1, 2, 3]
                    },
                },
            },
        })
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1})
        self.assertEqual(trials[0].agent_id, "0_bar=True_foo=1")
        self.assertEqual(trials[1].config, {"bar": False, "foo": 1})
        self.assertEqual(trials[1].agent_id, "1_bar=False_foo=1")
        self.assertEqual(trials[2].config, {"bar": True, "foo": 2})
        self.assertEqual(trials[3].config, {"bar": False, "foo": 2})
        self.assertEqual(trials[4].config, {"bar": True, "foo": 3})
        self.assertEqual(trials[5].config, {"bar": False, "foo": 3})

    def testGridSearchAndEval(self):
        trials = parse_to_trials({
            "tune-pong": {
                "env": "Pong-v0",
                "num_trials": 1,
                "config": {
                    "qux": {
                        "eval": "2 + 2"
                    },
                    "bar": {
                        "grid_search": [True, False]
                    },
                    "foo": {
                        "grid_search": [1, 2, 3]
                    },
                },
            },
        })
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1, "qux": 4})
        self.assertEqual(trials[0].agent_id, "0_bar=True_foo=1_qux=4")


class TrialRunnerTest(unittest.TestCase):
    def tearDown(self):
        ray.worker.cleanup()

    def testTrialStatus(self):
        ray.init()
        trial = Trial("CartPole-v0", "__fake")
        self.assertEqual(trial.status, Trial.PENDING)
        trial.start()
        self.assertEqual(trial.status, Trial.RUNNING)
        trial.stop()
        self.assertEqual(trial.status, Trial.TERMINATED)
        trial.stop(error=True)
        self.assertEqual(trial.status, Trial.ERROR)

    def testTrialErrorOnStart(self):
        ray.init()
        trial = Trial("CartPole-v0", "asdf")
        try:
            trial.start()
        except Exception as e:
            self.assertIn("Unknown algorithm", str(e))

    def testResourceScheduler(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("CartPole-v0", "__fake", **kwargs),
            Trial("CartPole-v0", "__fake", **kwargs)]
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
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 5},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("CartPole-v0", "__fake", **kwargs),
            Trial("CartPole-v0", "__fake", **kwargs)]
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
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("CartPole-v0", "asdf", **kwargs),
            Trial("CartPole-v0", "__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("CartPole-v0", "__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].agent.set_info.remote(1)), 1)

        path = trials[0].checkpoint()
        kwargs["restore_path"] = path

        runner.add_trial(Trial("CartPole-v0", "__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[1].agent.get_info.remote()), 1)
        self.addCleanup(os.remove, path)

    def testPauseThenResume(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("CartPole-v0", "__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].agent.get_info.remote()), None)

        self.assertEqual(ray.get(trials[0].agent.set_info.remote(1)), 1)

        trials[0].pause()
        self.assertEqual(trials[0].status, Trial.PAUSED)

        trials[0].resume()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].agent.get_info.remote()), 1)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)


if __name__ == "__main__":
    unittest.main(verbosity=2)
