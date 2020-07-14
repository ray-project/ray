import json
import unittest
import shutil
import tempfile
import os
import random
import pandas as pd
import pytest
import numpy as np

import ray
from ray.tune import (run, Trainable, sample_from, Analysis,
                      ExperimentAnalysis, grid_search)
from ray.tune.examples.async_hyperband_example import MyTrainableClass


class ExperimentAnalysisInMemorySuite(unittest.TestCase):
    def setUp(self):
        class MockTrainable(Trainable):
            scores_dict = {
                0: [5, 4, 4, 4, 4, 4, 4, 4, 0],
                1: [4, 3, 3, 3, 3, 3, 3, 3, 1],
                2: [2, 1, 1, 1, 1, 1, 1, 1, 8],
                3: [9, 7, 7, 7, 7, 7, 7, 7, 6],
                4: [7, 5, 5, 5, 5, 5, 5, 5, 3]
            }

            def setup(self, config):
                self.id = config["id"]
                self.idx = 0

            def step(self):
                val = self.scores_dict[self.id][self.idx]
                self.idx += 1
                return {"score": val}

            def save_checkpoint(self, checkpoint_dir):
                pass

            def load_checkpoint(self, checkpoint_path):
                pass

        self.MockTrainable = MockTrainable
        self.test_dir = tempfile.mkdtemp()
        ray.init(local_mode=False, num_cpus=1)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        ray.shutdown()

    def testInit(self):
        experiment_checkpoint_path = os.path.join(self.test_dir,
                                                  "experiment_state.json")
        checkpoint_data = {
            "checkpoints": [{
                "trainable_name": "MockTrainable",
                "logdir": "/mock/test/MockTrainable_0_id=3_2020-07-12"
            }]
        }

        with open(experiment_checkpoint_path, "w") as f:
            f.write(json.dumps(checkpoint_data))

        experiment_analysis = ExperimentAnalysis(experiment_checkpoint_path)
        self.assertEqual(len(experiment_analysis._checkpoints), 1)
        self.assertTrue(experiment_analysis.trials is None)

    def testInitException(self):
        experiment_checkpoint_path = os.path.join(self.test_dir, "mock.json")
        with pytest.raises(ValueError):
            ExperimentAnalysis(experiment_checkpoint_path)

    def testCompareTrials(self):
        scores = np.asarray(list(self.MockTrainable.scores_dict.values()))
        scores_all = scores.flatten("F")
        scores_last = scores_all[5:]

        ea = run(
            self.MockTrainable,
            name="analysis_exp",
            local_dir=self.test_dir,
            stop={"training_iteration": len(scores[0])},
            num_samples=1,
            config={"id": grid_search(list(range(5)))})

        max_all = ea.get_best_trial("score",
                                    "max").metric_analysis["score"]["max"]
        min_all = ea.get_best_trial("score",
                                    "min").metric_analysis["score"]["min"]
        max_last = ea.get_best_trial("score", "max",
                                     "last").metric_analysis["score"]["last"]
        max_avg = ea.get_best_trial("score", "max",
                                    "avg").metric_analysis["score"]["avg"]
        min_avg = ea.get_best_trial("score", "min",
                                    "avg").metric_analysis["score"]["avg"]
        max_avg_5 = ea.get_best_trial(
            "score", "max",
            "last-5-avg").metric_analysis["score"]["last-5-avg"]
        min_avg_5 = ea.get_best_trial(
            "score", "min",
            "last-5-avg").metric_analysis["score"]["last-5-avg"]
        max_avg_10 = ea.get_best_trial(
            "score", "max",
            "last-10-avg").metric_analysis["score"]["last-10-avg"]
        min_avg_10 = ea.get_best_trial(
            "score", "min",
            "last-10-avg").metric_analysis["score"]["last-10-avg"]
        self.assertEqual(max_all, max(scores_all))
        self.assertEqual(min_all, min(scores_all))
        self.assertEqual(max_last, max(scores_last))
        self.assertNotEqual(max_last, max(scores_all))

        self.assertAlmostEqual(max_avg, max(np.mean(scores, axis=1)))
        self.assertAlmostEqual(min_avg, min(np.mean(scores, axis=1)))

        self.assertAlmostEqual(max_avg_5, max(np.mean(scores[:, -5:], axis=1)))
        self.assertAlmostEqual(min_avg_5, min(np.mean(scores[:, -5:], axis=1)))

        self.assertAlmostEqual(max_avg_10, max(
            np.mean(scores[:, -10:], axis=1)))
        self.assertAlmostEqual(min_avg_10, min(
            np.mean(scores[:, -10:], axis=1)))


class AnalysisSuite(unittest.TestCase):
    def setUp(self):
        ray.init(local_mode=True)
        self.test_dir = tempfile.mkdtemp()
        self.num_samples = 10
        self.metric = "episode_reward_mean"
        self.run_test_exp(test_name="analysis_exp1")
        self.run_test_exp(test_name="analysis_exp2")

    def run_test_exp(self, test_name=None):
        run(MyTrainableClass,
            name=test_name,
            local_dir=self.test_dir,
            return_trials=False,
            stop={"training_iteration": 1},
            num_samples=self.num_samples,
            config={
                "width": sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": sample_from(lambda spec: int(100 * random.random())),
            })

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        ray.shutdown()

    def testDataframe(self):
        analysis = Analysis(self.test_dir)
        df = analysis.dataframe()
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEqual(df.shape[0], self.num_samples * 2)

    def testBestLogdir(self):
        analysis = Analysis(self.test_dir)
        logdir = analysis.get_best_logdir(self.metric)
        self.assertTrue(logdir.startswith(self.test_dir))
        logdir2 = analysis.get_best_logdir(self.metric, mode="min")
        self.assertTrue(logdir2.startswith(self.test_dir))
        self.assertNotEqual(logdir, logdir2)

    def testBestConfigIsLogdir(self):
        analysis = Analysis(self.test_dir)
        for metric, mode in [(self.metric, "min"), (self.metric, "max")]:
            logdir = analysis.get_best_logdir(metric, mode=mode)
            best_config = analysis.get_best_config(metric, mode=mode)
            self.assertEqual(analysis.get_all_configs()[logdir], best_config)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
