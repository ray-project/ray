import unittest
import shutil
import tempfile
import random
import pandas as pd

import ray
from ray.tune import run, Trainable, sample_from, Analysis, grid_search
from ray.tune.examples.async_hyperband_example import MyTrainableClass


class ExperimentAnalysisInMemorySuite(unittest.TestCase):
    def setUp(self):
        class MockTrainable(Trainable):
            def _setup(self, config):
                self.id = config["id"]
                self.idx = 0
                self.scores_dict = {
                    0: [5, 0],
                    1: [4, 1],
                    2: [2, 8],
                    3: [9, 6],
                    4: [7, 3]
                }

            def _train(self):
                val = self.scores_dict[self.id][self.idx]
                self.idx += 1
                return {"score": val}

            def _save(self, checkpoint_dir):
                pass

            def _restore(self, checkpoint_path):
                pass

        self.MockTrainable = MockTrainable
        ray.init(local_mode=False, num_cpus=1)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        ray.shutdown()

    def testCompareTrials(self):
        self.test_dir = tempfile.mkdtemp()
        scores_all = [5, 4, 2, 9, 7, 0, 1, 8, 6, 3]
        scores_last = scores_all[5:]

        ea = run(
            self.MockTrainable,
            name="analysis_exp",
            local_dir=self.test_dir,
            stop={"training_iteration": 2},
            num_samples=1,
            config={"id": grid_search(list(range(5)))})

        max_all = ea.get_best_trial("score",
                                    "max").metric_analysis["score"]["max"]
        min_all = ea.get_best_trial("score",
                                    "min").metric_analysis["score"]["min"]
        max_last = ea.get_best_trial("score", "max",
                                     "last").metric_analysis["score"]["last"]
        self.assertEqual(max_all, max(scores_all))
        self.assertEqual(min_all, min(scores_all))
        self.assertEqual(max_last, max(scores_last))
        self.assertNotEqual(max_last, max(scores_all))


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
        self.assertEquals(df.shape[0], self.num_samples * 2)

    def testBestLogdir(self):
        analysis = Analysis(self.test_dir)
        logdir = analysis.get_best_logdir(self.metric)
        self.assertTrue(logdir.startswith(self.test_dir))
        logdir2 = analysis.get_best_logdir(self.metric, mode="min")
        self.assertTrue(logdir2.startswith(self.test_dir))
        self.assertNotEquals(logdir, logdir2)

    def testBestConfigIsLogdir(self):
        analysis = Analysis(self.test_dir)
        for metric, mode in [(self.metric, "min"), (self.metric, "max")]:
            logdir = analysis.get_best_logdir(metric, mode=mode)
            best_config = analysis.get_best_config(metric, mode=mode)
            self.assertEquals(analysis.get_all_configs()[logdir], best_config)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
