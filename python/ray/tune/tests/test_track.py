import pandas as pd
import unittest

import ray
from ray import tune
from ray.tune import session


def _check_json_val(fname, key, val):
    with open(fname, "r") as f:
        df = pd.read_json(f, typ="frame", lines=True)
        return key in df.columns and (df[key].tail(n=1) == val).all()


class TrackApiTest(unittest.TestCase):
    def tearDown(self):
        session.shutdown()
        ray.shutdown()

    def testSoftDeprecation(self):
        """Checks that tune.track.log code does not break."""
        from ray.tune import track
        ray.init(num_cpus=2)

        def testme(config):
            for i in range(config["iters"]):
                track.log(iteration=i, hi="test")

        trials = tune.run(testme, config={"iters": 5}).trials
        trial_res = trials[0].last_result
        self.assertTrue(trial_res["hi"], "test")
        self.assertTrue(trial_res["training_iteration"], 5)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
