from pathlib import Path
import os
import unittest

import ray
import ray.rllib.agents.cql as cql
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestCQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_cql_compilation(self):
        """Test whether a CQLTrainer can be built with all frameworks."""

        # Learns from a historic-data file.
        # To generate this data, first run:
        # $ ./train.py --run=SAC --env=Pendulum-v0 \
        #   --stop='{"timesteps_total": 50000}' \
        #   --config='{"output": "/tmp/out"}'
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/pendulum/small.json")
        print("data_file={} exists={}".format(data_file,
                                              os.path.isfile(data_file)))

        config = cql.CQL_DEFAULT_CONFIG.copy()
        config["env"] = "Pendulum-v0"
        config["input"] = [data_file]

        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["rollout_fragment_length"] = 1
        config["train_batch_size"] = 10

        num_iterations = 2

        # Test for tf framework (torch not implemented yet).
        for _ in framework_iterator(config, frameworks=("torch")):
            trainer = cql.CQLTrainer(config=config)
            for i in range(num_iterations):
                trainer.train()
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
