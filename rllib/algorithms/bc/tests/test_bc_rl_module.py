import os
from pathlib import Path
import unittest

import ray
import ray.rllib.algorithms.bc as bc
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)

tf1, tf, tfv = try_import_tf()


class TestBRLModuleC(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_bc_compilation_and_learning_from_offline_file(self):
        """Test whether BC can be built with all frameworks.

        And learns from a historic-data file (while being evaluated on an
        actual env using  evaluation_num_workers > 0).
        """
        rllib_dir = Path(__file__).parents[3]
        print("rllib_dir={}".format(rllib_dir))
        # This has still be done until `pathlib` will be used in the readers.
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print(f"data_file={data_file} exists={os.path.isfile(data_file)}")
        # print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        config = (
            bc.BCConfig()
            .evaluation(
                evaluation_interval=3,
                evaluation_num_workers=1,
                evaluation_duration=5,
                evaluation_parallel_to_training=True,
                evaluation_config=bc.BCConfig.overrides(input_="sampler"),
            )
            .offline_data(input_=[data_file])
            # training(_enable_learner_api=True, use_gae=False)
        )
        num_iterations = 350
        min_reward = 75.0

        # Test for all frameworks.
        for _ in framework_iterator(config, frameworks=("torch", "tf2")):
            algo = config.build(env="CartPole-v1")
            learnt = False
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)

                eval_results = results.get("evaluation")
                if eval_results:
                    print("iter={} R={}".format(i, eval_results["episode_reward_mean"]))
                    # Learn until good reward is reached in the actual env.
                    if eval_results["episode_reward_mean"] > min_reward:
                        print("learnt!")
                        learnt = True
                        break

            if not learnt:
                raise ValueError(
                    "`BC` did not reach {} reward from expert offline "
                    "data!".format(min_reward)
                )

            check_compute_single_action(algo, include_prev_action_reward=True)

            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
