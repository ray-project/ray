import functools
from pathlib import Path
import os
import unittest

import ray
from ray.rllib.algorithms.dt import DTConfig
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
)

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestDT(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dt_compilation(self):
        """Test whether a CRR algorithm can be built with all supported frameworks."""

        # TODO: terrible asset management style
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/pendulum/large.json")
        # data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))
        # Will use the Json Reader in this example until we convert over the example
        # files over to Parquet, since the dataset json reader cannot handle large
        # block sizes.

        def input_reading_fn(ioctx):
            return JsonReader(ioctx.config["input_config"]["paths"], ioctx)

        input_config = {"paths": data_file}

        config = (
            DTConfig()
            .environment(env="Pendulum-v1", clip_actions=True)
            # .environment(env="CartPole-v0")
            .framework("torch")
            .offline_data(
                input_=input_reading_fn,
                input_config=input_config,
                actions_in_input_normalized=True,
            )
            .training(
                train_batch_size=200,
                # use_obs_output=True,
                # use_return_output=True,
                target_return=-300,
                replay_buffer_config={
                    "capacity": 8,
                },
                model={
                    "max_seq_len": 4,
                },
            )
            .evaluation(
                evaluation_interval=2,
                evaluation_num_workers=0,
                evaluation_duration=10,
                evaluation_duration_unit="episodes",
                evaluation_parallel_to_training=False,
                evaluation_config={"input": "sampler", "explore": False},
            )
            .rollouts(
                num_rollout_workers=0,
                horizon=200,
            )
        )

        num_iterations = 4

        for _ in ["torch"]:
            algorithm = config.build()
            # check if 4 iterations raises any errors
            for i in range(num_iterations):
                results = algorithm.train()
                check_train_results(results)
                print(results)
                if (i + 1) % 2 == 0:
                    # evaluation happens every 2 iterations
                    eval_results = results["evaluation"]
                    print(
                        f"iter={algorithm.iteration} "
                        f"R={eval_results['episode_reward_mean']}"
                    )

            # check_compute_single_action(algorithm)

            algorithm.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
