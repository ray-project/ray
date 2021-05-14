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

        data_file = "/Users/sven/Dropbox/Projects/anyscale_projects/wildlife_studios/cql_issue/dataset_sample.json"
        config = cql.CQL_DEFAULT_CONFIG.copy()
        #TODO
        from ray.rllib.examples.env.random_env import RandomEnv
        from gym.spaces import Box, Discrete
        import numpy as np
        #END TODO
        config["env"] = RandomEnv#"Pendulum-v0"
        config["env_config"] = {
            "observation_space": Box(-1000.0, 1000.0, (47,), np.float32),
            "action_space": Discrete(12),
        }
        config["input"] = [data_file]

        #TODO
        config["input_evaluation"] = ["is"]

        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["rollout_fragment_length"] = 1
        config["train_batch_size"] = 10

        config = {
            "Q_model": {
                "custom_model": None,
                "custom_model_config": {},
                "fcnet_activation": "relu",
                "fcnet_hiddens": [
                    256,
                    256
                ],
                "post_fcnet_activation": None,
                "post_fcnet_hiddens": []
            },
            "bc_iters": 20000,
            "buffer_size": 1571951,
            "compress_observations": False,
            "env": RandomEnv,
            "env_config": {
                "action_space": Box(0.0, 100.0, (4,), np.float32),
                "observation_space": Box(-1000.0, 1000.0, (47,), np.float32)
            },
            "evaluation_interval": 20000,
            "evaluation_num_episodes": 20000,
            "evaluation_num_workers": 0,
            "explore": False,
            "final_prioritized_replay_beta": 0.4,
            "framework": "torch",
            "grad_clip": None,
            "ignore_worker_failures": True,
            "initial_alpha": 1.0,
            "input": [
                data_file
            ],
            "input_evaluation": [
                "is",
                #"wis"
            ],
            "lagrangian": False,
            "lagrangian_thresh": 5.0,
            "learning_starts": 1500,
            "log_level": "DEBUG",
            "min_q_weight": 5.0,
            "monitor": True,
            "n_step": 1,
            "no_done_at_end": False,
            "normalize_actions": True,
            "num_actions": 10,
            "num_cpus_for_driver": 3,
            "num_cpus_per_worker": 0,
            "num_gpus": 0,
            "num_gpus_per_worker": 0,
            "num_workers": 0,
            "optimization": {
                "actor_learning_rate": 0.0003,
                "critic_learning_rate": 0.0003,
                "entropy_learning_rate": 0.0003
            },
            "policy_model": {
                "custom_model": None,
                "custom_model_config": {},
                "fcnet_activation": "relu",
                "fcnet_hiddens": [
                    256,
                    256
                ],
                "post_fcnet_activation": None,
                "post_fcnet_hiddens": []
            },
            "prioritized_replay": False,
            "prioritized_replay_alpha": 0.6,
            "prioritized_replay_beta": 0.4,
            "prioritized_replay_beta_annealing_timesteps": 20000,
            "prioritized_replay_eps": 1e-06,
            "rollout_fragment_length": 1,
            "seed": 42,
            "target_entropy": None,
            "target_network_update_freq": 0,
            "tau": 0.005,
            "temperature": 0.5,
            "timesteps_per_iteration": 100,
            "train_batch_size": 256,
            "training_intensity": None,
            "twin_q": True,
        }

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
