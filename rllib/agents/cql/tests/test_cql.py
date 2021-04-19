import os
import unittest

import ray
import ray.rllib.agents.cql as cql
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestCQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)#TODO

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_cql_compilation(self):
        """Test whether a MAMLTrainer can be built with all frameworks."""
        config = cql.CQL_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["rollout_fragment_length"] = 10
        config["train_batch_size"] = 10
        num_iterations = 1

        # Test for tf framework (torch not implemented yet).
        for fw in framework_iterator(config, frameworks=("torch")):
            for env in [
                    "MountainCarContinuous-v0",
            ]:
                print("env={}".format(env))
                trainer = cql.CQLTrainer(config=config, env=env)
                for i in range(num_iterations):
                    trainer.train()
                check_compute_single_action(trainer)
                trainer.stop()

    def test_cql_offline_learning_pendulum(self):
        """Test whether a MAMLTrainer can be built with all frameworks."""
        config = cql.CQL_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        #config["twin_q"] = True
        config["bc_iters"] = 0
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["rollout_fragment_length"] = 1
        #config["twin_q"] = False #TODO
        config["lagrangian"] = True
        config["Q_model"] = {
            "fcnet_hiddens": [256, 256, 256]
        }
        config["policy_model"] = {
            "fcnet_hiddens": [256, 256, 256]
        }
        config["optimization"] = {
            "actor_learning_rate": 3e-5,
            "critic_learning_rate": 3e-4,
            "entropy_learning_rate": 3e-4,
        }
        config["train_batch_size"] = 256
        config["timesteps_per_iteration"] = 1000
        data_file_pend = "/tmp/out/output-2021-04-19_12-25-05_worker-0_4.json"
        print("data_file_pend={} exists={}".format(
            data_file_pend, os.path.isfile(data_file_pend)))
        data_file = data_file_pend
        config["input"] = [data_file]
        config["evaluation_num_workers"] = 1
        config["evaluation_interval"] = 3
        #config["evaluation_num_episodes"] = 1
        config["evaluation_parallel_to_training"] = True
        # Evaluate on actual environment.
        config["evaluation_config"] = {"input": "sampler"}

        num_iterations = 5000
        min_reward = -500
        env = "Pendulum-v0"

        # Test for tf framework (torch not implemented yet).
        for _ in framework_iterator(config, frameworks=("torch")):
            trainer = cql.CQLTrainer(config=config, env=env)
            learnt = False
            for i in range(num_iterations):
                eval_results = trainer.train().get("evaluation")
                if eval_results:
                    print("iter={} R={}".format(
                        i, eval_results["episode_reward_mean"]))
                    # Learn until some reward is reached on an actual live env.
                    if eval_results["episode_reward_mean"] >= min_reward:
                        learnt = True
                        break
            if not learnt:
                raise ValueError(
                    "CQLTrainer did not reach {} reward from expert "
                    "offline data!".format(min_reward))
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
