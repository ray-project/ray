import copy
from gym.spaces import Box, Discrete
import time
import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import framework_iterator


class TestTrajectoryViewAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_plain(self):
        config = ppo.DEFAULT_CONFIG.copy()
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config, env="CartPole-v0")
            policy = trainer.get_policy()
            view_req_model = policy.model.inference_view_requirements()
            view_req_policy = policy.training_view_requirements()
            assert len(view_req_model) == 1
            assert len(view_req_policy) == 6
            for key in [
                    SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                    SampleBatch.DONES, SampleBatch.NEXT_OBS,
                    SampleBatch.VF_PREDS
            ]:
                assert key in view_req_policy
                # None of the view cols has a special underlying data_col,
                # except next-obs.
                if key != SampleBatch.NEXT_OBS:
                    assert view_req_policy[key].data_col is None
                else:
                    assert view_req_policy[key].data_col == SampleBatch.OBS
                    assert view_req_policy[key].shift == 1
            trainer.stop()

    def test_lstm_prev_actions_and_rewards(self):
        config = ppo.DEFAULT_CONFIG.copy()
        config["model"] = config["model"].copy()
        # Activate LSTM + prev-action + rewards.
        config["model"]["use_lstm"] = True
        config["model"]["lstm_use_prev_action_reward"] = True

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config, env="CartPole-v0")
            policy = trainer.get_policy()
            view_req_model = policy.model.inference_view_requirements()
            view_req_policy = policy.training_view_requirements()
            assert len(view_req_model) == 5  # obs, prev_a, prev_r, 2xstate_in
            assert len(view_req_policy) == 14
            for key in [
                    SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                    SampleBatch.DONES, SampleBatch.NEXT_OBS,
                    SampleBatch.VF_PREDS, SampleBatch.PREV_ACTIONS,
                    SampleBatch.PREV_REWARDS
            ]:
                assert key in view_req_policy

                if key == SampleBatch.PREV_ACTIONS:
                    assert view_req_policy[key].data_col == SampleBatch.ACTIONS
                    assert view_req_policy[key].shift == -1
                elif key == SampleBatch.PREV_REWARDS:
                    assert view_req_policy[key].data_col == SampleBatch.REWARDS
                    assert view_req_policy[key].shift == -1
                elif key not in [
                        SampleBatch.NEXT_OBS, SampleBatch.PREV_ACTIONS,
                        SampleBatch.PREV_REWARDS
                ]:
                    assert view_req_policy[key].data_col is None
                else:
                    assert view_req_policy[key].data_col == SampleBatch.OBS
                    assert view_req_policy[key].shift == 1
            trainer.stop()

    def test_ppo_trajectory_view_api_performance(self):
        """Test whether PPOTrainer runs faster w/ `_use_trajectory_view_api`.
        """
        config = copy.deepcopy(ppo.DEFAULT_CONFIG)
        action_space = Discrete(2)
        obs_space = Box(-1.0, 1.0, shape=(7000, ))

        from ray.rllib.examples.env.random_env import RandomMultiAgentEnv

        from ray.tune import register_env
        register_env("ma_env", lambda c: RandomMultiAgentEnv({
            "num_agents": 2,
            "p_done": 0.02,
            "action_space": action_space,
            "observation_space": obs_space
        }))

        config["num_workers"] = 0
        config["num_envs_per_worker"] = 2
        config["num_sgd_iter"] = 4
        config["model"]["use_lstm"] = True
        config["model"]["lstm_use_prev_action_reward"] = True
        config["model"]["max_seq_len"] = 100

        policies = {
            "pol0": (None, obs_space, action_space, {}),
        }

        def policy_fn(agent_id):
            return "pol0"

        config["multiagent"] = {
            "policies": policies,
            "policy_mapping_fn": policy_fn,
        }
        num_iterations = 5
        # Only works in torch so far.
        for _ in framework_iterator(config, frameworks="torch"):
            config["_use_trajectory_view_api"] = True
            config["model"]["_time_major"] = True
            trainer = ppo.PPOTrainer(config=config, env="ma_env")
            learn_time_w = 0.0
            start = time.time()
            for i in range(num_iterations):
                results = trainer.train()
                learn_time_w += results["timers"]["learn_time_ms"]
            duration_w = time.time() - start
            preprocessing_w = results["sampler_perf"]["mean_processing_ms"]
            inference_w = results["sampler_perf"]["mean_inference_ms"]
            print("w/ _fast_sampling: Duration: {}s mean-preprocessing={}ms "
                  "mean-inference={}ms learn-time/iter={}ms".format(
                      duration_w, preprocessing_w, inference_w,
                      learn_time_w / num_iterations))
            trainer.stop()

            config["_use_trajectory_view_api"] = False
            config["model"]["_time_major"] = False
            trainer = ppo.PPOTrainer(config=config, env="ma_env")
            learn_time_wo = 0.0
            start = time.time()
            for i in range(num_iterations):
                results = trainer.train()
                learn_time_wo += results["timers"]["learn_time_ms"]
            duration_wo = time.time() - start
            preprocessing_wo = results["sampler_perf"]["mean_processing_ms"]
            inference_wo = results["sampler_perf"]["mean_inference_ms"]
            print("w/o _fast_sampling: Duration: {}s mean-preprocessing={}ms "
                  "mean-inference={}ms learn-time/iter={}ms".format(
                      duration_wo, preprocessing_wo, inference_wo,
                      learn_time_wo / num_iterations))
            trainer.stop()

            # Assert `_fasts_sampling` is faster across all affected metrics.
            self.assertLess(duration_w, duration_wo)
            self.assertLess(preprocessing_w, preprocessing_wo)
            self.assertLess(inference_w, inference_wo)

            ## Check learning success.
            #print("w/ _fast_sampling: reward={}".format(
            #    results["episode_reward_mean"]))
            #self.assertGreater(results["episode_reward_mean"], 80.0)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
