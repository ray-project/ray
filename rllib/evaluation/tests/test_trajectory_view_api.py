import copy
from gym.spaces import Box, Discrete
import time
import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.env.debug_counter_env import MultiAgentDebugCounterEnv
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.policy.episode_env_aware_policy import \
    EpisodeEnvAwareCallback, EpisodeEnvAwarePolicy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import framework_iterator


class TestTrajectoryViewAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        #TODO remove local-mode
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_traj_view_normal_case(self):
        """Tests, whether Model and Policy return the correct ViewRequirements.
        """
        config = ppo.DEFAULT_CONFIG.copy()
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config, env="CartPole-v0")
            policy = trainer.get_policy()
            view_req_model = policy.model.inference_view_requirements
            view_req_policy = policy.training_view_requirements
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

    def test_traj_view_lstm_prev_actions_and_rewards(self):
        """Tests, whether Policy/Model return correct LSTM ViewRequirements.
        """
        config = ppo.DEFAULT_CONFIG.copy()
        config["model"] = config["model"].copy()
        # Activate LSTM + prev-action + rewards.
        config["model"]["use_lstm"] = True
        config["model"]["lstm_use_prev_action_reward"] = True

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config, env="CartPole-v0")
            policy = trainer.get_policy()
            view_req_model = policy.model.inference_view_requirements
            view_req_policy = policy.training_view_requirements
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

    def test_traj_view_lstm_performance(self):
        """Test whether PPOTrainer runs faster w/ `_use_trajectory_view_api`.
        """
        config = copy.deepcopy(ppo.DEFAULT_CONFIG)
        action_space = Discrete(2)
        obs_space = Box(-1.0, 1.0, shape=(7000, ))

        from ray.rllib.examples.env.random_env import RandomMultiAgentEnv

        from ray.tune import register_env
        register_env("ma_env", lambda c: RandomMultiAgentEnv({
            "num_agents": 2,
            "p_done": 0.01,
            "action_space": action_space,
            "observation_space": obs_space
        }))

        config["num_workers"] = 2
        config["num_envs_per_worker"] = 4
        config["num_sgd_iter"] = 8
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
        num_iterations = 4
        # Only works in torch so far.
        for _ in framework_iterator(config, frameworks="torch"):
            print("w/ API")
            config["_use_trajectory_view_api"] = True
            config["model"]["_time_major"] = True
            trainer = ppo.PPOTrainer(config=config, env="ma_env")
            learn_time_w = 0.0
            sampler_perf = {}
            start = time.time()
            for i in range(num_iterations):
                out = trainer.train()
                sampler_perf_ = out["sampler_perf"]
                sampler_perf = {
                    k: sampler_perf.get(k, 0.0) + sampler_perf_[k] for
                    k, v in sampler_perf_.items()}
                delta = out["timers"]["learn_time_ms"] / 1000
                learn_time_w += delta
                print("{}={}s".format(i, delta))
            sampler_perf = {
                k: sampler_perf[k] / (num_iterations if "mean_" in k else 1)
                for k, v in sampler_perf.items()}
            duration_w = time.time() - start - sampler_perf["total_env_wait_s"]
            get_ma_train_batch_w = sampler_perf["total_get_ma_train_batch_s"]
            postproc_traj_so_far_w = \
                sampler_perf["total_postprocess_trajectories_so_far_s"]
            print("w/ traj-view API: Duration (no Env): {}s "
                  "sampler-perf.={} learn-time/iter={}s".format(
                duration_w, sampler_perf, learn_time_w / num_iterations))
            trainer.stop()

            print("w/o API")
            config["_use_trajectory_view_api"] = False
            config["model"]["_time_major"] = False
            trainer = ppo.PPOTrainer(config=config, env="ma_env")
            learn_time_wo = 0.0
            sampler_perf = {}
            start = time.time()
            for i in range(num_iterations):
                out = trainer.train()
                sampler_perf_ = out["sampler_perf"]
                sampler_perf = {
                    k: sampler_perf.get(k, 0.0) + sampler_perf_[k] for
                    k, v in sampler_perf_.items()}
                delta = out["timers"]["learn_time_ms"] / 1000
                learn_time_wo += delta
                print("{}={}s".format(i, delta))
            sampler_perf = {
                k: sampler_perf[k] / (num_iterations if "mean_" in k else 1)
                for k, v in sampler_perf.items()}
            duration_wo = time.time() - start - \
                          sampler_perf["total_env_wait_s"]
            get_ma_train_batch_wo = sampler_perf["total_get_ma_train_batch_s"]
            postproc_traj_so_far_wo = \
                sampler_perf["total_postprocess_trajectories_so_far_s"]
            print("w/o traj-view API: Duration (no Env): {}s "
                  "sampler-perf.={} learn-time/iter={}s".format(
                duration_wo, sampler_perf, learn_time_wo / num_iterations))
            trainer.stop()

            # Assert `_fasts_sampling` is much(!) faster across important
            # metrics.
            self.assertLess(duration_w, duration_wo * 0.6)
            self.assertLess(learn_time_w, learn_time_wo * 0.5)
            self.assertLess(get_ma_train_batch_w, get_ma_train_batch_wo * 0.6)
            self.assertLess(
                postproc_traj_so_far_w, postproc_traj_so_far_wo * 0.3)

            ## Check learning success.
            #print("w/ _fast_sampling: reward={}".format(
            #    results["episode_reward_mean"]))
            #self.assertGreater(results["episode_reward_mean"], 80.0)

    def test_traj_view_lstm_functionality(self):
        action_space = Box(-float("inf"), float("inf"), shape=(2,))
        obs_space = Box(float("-inf"), float("inf"), (4, ))
        policies = {
            "pol0": (EpisodeEnvAwarePolicy, obs_space, action_space, {}),
        }

        def policy_fn(agent_id):
            return "pol0"

        rollout_worker = RolloutWorker(
            env_creator=lambda _: MultiAgentDebugCounterEnv({"num_agents": 4}),
            policy_config={
                "multiagent": {
                    "policies": policies,
                    "policy_mapping_fn": policy_fn,
                },
                "_use_trajectory_view_api": True,
                "model": {
                    "use_lstm": True,
                    "_time_major": True,
                    "max_seq_len": 50,
                },
            },
            callbacks=EpisodeEnvAwareCallback,
            policy=policies,
            policy_mapping_fn=policy_fn,
            num_envs=1,
        )
        result = rollout_worker.sample()
        print(result)
        #TODO: finish this test case: check, whether everything in buffer makes sense:
        # - obs -> next_obs shift (prev_actions/rewards shifts)
        # - seq_lens make sense (match ts)
        # - all cursors in sample collector make sense
        # - sample as long as there is a rollover in the buffer, then check again


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
