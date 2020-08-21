import copy
from gym.spaces import Box, Discrete
import time
import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.env.debug_counter_env import MultiAgentDebugCounterEnv
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.policy.episode_env_aware_policy import \
    EpisodeEnvAwarePolicy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import framework_iterator


class TestTrajectoryViewAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

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
            assert len(view_req_policy) == 10
            for key in [
                    SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                    SampleBatch.DONES, SampleBatch.NEXT_OBS,
                    SampleBatch.VF_PREDS, "advantages", "value_targets",
                    SampleBatch.ACTION_DIST_INPUTS, SampleBatch.ACTION_LOGP
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
            assert len(view_req_model) == 7  # obs, prev_a, prev_r, 4xstates
            assert len(view_req_policy) == 16
            for key in [
                    SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                    SampleBatch.DONES, SampleBatch.NEXT_OBS,
                    SampleBatch.VF_PREDS, SampleBatch.PREV_ACTIONS,
                    SampleBatch.PREV_REWARDS, "advantages", "value_targets",
                    SampleBatch.ACTION_DIST_INPUTS, SampleBatch.ACTION_LOGP
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
        obs_space = Box(-1.0, 1.0, shape=(700, ))

        from ray.rllib.examples.env.random_env import RandomMultiAgentEnv

        from ray.tune import register_env
        register_env("ma_env", lambda c: RandomMultiAgentEnv({
            "num_agents": 2,
            "p_done": 0.01,
            "action_space": action_space,
            "observation_space": obs_space
        }))

        config["num_workers"] = 3
        config["num_envs_per_worker"] = 8
        config["num_sgd_iter"] = 6
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
        num_iterations = 1
        # Only works in torch so far.
        for _ in framework_iterator(config, frameworks="torch"):
            print("w/ traj. view API (and time-major)")
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
                    k: sampler_perf.get(k, 0.0) + sampler_perf_[k]
                    for k, v in sampler_perf_.items()
                }
                delta = out["timers"]["learn_time_ms"] / 1000
                learn_time_w += delta
                print("{}={}s".format(i, delta))
            sampler_perf = {
                k: sampler_perf[k] / (num_iterations if "mean_" in k else 1)
                for k, v in sampler_perf.items()
            }
            duration_w = time.time() - start
            print("Duration: {}s "
                  "sampler-perf.={} learn-time/iter={}s".format(
                      duration_w, sampler_perf, learn_time_w / num_iterations))
            trainer.stop()

            print("w/o traj. view API (and w/o time-major)")
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
                    k: sampler_perf.get(k, 0.0) + sampler_perf_[k]
                    for k, v in sampler_perf_.items()
                }
                delta = out["timers"]["learn_time_ms"] / 1000
                learn_time_wo += delta
                print("{}={}s".format(i, delta))
            sampler_perf = {
                k: sampler_perf[k] / (num_iterations if "mean_" in k else 1)
                for k, v in sampler_perf.items()
            }
            duration_wo = time.time() - start
            print("Duration: {}s "
                  "sampler-perf.={} learn-time/iter={}s".format(
                      duration_wo, sampler_perf,
                      learn_time_wo / num_iterations))
            trainer.stop()

            # Assert `_use_trajectory_view_api` is much faster.
            self.assertLess(duration_w, duration_wo)
            self.assertLess(learn_time_w, learn_time_wo * 0.6)

    def test_traj_view_lstm_functionality(self):
        action_space = Box(-float("inf"), float("inf"), shape=(2, ))
        obs_space = Box(float("-inf"), float("inf"), (4, ))
        max_seq_len = 50
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
                    "max_seq_len": max_seq_len,
                },
            },
            policy=policies,
            policy_mapping_fn=policy_fn,
            num_envs=1,
        )
        for i in range(100):
            pc = rollout_worker.sampler.sample_collector. \
                policy_sample_collectors["pol0"]
            sample_batch_offset_before = pc.sample_batch_offset
            buffers = pc.buffers
            result = rollout_worker.sample()
            pol_batch = result.policy_batches["pol0"]

            self.assertTrue(result.count == 100)
            self.assertTrue(pol_batch.count >= 100)
            self.assertFalse(0 in pol_batch.seq_lens)
            # Check prev_reward/action, next_obs consistency.
            for t in range(max_seq_len):
                obs_t = pol_batch["obs"][t]
                r_t = pol_batch["rewards"][t]
                if t > 0:
                    next_obs_t_m_1 = pol_batch["new_obs"][t - 1]
                    self.assertTrue((obs_t == next_obs_t_m_1).all())
                if t < max_seq_len - 1:
                    prev_rewards_t_p_1 = pol_batch["prev_rewards"][t + 1]
                    self.assertTrue((r_t == prev_rewards_t_p_1).all())

            # Check the sanity of all the buffers in the un underlying
            # PerPolicy collector.
            for sample_batch_slot, agent_slot in enumerate(
                    range(sample_batch_offset_before, pc.sample_batch_offset)):
                t_buf = buffers["t"][:, agent_slot]
                obs_buf = buffers["obs"][:, agent_slot]
                # Skip empty seqs at end (these won't be part of the batch
                # and have been copied to new agent-slots (even if seq-len=0)).
                if sample_batch_slot < len(pol_batch.seq_lens):
                    seq_len = pol_batch.seq_lens[sample_batch_slot]
                    # Make sure timesteps are always increasing within the seq.
                    assert all(t_buf[1] + j == n + 1
                               for j, n in enumerate(t_buf)
                               if j < seq_len and j != 0)
                    # Make sure all obs within seq are non-0.0.
                    assert all(
                        any(obs_buf[j] != 0.0) for j in range(1, seq_len + 1))

            # Check seq-lens.
            for agent_slot, seq_len in enumerate(pol_batch.seq_lens):
                if seq_len < max_seq_len - 1:
                    # At least in the beginning, the next slots should always
                    # be empty (once all agent slots have been used once, these
                    # may be filled with "old" values (from longer sequences)).
                    if i < 10:
                        self.assertTrue(
                            (pol_batch["obs"][seq_len +
                                              1][agent_slot] == 0.0).all())
                    print(end="")
                    self.assertFalse(
                        (pol_batch["obs"][seq_len][agent_slot] == 0.0).all())


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
