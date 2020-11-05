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
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import framework_iterator, check


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
            view_req_policy = policy.view_requirements
            assert len(view_req_model) == 1, view_req_model
            assert len(view_req_policy) == 12, view_req_policy
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
            view_req_policy = policy.view_requirements
            assert len(view_req_model) == 5, view_req_model
            assert len(view_req_policy) == 18, view_req_policy
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

    def test_traj_view_simple_performance(self):
        """Test whether PPOTrainer runs faster w/ `_use_trajectory_view_api`.
        """
        config = copy.deepcopy(ppo.DEFAULT_CONFIG)
        action_space = Discrete(2)
        obs_space = Box(-1.0, 1.0, shape=(700, ))

        from ray.rllib.examples.env.random_env import RandomMultiAgentEnv

        from ray.tune import register_env
        register_env("ma_env", lambda c: RandomMultiAgentEnv({
            "num_agents": 2,
            "p_done": 0.0,
            "max_episode_len": 104,
            "action_space": action_space,
            "observation_space": obs_space
        }))

        config["num_workers"] = 3
        config["num_envs_per_worker"] = 8
        config["num_sgd_iter"] = 1  # Put less weight on training.

        policies = {
            "pol0": (None, obs_space, action_space, {}),
        }

        def policy_fn(agent_id):
            return "pol0"

        config["multiagent"] = {
            "policies": policies,
            "policy_mapping_fn": policy_fn,
        }
        num_iterations = 2
        # Only works in torch so far.
        for _ in framework_iterator(config, frameworks="torch"):
            print("w/ traj. view API")
            config["_use_trajectory_view_api"] = True
            trainer = ppo.PPOTrainer(config=config, env="ma_env")
            learn_time_w = 0.0
            sampler_perf_w = {}
            start = time.time()
            for i in range(num_iterations):
                out = trainer.train()
                ts = out["timesteps_total"]
                sampler_perf_ = out["sampler_perf"]
                sampler_perf_w = {
                    k:
                    sampler_perf_w.get(k, 0.0) + (sampler_perf_[k] * 1000 / ts)
                    for k, v in sampler_perf_.items()
                }
                delta = out["timers"]["learn_time_ms"] / ts
                learn_time_w += delta
                print("{}={}s".format(i, delta))
            sampler_perf_w = {
                k: sampler_perf_w[k] / (num_iterations if "mean_" in k else 1)
                for k, v in sampler_perf_w.items()
            }
            duration_w = time.time() - start
            print("Duration: {}s "
                  "sampler-perf.={} learn-time/iter={}s".format(
                      duration_w, sampler_perf_w,
                      learn_time_w / num_iterations))
            trainer.stop()

            print("w/o traj. view API")
            config["_use_trajectory_view_api"] = False
            trainer = ppo.PPOTrainer(config=config, env="ma_env")
            learn_time_wo = 0.0
            sampler_perf_wo = {}
            start = time.time()
            for i in range(num_iterations):
                out = trainer.train()
                ts = out["timesteps_total"]
                sampler_perf_ = out["sampler_perf"]
                sampler_perf_wo = {
                    k: sampler_perf_wo.get(k, 0.0) +
                    (sampler_perf_[k] * 1000 / ts)
                    for k, v in sampler_perf_.items()
                }
                delta = out["timers"]["learn_time_ms"] / ts
                learn_time_wo += delta
                print("{}={}s".format(i, delta))
            sampler_perf_wo = {
                k: sampler_perf_wo[k] / (num_iterations if "mean_" in k else 1)
                for k, v in sampler_perf_wo.items()
            }
            duration_wo = time.time() - start
            print("Duration: {}s "
                  "sampler-perf.={} learn-time/iter={}s".format(
                      duration_wo, sampler_perf_wo,
                      learn_time_wo / num_iterations))
            trainer.stop()

            # Assert `_use_trajectory_view_api` is faster.
            self.assertLess(sampler_perf_w["mean_raw_obs_processing_ms"],
                            sampler_perf_wo["mean_raw_obs_processing_ms"])
            self.assertLess(sampler_perf_w["mean_action_processing_ms"],
                            sampler_perf_wo["mean_action_processing_ms"])
            self.assertLess(duration_w, duration_wo)

    def test_traj_view_lstm_functionality(self):
        action_space = Box(-float("inf"), float("inf"), shape=(3, ))
        obs_space = Box(float("-inf"), float("inf"), (4, ))
        max_seq_len = 50
        rollout_fragment_length = 200
        assert rollout_fragment_length % max_seq_len == 0
        policies = {
            "pol0": (EpisodeEnvAwarePolicy, obs_space, action_space, {}),
        }

        def policy_fn(agent_id):
            return "pol0"

        config = {
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": policy_fn,
            },
            "model": {
                "use_lstm": True,
                "max_seq_len": max_seq_len,
            },
        },

        rollout_worker_w_api = RolloutWorker(
            env_creator=lambda _: MultiAgentDebugCounterEnv({"num_agents": 4}),
            policy_config=dict(config, **{"_use_trajectory_view_api": True}),
            rollout_fragment_length=rollout_fragment_length,
            policy_spec=policies,
            policy_mapping_fn=policy_fn,
            num_envs=1,
        )
        rollout_worker_wo_api = RolloutWorker(
            env_creator=lambda _: MultiAgentDebugCounterEnv({"num_agents": 4}),
            policy_config=dict(config, **{"_use_trajectory_view_api": False}),
            rollout_fragment_length=rollout_fragment_length,
            policy_spec=policies,
            policy_mapping_fn=policy_fn,
            num_envs=1,
        )
        for iteration in range(20):
            result = rollout_worker_w_api.sample()
            check(result.count, rollout_fragment_length)
            pol_batch_w = result.policy_batches["pol0"]
            assert pol_batch_w.count >= rollout_fragment_length
            analyze_rnn_batch(pol_batch_w, max_seq_len)

            result = rollout_worker_wo_api.sample()
            pol_batch_wo = result.policy_batches["pol0"]
            check(pol_batch_w.data, pol_batch_wo.data)


def analyze_rnn_batch(batch, max_seq_len):
    count = batch.count

    # Check prev_reward/action, next_obs consistency.
    for idx in range(count):
        # If timestep tracked by batch, good.
        if "t" in batch:
            ts = batch["t"][idx]
        # Else, ts
        else:
            ts = batch["obs"][idx][3]
        obs_t = batch["obs"][idx]
        a_t = batch["actions"][idx]
        r_t = batch["rewards"][idx]
        state_in_0 = batch["state_in_0"][idx]
        state_in_1 = batch["state_in_1"][idx]

        # Check postprocessing outputs.
        if "postprocessed_column" in batch:
            postprocessed_col_t = batch["postprocessed_column"][idx]
            assert (obs_t == postprocessed_col_t / 2.0).all()

        # Check state-in/out and next-obs values.
        if idx > 0:
            next_obs_t_m_1 = batch["new_obs"][idx - 1]
            state_out_0_t_m_1 = batch["state_out_0"][idx - 1]
            state_out_1_t_m_1 = batch["state_out_1"][idx - 1]
            # Same trajectory as for t-1 -> Should be able to match.
            if (batch[SampleBatch.AGENT_INDEX][idx] ==
                    batch[SampleBatch.AGENT_INDEX][idx - 1]
                    and batch[SampleBatch.EPS_ID][idx] ==
                    batch[SampleBatch.EPS_ID][idx - 1]):
                assert batch["unroll_id"][idx - 1] == batch["unroll_id"][idx]
                assert (obs_t == next_obs_t_m_1).all()
                assert (state_in_0 == state_out_0_t_m_1).all()
                assert (state_in_1 == state_out_1_t_m_1).all()
            # Different trajectory.
            else:
                assert batch["unroll_id"][idx - 1] != batch["unroll_id"][idx]
                assert not (obs_t == next_obs_t_m_1).all()
                assert not (state_in_0 == state_out_0_t_m_1).all()
                assert not (state_in_1 == state_out_1_t_m_1).all()
                # Check initial 0-internal states.
                if ts == 0:
                    assert (state_in_0 == 0.0).all()
                    assert (state_in_1 == 0.0).all()

        # Check initial 0-internal states (at ts=0).
        if ts == 0:
            assert (state_in_0 == 0.0).all()
            assert (state_in_1 == 0.0).all()

        # Check prev. a/r values.
        if idx < count - 1:
            prev_actions_t_p_1 = batch["prev_actions"][idx + 1]
            prev_rewards_t_p_1 = batch["prev_rewards"][idx + 1]
            # Same trajectory as for t+1 -> Should be able to match.
            if batch[SampleBatch.AGENT_INDEX][idx] == \
                    batch[SampleBatch.AGENT_INDEX][idx + 1] and \
                    batch[SampleBatch.EPS_ID][idx] == \
                    batch[SampleBatch.EPS_ID][idx + 1]:
                assert (a_t == prev_actions_t_p_1).all()
                assert r_t == prev_rewards_t_p_1
            # Different (new) trajectory. Assume t-1 (prev-a/r) to be
            # always 0.0s. [3]=ts
            elif ts == 0:
                assert (prev_actions_t_p_1 == 0).all()
                assert prev_rewards_t_p_1 == 0.0

    pad_batch_to_sequences_of_same_size(
        batch,
        max_seq_len=max_seq_len,
        shuffle=False,
        batch_divisibility_req=1)

    # Check after seq-len 0-padding.
    cursor = 0
    for i, seq_len in enumerate(batch["seq_lens"]):
        state_in_0 = batch["state_in_0"][i]
        state_in_1 = batch["state_in_1"][i]
        for j in range(seq_len):
            k = cursor + j
            ts = batch["t"][k]
            obs_t = batch["obs"][k]
            a_t = batch["actions"][k]
            r_t = batch["rewards"][k]

            # Check postprocessing outputs.
            if "postprocessed_column" in batch:
                postprocessed_col_t = batch["postprocessed_column"][k]
                assert (obs_t == postprocessed_col_t / 2.0).all()

            # Check state-in/out and next-obs values.
            if j > 0:
                next_obs_t_m_1 = batch["new_obs"][k - 1]
                # state_out_0_t_m_1 = batch["state_out_0"][k - 1]
                # state_out_1_t_m_1 = batch["state_out_1"][k - 1]
                # Always same trajectory as for t-1.
                assert batch["unroll_id"][k - 1] == batch["unroll_id"][k]
                assert (obs_t == next_obs_t_m_1).all()
                # assert (state_in_0 == state_out_0_t_m_1).all())
                # assert (state_in_1 == state_out_1_t_m_1).all())
            # Check initial 0-internal states.
            elif ts == 0:
                assert (state_in_0 == 0.0).all()
                assert (state_in_1 == 0.0).all()

        for j in range(seq_len, max_seq_len):
            k = cursor + j
            obs_t = batch["obs"][k]
            a_t = batch["actions"][k]
            r_t = batch["rewards"][k]
            assert (obs_t == 0.0).all()
            assert (a_t == 0.0).all()
            assert (r_t == 0.0).all()

        cursor += max_seq_len


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
