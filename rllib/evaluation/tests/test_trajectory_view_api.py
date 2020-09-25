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
        ray.init(local_mode=True) #TODO

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
            assert len(view_req_policy) == 11, view_req_policy
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
            assert len(view_req_model) == 7, view_req_model
            assert len(view_req_policy) == 17, view_req_policy
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
        obs_space = Box(-1.0, 1.0, shape=(700,))

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
                    k: sampler_perf_w.get(k, 0.0) +
                       (sampler_perf_[k] * 1000 / ts)
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
                      duration_w, sampler_perf_w, learn_time_w / num_iterations))
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
                    "max_seq_len": max_seq_len,
                },
            },
            rollout_fragment_length=rollout_fragment_length,
            policy=policies,
            policy_mapping_fn=policy_fn,
            num_envs=1,
        )
        for iteration in range(20):
            sc = rollout_worker.sampler.sample_collector
            pc = sc.policy_collectors["pol0"]
            buffers = pc.buffers
            result = rollout_worker.sample()
            pol_batch = result.policy_batches["pol0"]

            self.assertTrue(result.count == rollout_fragment_length)
            self.assertTrue(pol_batch.count >= rollout_fragment_length)

            # Check prev_reward/action, next_obs consistency.
            for t in range(pol_batch.count):
                obs_t = pol_batch["obs"][t]
                a_t = pol_batch["actions"][t]
                r_t = pol_batch["rewards"][t]
                state_in_0 = pol_batch["state_in_0"][t]
                state_in_1 = pol_batch["state_in_1"][t]

                # Check postprocessing outputs.
                postprocessed_col_t = pol_batch["postprocessed_column"][t]
                self.assertTrue((obs_t == postprocessed_col_t - 1.0).all())

                # Check state-in/out and next-obs values.
                if t > 0:
                    next_obs_t_m_1 = pol_batch["new_obs"][t - 1]
                    state_out_0_t_m_1 = pol_batch["state_out_0"][t - 1]
                    state_out_1_t_m_1 = pol_batch["state_out_1"][t - 1]
                    # Same trajectory as for t-1 -> Should be able to match.
                    if (pol_batch[SampleBatch.AGENT_INDEX][t] ==
                            pol_batch[SampleBatch.AGENT_INDEX][t - 1] and
                            pol_batch[SampleBatch.EPS_ID][t] ==
                            pol_batch[SampleBatch.EPS_ID][t - 1]):
                        self.assertTrue((obs_t == next_obs_t_m_1).all())
                        self.assertTrue((state_in_0 == state_out_0_t_m_1).all())
                        self.assertTrue((state_in_1 == state_out_1_t_m_1).all())
                    # Different trajectory.
                    else:
                        self.assertFalse((obs_t == next_obs_t_m_1).all())
                        self.assertFalse((state_in_0 == state_out_0_t_m_1).all())
                        self.assertFalse((state_in_1 == state_out_1_t_m_1).all())
                        # Check initial 0-internal states.
                        if pol_batch["dones"][t - 1]:
                            self.assertTrue((state_in_0 == 0.0).all())
                            self.assertTrue((state_in_1 == 0.0).all())

                # Check initial 0-internal states (at ts==0; obs[3] is always
                # the ts).
                if pol_batch["obs"][t][3] == 0:
                    self.assertTrue((state_in_0 == 0.0).all())
                    self.assertTrue((state_in_1 == 0.0).all())

                # Check prev. a/r values.
                if t < pol_batch.count - 1:
                    prev_actions_t_p_1 = pol_batch["prev_actions"][t + 1]
                    prev_rewards_t_p_1 = pol_batch["prev_rewards"][t + 1]
                    # Same trajectory as for t+1 -> Should be able to match.
                    if pol_batch[SampleBatch.AGENT_INDEX][t] == \
                            pol_batch[SampleBatch.AGENT_INDEX][t + 1] and \
                            pol_batch[SampleBatch.EPS_ID][t] == \
                            pol_batch[SampleBatch.EPS_ID][t + 1]:
                        self.assertTrue((a_t == prev_actions_t_p_1).all())
                        self.assertTrue(r_t == prev_rewards_t_p_1)
                    # Different (new) trajectory. Assume t-1 (prev-a/r) to be
                    # always 0.0s.
                    else:
                        self.assertTrue((prev_actions_t_p_1 == 0).all())
                        self.assertTrue(prev_rewards_t_p_1 == 0.0)

            # Check seq-lens.
            #for agent_slot, seq_len in enumerate(pol_batch.seq_lens):
            #    if seq_len < rollout_fragment_length - 1:
            #        # At least in the beginning, the next slots should always
            #        # be empty (once all agent slots have been used once, these
            #        # may be filled with "old" values (from longer sequences)).
            #        if iteration < 8:
            #            self.assertTrue(
            #                (pol_batch["obs"][seq_len +
            #                                  1][agent_slot] == 0.0).all())
            #            self.assertTrue(
            #                (pol_batch["rewards"][seq_len][agent_slot] == 0.0
            #                 ).all())
            #            self.assertTrue(
            #                (pol_batch["actions"][seq_len][agent_slot] == 0.0
            #                 ).all())
            #        self.assertFalse(
            #            (pol_batch["obs"][seq_len][agent_slot] == 0.0).all())


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
