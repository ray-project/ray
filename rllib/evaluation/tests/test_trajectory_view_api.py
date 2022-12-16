import gym
from gym.spaces import Box, Discrete
import numpy as np
import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
import ray.rllib.algorithms.dqn as dqn
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.examples.env.debug_counter_env import MultiAgentDebugCounterEnv
from ray.rllib.examples.env.multi_agent import MultiAgentPendulum
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.policy.episode_env_aware_policy import (
    EpisodeEnvAwareAttentionPolicy,
    EpisodeEnvAwareLSTMPolicy,
)
from ray.rllib.models.tf.attention_net import GTrXLNet
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    SampleBatch,
    convert_ma_batch_to_sample_batch,
)
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.test_utils import framework_iterator, check


class MyCallbacks(DefaultCallbacks):
    @override(DefaultCallbacks)
    def on_learn_on_batch(self, *, policy, train_batch, result, **kwargs):
        assert train_batch.count == 201
        assert sum(train_batch[SampleBatch.SEQ_LENS]) == 201
        for k, v in train_batch.items():
            if k in ["state_in_0", SampleBatch.SEQ_LENS]:
                assert len(v) == len(train_batch[SampleBatch.SEQ_LENS])
            else:
                assert len(v) == 201
        current = None
        for o in train_batch[SampleBatch.OBS]:
            if current:
                assert o == current + 1
            current = o
            if o == 15:
                current = None


class TestTrajectoryViewAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_traj_view_normal_case(self):
        """Tests, whether Model and Policy return the correct ViewRequirements."""
        config = (
            dqn.DQNConfig()
            .rollouts(num_envs_per_worker=10, rollout_fragment_length=4)
            .environment("ray.rllib.examples.env.debug_counter_env.DebugCounterEnv")
        )

        for _ in framework_iterator(config):
            algo = config.build()
            policy = algo.get_policy()
            view_req_model = policy.model.view_requirements
            view_req_policy = policy.view_requirements
            assert len(view_req_model) == 1, view_req_model
            assert len(view_req_policy) == 11, view_req_policy
            for key in [
                SampleBatch.OBS,
                SampleBatch.ACTIONS,
                SampleBatch.REWARDS,
                SampleBatch.DONES,
                SampleBatch.NEXT_OBS,
                SampleBatch.EPS_ID,
                SampleBatch.AGENT_INDEX,
                "weights",
            ]:
                assert key in view_req_policy
                # None of the view cols has a special underlying data_col,
                # except next-obs.
                if key != SampleBatch.NEXT_OBS:
                    assert view_req_policy[key].data_col is None
                else:
                    assert view_req_policy[key].data_col == SampleBatch.OBS
                    assert view_req_policy[key].shift == 1
            rollout_worker = algo.workers.local_worker()
            sample_batch = rollout_worker.sample()
            sample_batch = convert_ma_batch_to_sample_batch(sample_batch)
            expected_count = config.num_envs_per_worker * config.rollout_fragment_length
            assert sample_batch.count == expected_count
            for v in sample_batch.values():
                assert len(v) == expected_count
            algo.stop()

    def test_traj_view_lstm_prev_actions_and_rewards(self):
        """Tests, whether Policy/Model return correct LSTM ViewRequirements."""
        config = (
            ppo.PPOConfig()
            .environment("CartPole-v1")
            # Activate LSTM + prev-action + rewards.
            .training(
                model={
                    "use_lstm": True,
                    "lstm_use_prev_action": True,
                    "lstm_use_prev_reward": True,
                }
            )
            .rollouts(create_env_on_local_worker=True)
        )

        for _ in framework_iterator(config):
            algo = config.build()
            policy = algo.get_policy()
            view_req_model = policy.model.view_requirements
            view_req_policy = policy.view_requirements
            # 7=obs, prev-a + r, 2x state-in, 2x state-out.
            assert len(view_req_model) == 7, view_req_model
            assert len(view_req_policy) == 21, (len(view_req_policy), view_req_policy)
            for key in [
                SampleBatch.OBS,
                SampleBatch.ACTIONS,
                SampleBatch.REWARDS,
                SampleBatch.DONES,
                SampleBatch.NEXT_OBS,
                SampleBatch.VF_PREDS,
                SampleBatch.PREV_ACTIONS,
                SampleBatch.PREV_REWARDS,
                "advantages",
                "value_targets",
                SampleBatch.ACTION_DIST_INPUTS,
                SampleBatch.ACTION_LOGP,
            ]:
                assert key in view_req_policy

                if key == SampleBatch.PREV_ACTIONS:
                    assert view_req_policy[key].data_col == SampleBatch.ACTIONS
                    assert view_req_policy[key].shift == -1
                elif key == SampleBatch.PREV_REWARDS:
                    assert view_req_policy[key].data_col == SampleBatch.REWARDS
                    assert view_req_policy[key].shift == -1
                elif key not in [
                    SampleBatch.NEXT_OBS,
                    SampleBatch.PREV_ACTIONS,
                    SampleBatch.PREV_REWARDS,
                ]:
                    assert view_req_policy[key].data_col is None
                else:
                    assert view_req_policy[key].data_col == SampleBatch.OBS
                    assert view_req_policy[key].shift == 1

            rollout_worker = algo.workers.local_worker()
            sample_batch = rollout_worker.sample()
            sample_batch = convert_ma_batch_to_sample_batch(sample_batch)

            # Rollout fragment length should be auto-computed to 2000:
            # 2 workers, 1 env per worker, train batch size=4000 -> 2000 per worker.
            self.assertEqual(sample_batch.count, 2000, "ppo rollout count != 2000")
            self.assertEqual(sum(sample_batch["seq_lens"]), sample_batch.count)
            self.assertEqual(
                len(sample_batch["seq_lens"]), sample_batch["state_in_0"].shape[0]
            )

            # check if non-zero state_ins are pointing to the correct state_outs
            seq_counters = np.cumsum(sample_batch["seq_lens"])
            for i in range(sample_batch["state_in_0"].shape[0]):
                state_in = sample_batch["state_in_0"][i]
                if np.any(state_in != 0):
                    # non-zero state-in should be one of th state_outs.
                    state_out_ind = seq_counters[i - 1] - 1
                    check(sample_batch["state_out_0"][state_out_ind], state_in)
            algo.stop()

    def test_traj_view_attention_net(self):
        config = (
            ppo.PPOConfig()
            .environment(
                "ray.rllib.examples.env.debug_counter_env.DebugCounterEnv",
                env_config={"config": {"start_at_t": 1}},  # first obs is [1.0]
            )
            .rollouts(num_rollout_workers=0)
            .callbacks(MyCallbacks)
            # Setup attention net.
            .training(
                model={
                    "custom_model": GTrXLNet,
                    "custom_model_config": {
                        "num_transformer_units": 1,
                        "attention_dim": 64,
                        "num_heads": 2,
                        "memory_inference": 50,
                        "memory_training": 50,
                        "head_dim": 32,
                        "ff_hidden_dim": 32,
                    },
                    "max_seq_len": 50,
                },
                # Test with odd batch numbers.
                train_batch_size=1031,
                sgd_minibatch_size=201,
                num_sgd_iter=5,
            )
        )

        for _ in framework_iterator(config, frameworks="tf2"):
            algo = config.build()
            rw = algo.workers.local_worker()
            sample = rw.sample()
            assert sample.count == algo.config.get_rollout_fragment_length()
            results = algo.train()
            assert results["timesteps_total"] == config["train_batch_size"]
            algo.stop()

    def test_traj_view_next_action(self):
        action_space = Discrete(2)
        rollout_worker_w_api = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v1"),
            default_policy_class=ppo.PPOTorchPolicy,
            config=ppo.PPOConfig().rollouts(
                rollout_fragment_length=200, num_rollout_workers=0
            ),
        )
        # Add the next action (a') and 2nd next action (a'') to the view
        # requirements of the policy.
        # This should be visible then in postprocessing and train batches.
        # Switch off for action computations (can't be there as we don't know
        # the next actions already at action computation time).
        rollout_worker_w_api.policy_map[DEFAULT_POLICY_ID].view_requirements[
            "next_actions"
        ] = ViewRequirement(
            SampleBatch.ACTIONS,
            shift=1,
            space=action_space,
            used_for_compute_actions=False,
        )
        rollout_worker_w_api.policy_map[DEFAULT_POLICY_ID].view_requirements[
            "2nd_next_actions"
        ] = ViewRequirement(
            SampleBatch.ACTIONS,
            shift=2,
            space=action_space,
            used_for_compute_actions=False,
        )

        # Make sure, we have DONEs as well.
        rollout_worker_w_api.policy_map[DEFAULT_POLICY_ID].view_requirements[
            "dones"
        ] = ViewRequirement()
        batch = convert_ma_batch_to_sample_batch(rollout_worker_w_api.sample())
        self.assertTrue("next_actions" in batch)
        self.assertTrue("2nd_next_actions" in batch)
        expected_a_ = None  # expected next action
        expected_a__ = None  # expected 2nd next action
        for i in range(len(batch["actions"])):
            a, d, a_, a__ = (
                batch["actions"][i],
                batch["dones"][i],
                batch["next_actions"][i],
                batch["2nd_next_actions"][i],
            )
            # Episode done: next action and 2nd next action should be 0.
            if d:
                check(a_, 0)
                check(a__, 0)
                expected_a_ = None
                expected_a__ = None
                continue
            # Episode is not done and we have an expected next-a.
            if expected_a_ is not None:
                check(a, expected_a_)
            if expected_a__ is not None:
                check(a_, expected_a__)
            expected_a__ = a__
            expected_a_ = a_

    def test_traj_view_lstm_functionality(self):
        action_space = Box(float("-inf"), float("inf"), shape=(3,))
        obs_space = Box(float("-inf"), float("inf"), (4,))
        max_seq_len = 50
        rollout_fragment_length = 200
        assert rollout_fragment_length % max_seq_len == 0
        policies = {
            "pol0": (EpisodeEnvAwareLSTMPolicy, obs_space, action_space, None),
        }

        def policy_fn(agent_id, episode, worker, **kwargs):
            return "pol0"

        rw = RolloutWorker(
            env_creator=lambda _: MultiAgentDebugCounterEnv({"num_agents": 4}),
            config=ppo.PPOConfig()
            .rollouts(
                rollout_fragment_length=rollout_fragment_length,
                num_rollout_workers=0,
            )
            .multi_agent(
                policies=policies,
                policy_mapping_fn=policy_fn,
            )
            .environment(normalize_actions=False)
            .training(
                model={
                    "use_lstm": True,
                    "max_seq_len": max_seq_len,
                }
            ),
        )

        for iteration in range(20):
            result = rw.sample()
            check(result.count, rollout_fragment_length)
            pol_batch_w = result.policy_batches["pol0"]
            assert pol_batch_w.count >= rollout_fragment_length
            analyze_rnn_batch(
                pol_batch_w,
                max_seq_len,
                view_requirements=rw.policy_map["pol0"].view_requirements,
            )

    def test_traj_view_attention_functionality(self):
        action_space = Box(float("-inf"), float("inf"), shape=(3,))
        obs_space = Box(float("-inf"), float("inf"), (4,))
        max_seq_len = 50
        rollout_fragment_length = 201
        policies = {
            "pol0": (EpisodeEnvAwareAttentionPolicy, obs_space, action_space, None),
        }

        def policy_fn(agent_id, episode, worker, **kwargs):
            return "pol0"

        config = (
            ppo.PPOConfig()
            .multi_agent(policies=policies, policy_mapping_fn=policy_fn)
            .training(model={"max_seq_len": max_seq_len}, train_batch_size=2010)
            .rollouts(
                num_rollout_workers=0,
                rollout_fragment_length=rollout_fragment_length,
            )
            .environment(normalize_actions=False)
        )

        rollout_worker_w_api = RolloutWorker(
            env_creator=lambda _: MultiAgentDebugCounterEnv({"num_agents": 4}),
            config=config,
        )
        batch = rollout_worker_w_api.sample()  # noqa: F841

    def test_counting_by_agent_steps(self):
        num_agents = 3

        config = ppo.PPOConfig()
        # Env setup.
        config.environment(MultiAgentPendulum, env_config={"num_agents": num_agents})
        config.rollouts(num_rollout_workers=2, rollout_fragment_length=21)
        config.training(num_sgd_iter=2, train_batch_size=168)
        config.framework("torch")
        config.multi_agent(
            policies={f"p{i}" for i in range(num_agents)},
            policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: (
                "p{}".format(agent_id)
            ),
            count_steps_by="agent_steps",
        )

        num_iterations = 2
        algo = config.build()
        results = None
        for i in range(num_iterations):
            results = algo.train()
        self.assertEqual(results["agent_timesteps_total"], results["timesteps_total"])
        self.assertEqual(
            results["num_env_steps_trained"] * num_agents,
            results["num_agent_steps_trained"],
        )
        self.assertGreaterEqual(
            results["agent_timesteps_total"],
            num_iterations * config.train_batch_size,
        )
        self.assertLessEqual(
            results["agent_timesteps_total"],
            (num_iterations + 1) * config.train_batch_size,
        )
        algo.stop()

    def test_get_single_step_input_dict_batch_repeat_value_larger_1(self):
        """Test whether a SampleBatch produces the correct 1-step input dict."""
        space = Box(-1.0, 1.0, ())

        # With batch-repeat-value > 1: state_in_0 is only built every n
        # timesteps.
        view_reqs = {
            "state_in_0": ViewRequirement(
                data_col="state_out_0",
                shift="-5:-1",
                space=space,
                batch_repeat_value=5,
            ),
            "state_out_0": ViewRequirement(space=space, used_for_compute_actions=False),
        }

        # Trajectory of 1 ts (0) (we would like to compute the 1st).
        batch = SampleBatch(
            {
                "state_in_0": np.array(
                    [
                        [0, 0, 0, 0, 0],  # ts=0
                    ]
                ),
                "state_out_0": np.array([1]),
            }
        )
        input_dict = batch.get_single_step_input_dict(
            view_requirements=view_reqs, index="last"
        )
        check(
            input_dict,
            {
                "state_in_0": [[0, 0, 0, 0, 1]],  # ts=1
                "seq_lens": [1],
            },
        )

        # Trajectory of 6 ts (0-5) (we would like to compute the 6th).
        batch = SampleBatch(
            {
                "state_in_0": np.array(
                    [
                        [0, 0, 0, 0, 0],  # ts=0
                        [1, 2, 3, 4, 5],  # ts=5
                    ]
                ),
                "state_out_0": np.array([1, 2, 3, 4, 5, 6]),
            }
        )
        input_dict = batch.get_single_step_input_dict(
            view_requirements=view_reqs, index="last"
        )
        check(
            input_dict,
            {
                "state_in_0": [[2, 3, 4, 5, 6]],  # ts=6
                "seq_lens": [1],
            },
        )

        # Trajectory of 12 ts (0-11) (we would like to compute the 12th).
        batch = SampleBatch(
            {
                "state_in_0": np.array(
                    [
                        [0, 0, 0, 0, 0],  # ts=0
                        [1, 2, 3, 4, 5],  # ts=5
                        [6, 7, 8, 9, 10],  # ts=10
                    ]
                ),
                "state_out_0": np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            }
        )
        input_dict = batch.get_single_step_input_dict(
            view_requirements=view_reqs, index="last"
        )
        check(
            input_dict,
            {
                "state_in_0": [[8, 9, 10, 11, 12]],  # ts=12
                "seq_lens": [1],
            },
        )

    def test_get_single_step_input_dict_batch_repeat_value_1(self):
        """Test whether a SampleBatch produces the correct 1-step input dict."""
        space = Box(-1.0, 1.0, ())

        # With batch-repeat-value==1: state_in_0 is built each timestep.
        view_reqs = {
            "state_in_0": ViewRequirement(
                data_col="state_out_0",
                shift="-5:-1",
                space=space,
                batch_repeat_value=1,
            ),
            "state_out_0": ViewRequirement(space=space, used_for_compute_actions=False),
        }

        # Trajectory of 1 ts (0) (we would like to compute the 1st).
        batch = SampleBatch(
            {
                "state_in_0": np.array(
                    [
                        [0, 0, 0, 0, 0],  # ts=0
                    ]
                ),
                "state_out_0": np.array([1]),
            }
        )
        input_dict = batch.get_single_step_input_dict(
            view_requirements=view_reqs, index="last"
        )
        check(
            input_dict,
            {
                "state_in_0": [[0, 0, 0, 0, 1]],  # ts=1
                "seq_lens": [1],
            },
        )

        # Trajectory of 6 ts (0-5) (we would like to compute the 6th).
        batch = SampleBatch(
            {
                "state_in_0": np.array(
                    [
                        [0, 0, 0, 0, 0],  # ts=0
                        [0, 0, 0, 0, 1],  # ts=1
                        [0, 0, 0, 1, 2],  # ts=2
                        [0, 0, 1, 2, 3],  # ts=3
                        [0, 1, 2, 3, 4],  # ts=4
                        [1, 2, 3, 4, 5],  # ts=5
                    ]
                ),
                "state_out_0": np.array([1, 2, 3, 4, 5, 6]),
            }
        )
        input_dict = batch.get_single_step_input_dict(
            view_requirements=view_reqs, index="last"
        )
        check(
            input_dict,
            {
                "state_in_0": [[2, 3, 4, 5, 6]],  # ts=6
                "seq_lens": [1],
            },
        )

        # Trajectory of 12 ts (0-11) (we would like to compute the 12th).
        batch = SampleBatch(
            {
                "state_in_0": np.array(
                    [
                        [0, 0, 0, 0, 0],  # ts=0
                        [0, 0, 0, 0, 1],  # ts=1
                        [0, 0, 0, 1, 2],  # ts=2
                        [0, 0, 1, 2, 3],  # ts=3
                        [0, 1, 2, 3, 4],  # ts=4
                        [1, 2, 3, 4, 5],  # ts=5
                        [2, 3, 4, 5, 6],  # ts=6
                        [3, 4, 5, 6, 7],  # ts=7
                        [4, 5, 6, 7, 8],  # ts=8
                        [5, 6, 7, 8, 9],  # ts=9
                        [6, 7, 8, 9, 10],  # ts=10
                        [7, 8, 9, 10, 11],  # ts=11
                    ]
                ),
                "state_out_0": np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            }
        )
        input_dict = batch.get_single_step_input_dict(
            view_requirements=view_reqs, index="last"
        )
        check(
            input_dict,
            {
                "state_in_0": [[8, 9, 10, 11, 12]],  # ts=12
                "seq_lens": [1],
            },
        )


def analyze_rnn_batch(batch, max_seq_len, view_requirements):
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
        if "2xobs" in batch:
            postprocessed_col_t = batch["2xobs"][idx]
            assert (obs_t == postprocessed_col_t / 2.0).all()

        # Check state-in/out and next-obs values.
        if idx > 0:
            next_obs_t_m_1 = batch["new_obs"][idx - 1]
            state_out_0_t_m_1 = batch["state_out_0"][idx - 1]
            state_out_1_t_m_1 = batch["state_out_1"][idx - 1]
            # Same trajectory as for t-1 -> Should be able to match.
            if (
                batch[SampleBatch.AGENT_INDEX][idx]
                == batch[SampleBatch.AGENT_INDEX][idx - 1]
                and batch[SampleBatch.EPS_ID][idx] == batch[SampleBatch.EPS_ID][idx - 1]
            ):
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
            if (
                batch[SampleBatch.AGENT_INDEX][idx]
                == batch[SampleBatch.AGENT_INDEX][idx + 1]
                and batch[SampleBatch.EPS_ID][idx] == batch[SampleBatch.EPS_ID][idx + 1]
            ):
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
        batch_divisibility_req=1,
        view_requirements=view_requirements,
    )

    # Check after seq-len 0-padding.
    cursor = 0
    for i, seq_len in enumerate(batch[SampleBatch.SEQ_LENS]):
        state_in_0 = batch["state_in_0"][i]
        state_in_1 = batch["state_in_1"][i]
        for j in range(seq_len):
            k = cursor + j
            ts = batch["t"][k]
            obs_t = batch["obs"][k]
            a_t = batch["actions"][k]
            r_t = batch["rewards"][k]

            # Check postprocessing outputs.
            if "2xobs" in batch:
                postprocessed_col_t = batch["2xobs"][k]
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
