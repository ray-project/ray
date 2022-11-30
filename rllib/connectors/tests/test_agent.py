import gymnasium as gym
from gymnasium.spaces import Box
import numpy as np
import tree  # pip install dm_tree
import unittest

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_torch_policy import PPOTorchPolicy
from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.lambdas import FlattenDataAgentConnector
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline
from ray.rllib.connectors.agent.state_buffer import StateBufferConnector
from ray.rllib.connectors.agent.view_requirement import ViewRequirementAgentConnector
from ray.rllib.connectors.connector import ConnectorContext
from ray.rllib.connectors.registry import get_connector
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    AgentConnectorsOutput,
)
from ray.rllib.connectors.agent.mean_std_filter import (
    MeanStdObservationFilterAgentConnector,
)


class TestAgentConnector(unittest.TestCase):
    def test_connector_pipeline(self):
        ctx = ConnectorContext()
        connectors = [ClipRewardAgentConnector(ctx, False, 1.0)]
        pipeline = AgentConnectorPipeline(ctx, connectors)
        name, params = pipeline.to_state()
        restored = get_connector(name, ctx, params)
        self.assertTrue(isinstance(restored, AgentConnectorPipeline))
        self.assertTrue(isinstance(restored.connectors[0], ClipRewardAgentConnector))

    def test_obs_preprocessor_connector(self):
        obs_space = gym.spaces.Dict(
            {
                "a": gym.spaces.Box(low=0, high=1, shape=(1,)),
                "b": gym.spaces.Tuple(
                    [gym.spaces.Discrete(2), gym.spaces.MultiDiscrete(nvec=[2, 3])]
                ),
            }
        )
        ctx = ConnectorContext(config={}, observation_space=obs_space)

        c = ObsPreprocessorConnector(ctx)
        name, params = c.to_state()

        restored = get_connector(name, ctx, params)
        self.assertTrue(isinstance(restored, ObsPreprocessorConnector))

        obs = obs_space.sample()
        # Fake deterministic data.
        obs["a"][0] = 0.5
        obs["b"] = (1, np.array([0, 2]))

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.OBS: obs,
            },
        )
        preprocessed = c([d])

        # obs is completely flattened.
        self.assertTrue(
            (preprocessed[0].data[SampleBatch.OBS] == [0.5, 0, 1, 1, 0, 0, 0, 1]).all()
        )

    def test_clip_reward_connector(self):
        ctx = ConnectorContext()

        c = ClipRewardAgentConnector(ctx, limit=2.0)
        name, params = c.to_state()

        self.assertEqual(name, "ClipRewardAgentConnector")
        self.assertAlmostEqual(params["limit"], 2.0)

        restored = get_connector(name, ctx, params)
        self.assertTrue(isinstance(restored, ClipRewardAgentConnector))

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.REWARDS: 5.8,
            },
        )
        clipped = restored([d])

        self.assertEqual(len(clipped), 1)
        self.assertEqual(clipped[0].data[SampleBatch.REWARDS], 2.0)

    def test_flatten_data_connector(self):
        ctx = ConnectorContext()

        c = FlattenDataAgentConnector(ctx)

        name, params = c.to_state()
        restored = get_connector(name, ctx, params)
        self.assertTrue(isinstance(restored, FlattenDataAgentConnector))

        sample_batch = {
            SampleBatch.NEXT_OBS: {
                "sensor1": [[1, 1], [2, 2]],
                "sensor2": 8.8,
            },
            SampleBatch.REWARDS: 5.8,
            SampleBatch.ACTIONS: [[1, 1], [2]],
            SampleBatch.INFOS: {"random": "info"},
        }

        d = AgentConnectorDataType(
            0,
            1,
            # FlattenDataAgentConnector does NOT touch raw_dict,
            # so simply pass None here.
            AgentConnectorsOutput(None, sample_batch),
        )

        flattened = c([d])
        self.assertEqual(len(flattened), 1)

        batch = flattened[0].data.sample_batch
        self.assertTrue((batch[SampleBatch.NEXT_OBS] == [1, 1, 2, 2, 8.8]).all())
        self.assertEqual(batch[SampleBatch.REWARDS][0], 5.8)
        # Not flattened.
        self.assertEqual(len(batch[SampleBatch.ACTIONS]), 2)
        self.assertEqual(batch[SampleBatch.INFOS]["random"], "info")

    def test_state_buffer_connector(self):
        ctx = ConnectorContext(
            action_space=gym.spaces.Box(low=-1.0, high=1.0, shape=(3,)),
        )
        c = StateBufferConnector(ctx)

        # Reset without any buffered data should do nothing.
        c.reset(env_id=0)

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.NEXT_OBS: {
                    "sensor1": [[1, 1], [2, 2]],
                    "sensor2": 8.8,
                },
            },
        )

        with_buffered = c([d])
        self.assertEqual(len(with_buffered), 1)
        self.assertTrue((with_buffered[0].data[SampleBatch.ACTIONS] == [0, 0, 0]).all())

        c.on_policy_output(ActionConnectorDataType(0, 1, {}, ([1, 2, 3], [], {})))

        with_buffered = c([d])
        self.assertEqual(len(with_buffered), 1)
        self.assertEqual(with_buffered[0].data[SampleBatch.ACTIONS], [1, 2, 3])

    def test_mean_std_observation_filter_connector(self):
        for bounds in [
            (-1, 1),  # normalized
            (-2, 2),  # scaled
            (0, 2),  # shifted
            (0, 4),  # scaled and shifted
        ]:
            print("Testing uniform sampling with bounds: {}".format(bounds))

            observation_space = Box(bounds[0], bounds[1], (3, 64, 64))
            ctx = ConnectorContext(observation_space=observation_space)
            filter_connector = MeanStdObservationFilterAgentConnector(ctx)

            # Warm up Mean-Std filter
            for i in range(1000):
                obs = observation_space.sample()
                sample_batch = {
                    SampleBatch.NEXT_OBS: obs,
                }
                ac = AgentConnectorDataType(0, 0, sample_batch)
                filter_connector.transform(ac)

            # Create another connector to set state to
            _, state = filter_connector.to_state()
            another_filter_connector = (
                MeanStdObservationFilterAgentConnector.from_state(ctx, state)
            )

            another_filter_connector.in_eval()

            # Collect transformed observations
            transformed_observations = []
            for i in range(1000):
                obs = observation_space.sample()
                sample_batch = {
                    SampleBatch.NEXT_OBS: obs,
                }
                ac = AgentConnectorDataType(0, 0, sample_batch)
                connector_output = another_filter_connector.transform(ac)
                transformed_observations.append(
                    connector_output.data[SampleBatch.NEXT_OBS]
                )

            # Check if transformed observations are actually mean-std filtered
            self.assertTrue(np.isclose(np.mean(transformed_observations), 0, atol=0.1))
            self.assertTrue(np.isclose(np.var(transformed_observations), 1, atol=0.1))

            # Check if filter parameters where frozen because we are not training
            self.assertTrue(
                filter_connector.filter.running_stats.num_pushes
                == another_filter_connector.filter.running_stats.num_pushes,
            )
            self.assertTrue(
                np.all(
                    filter_connector.filter.running_stats.mean_array
                    == another_filter_connector.filter.running_stats.mean_array,
                )
            )
            self.assertTrue(
                np.all(
                    filter_connector.filter.running_stats.std_array
                    == another_filter_connector.filter.running_stats.std_array,
                )
            )
            self.assertTrue(
                filter_connector.filter.buffer.num_pushes
                == another_filter_connector.filter.buffer.num_pushes,
            )
            self.assertTrue(
                np.all(
                    filter_connector.filter.buffer.mean_array
                    == another_filter_connector.filter.buffer.mean_array,
                )
            )
            self.assertTrue(
                np.all(
                    filter_connector.filter.buffer.std_array
                    == another_filter_connector.filter.buffer.std_array,
                )
            )


class TestViewRequirementAgentConnector(unittest.TestCase):
    def test_vr_connector_respects_training_or_inference_vr_flags(self):
        """Tests that the connector respects the flags within view_requirements (i.e.
        used_for_training, used_for_compute_actions).

        the returned data is the input dict itself, which the policy collector in
        env_runner will use to construct the episode, and a SampleBatch that can be
        used to run corresponding policy.
        """
        view_rq_dict = {
            "both": ViewRequirement(
                "obs", used_for_training=True, used_for_compute_actions=True
            ),
            "only_inference": ViewRequirement(
                "obs", used_for_training=False, used_for_compute_actions=True
            ),
            "none": ViewRequirement(
                "obs", used_for_training=False, used_for_compute_actions=False
            ),
            "only_training": ViewRequirement(
                "obs", used_for_training=True, used_for_compute_actions=False
            ),
        }

        obs_arr = np.array([0, 1, 2, 3])
        agent_data = {SampleBatch.NEXT_OBS: obs_arr}
        data = AgentConnectorDataType(0, 1, agent_data)

        config = PPOConfig().to_dict()
        ctx = ConnectorContext(
            view_requirements=view_rq_dict,
            config=config,
            is_policy_recurrent=True,
        )

        sample_batch_expected = SampleBatch(
            {
                "both": obs_arr[None],
                # Output in training model as well.
                "only_inference": obs_arr[None],
                "seq_lens": np.array([1]),
            }
        )

        c = ViewRequirementAgentConnector(ctx)
        c.in_training()
        processed = c([data])

        raw_dict = processed[0].data.raw_dict
        sample_batch = processed[0].data.sample_batch

        check(raw_dict, agent_data)
        check(sample_batch, sample_batch_expected)

    def test_vr_connector_shift_by_one(self):
        """Test that the ViewRequirementAgentConnector can handle shift by one correctly and
        can ignore future referencing view_requirements to respect causality"""
        view_rq_dict = {
            "state": ViewRequirement("obs"),
            "next_state": ViewRequirement(
                "obs", shift=1, used_for_compute_actions=False
            ),
            "prev_state": ViewRequirement("obs", shift=-1),
        }

        obs_arrs = np.arange(10)[:, None] + 1
        config = PPOConfig().to_dict()
        ctx = ConnectorContext(
            view_requirements=view_rq_dict, config=config, is_policy_recurrent=True
        )
        c = ViewRequirementAgentConnector(ctx)

        # keep a running list of observations
        obs_list = []
        for t, obs in enumerate(obs_arrs):
            # t=0 is the next state of t=-1
            data = AgentConnectorDataType(0, 1, {SampleBatch.NEXT_OBS: obs})
            processed = c([data])  # env.reset() for t == -1 else env.step()
            sample_batch = processed[0].data.sample_batch
            # add cur obs to the list
            obs_list.append(obs)

            if t == 0:
                check(sample_batch["prev_state"], sample_batch["state"])
            else:
                # prev state should be equal to the prev time step obs
                check(sample_batch["prev_state"], obs_list[-2][None])

    def test_vr_connector_causal_slice(self):
        """Test that the ViewRequirementAgentConnector can handle slice shifts."""
        view_rq_dict = {
            "state": ViewRequirement("obs"),
            # shift array should be [-2, -1, 0]
            "prev_states": ViewRequirement("obs", shift="-2:0"),
            # shift array should be [-4, -2, 0]
            "prev_strided_states_even": ViewRequirement("obs", shift="-4:0:2"),
            # shift array should be [-3, -1]
            "prev_strided_states_odd": ViewRequirement("obs", shift="-3:0:2"),
        }

        obs_arrs = np.arange(10)[:, None] + 1
        config = PPOConfig().to_dict()
        ctx = ConnectorContext(
            view_requirements=view_rq_dict, config=config, is_policy_recurrent=True
        )
        c = ViewRequirementAgentConnector(ctx)

        # keep a queue of observations
        obs_list = []
        for t, obs in enumerate(obs_arrs):
            # t=0 is the next state of t=-1
            data = AgentConnectorDataType(0, 1, {SampleBatch.NEXT_OBS: obs})
            processed = c([data])
            sample_batch = processed[0].data.sample_batch

            if t == 0:
                obs_list.extend([obs for _ in range(5)])
            else:
                # remove the first obs and add the current obs to the end
                obs_list.pop(0)
                obs_list.append(obs)

            # check state
            check(sample_batch["state"], obs[None])

            # check prev_states
            check(
                sample_batch["prev_states"],
                np.stack(obs_list)[np.array([-3, -2, -1])][None],
            )

            # check prev_strided_states_even
            check(
                sample_batch["prev_strided_states_even"],
                np.stack(obs_list)[np.array([-5, -3, -1])][None],
            )

            check(
                sample_batch["prev_strided_states_odd"],
                np.stack(obs_list)[np.array([-4, -2])][None],
            )

    def test_vr_connector_with_multiple_buffers(self):
        """Test that the ViewRequirementAgentConnector can handle slice shifts correctly
        when it has multiple buffers to shift."""
        context_len = 5
        # This view requirement simulates the use-case of a decision transformer
        # without reward-to-go.
        view_rq_dict = {
            # obs[t-context_len+1:t]
            "context_obs": ViewRequirement(
                "obs",
                shift=f"-{context_len-1}:0",
                space=Box(-np.inf, np.inf, shape=(1,), dtype=np.float64),
            ),
            # next_obs[t-context_len+1:t]
            "context_next_obs": ViewRequirement(
                "obs",
                shift=f"-{context_len}:1",
                used_for_compute_actions=False,
                space=Box(-np.inf, np.inf, shape=(1,), dtype=np.float64),
            ),
            # act[t-context_len+1:t]
            "context_act": ViewRequirement(
                SampleBatch.ACTIONS,
                shift=f"-{context_len-1}:-1",
                space=Box(-np.inf, np.inf, shape=(1,)),
            ),
        }

        obs_arrs = np.arange(10)[:, None] + 1
        act_arrs = (np.arange(10)[:, None] + 1) * 100
        n_steps = obs_arrs.shape[0]
        config = PPOConfig().to_dict()
        ctx = ConnectorContext(
            view_requirements=view_rq_dict, config=config, is_policy_recurrent=True
        )
        c = ViewRequirementAgentConnector(ctx)

        # keep a queue of length ctx_len of observations
        obs_list, act_list = [], []
        for t in range(n_steps):
            # next state and action at time t-1 are the following
            timestep_data = {
                SampleBatch.NEXT_OBS: obs_arrs[t],
            }
            if t > 0:
                timestep_data[SampleBatch.ACTIONS] = act_arrs[t - 1]
            data = AgentConnectorDataType(0, 1, timestep_data)
            processed = c([data])
            sample_batch = processed[0].data.sample_batch

            if t == 0:
                obs_list.extend([obs_arrs[0] for _ in range(context_len)])
                act_list.extend(
                    [np.zeros_like(act_arrs[0]) for _ in range(context_len)]
                )
            else:
                obs_list.pop(0)
                act_list.pop(0)
                obs_list.append(obs_arrs[t])
                act_list.append(act_arrs[t - 1])

            self.assertTrue("context_next_obs" not in sample_batch)
            # We should have the 5 (context_len) most recent observations here
            check(sample_batch["context_obs"], np.stack(obs_list)[None])
            # The context for actions is [t-context_len+1:t]. Since we build sample
            # batch for inference in ViewRequirementAgentConnector, it always
            # includes everything up until the last action (at t-1), but not the
            # action current action (at t).
            check(sample_batch["context_act"], np.stack(act_list[1:])[None])

    def test_connector_pipline_with_view_requirement(self):
        """A very minimal test that checks wheter pipeline connectors work in a
        simulation rollout."""
        # TODO: make this test beefier and more comprehensive
        config = (
            PPOConfig()
            .framework("torch")
            .environment(env="CartPole-v1")
            .rollouts(create_env_on_local_worker=True)
        )

        env = gym.make("CartPole-v1")
        policy = PPOTorchPolicy(
            observation_space=env.observation_space,
            action_space=env.action_space,
            config=config.to_dict(),
        )

        # create a connector context
        ctx = ConnectorContext(
            view_requirements=policy.view_requirements,
            config=policy.config,
            initial_states=policy.get_initial_state(),
            is_policy_recurrent=policy.is_recurrent(),
            observation_space=policy.observation_space,
            action_space=policy.action_space,
        )

        # build chain of connectors
        connectors = [
            ObsPreprocessorConnector(ctx),
            StateBufferConnector(ctx),
            ViewRequirementAgentConnector(ctx),
        ]
        agent_connector = AgentConnectorPipeline(ctx, connectors)

        name, params = agent_connector.to_state()
        restored = get_connector(name, ctx, params)
        self.assertTrue(isinstance(restored, AgentConnectorPipeline))
        for cidx, c in enumerate(connectors):
            check(restored.connectors[cidx].to_state(), c.to_state())

        # simulate a rollout
        n_steps = 10
        obs, info = env.reset()
        env_out = AgentConnectorDataType(
            0, 1, {SampleBatch.NEXT_OBS: obs, SampleBatch.T: -1}
        )
        agent_obs = agent_connector([env_out])[0]
        t = 0
        total_rewards = 0
        while t < n_steps:
            policy_output = policy.compute_actions_from_input_dict(
                agent_obs.data.sample_batch
            )
            # Removes batch dimension
            policy_output = tree.map_structure(lambda x: x[0], policy_output)

            agent_connector.on_policy_output(
                ActionConnectorDataType(0, 1, {}, policy_output)
            )
            action = policy_output[0]

            next_obs, rewards, terminateds, truncateds, info = env.step(action)
            env_out_dict = {
                SampleBatch.NEXT_OBS: next_obs,
                SampleBatch.REWARDS: rewards,
                SampleBatch.TERMINATEDS: terminateds,
                SampleBatch.TRUNCATEDS: truncateds,
                SampleBatch.INFOS: info,
                SampleBatch.ACTIONS: action,
                # state_out
            }
            env_out = AgentConnectorDataType(0, 1, env_out_dict)
            agent_obs = agent_connector([env_out])[0]
            total_rewards += rewards
            t += 1
        print(total_rewards)

    def test_vr_connector_only_keeps_useful_timesteps(self):
        """Tests that the connector respects the flags within view_requirements (i.e.
        used_for_training, used_for_compute_actions).

        the returned data is the input dict itself, which the policy collector in
        env_runner will use to construct the episode, and a SampleBatch that can be
        used to run corresponding policy.
        """
        view_rqs = {
            "obs": ViewRequirement(
                None, used_for_training=True, used_for_compute_actions=True
            ),
        }

        config = PPOConfig().to_dict()
        ctx = ConnectorContext(
            view_requirements=view_rqs,
            config=config,
            is_policy_recurrent=False,
        )

        c = ViewRequirementAgentConnector(ctx)
        c.in_training()

        for i in range(5):
            obs_arr = np.array([0, 1, 2, 3]) + i
            agent_data = {SampleBatch.NEXT_OBS: obs_arr}
            data = AgentConnectorDataType(0, 1, agent_data)

            # Feed ViewRequirementAgentConnector 5 samples.
            c([data])

        obs_data = c.agent_collectors[0][1].buffers["obs"][0]
        # Only keep data for the last timestep.
        self.assertEqual(len(obs_data), 1)
        # Data matches the latest timestep.
        self.assertTrue(np.array_equal(obs_data[0], np.array([4, 5, 6, 7])))

    def test_vr_connector_default_agent_collector_is_empty(self):
        """Tests that after reset() the view_requirement connector will
        create a fresh new agent collector.
        """
        view_rqs = {
            "obs": ViewRequirement(
                None, used_for_training=True, used_for_compute_actions=True
            ),
        }

        config = PPOConfig().to_dict()
        ctx = ConnectorContext(
            view_requirements=view_rqs,
            config=config,
            is_policy_recurrent=False,
        )

        c = ViewRequirementAgentConnector(ctx)
        c.in_training()

        for i in range(5):
            obs_arr = np.array([0, 1, 2, 3]) + i
            agent_data = {SampleBatch.NEXT_OBS: obs_arr}
            data = AgentConnectorDataType(0, 1, agent_data)

            # Feed ViewRequirementAgentConnector 5 samples.
            c([data])

        # 1 init_obs, plus 4 agent steps.
        self.assertEqual(c.agent_collectors[0][1].agent_steps, 4)

        # Reset.
        c.reset(0)  # env_id = 0

        # Process a new timestep.
        obs_arr = np.array([0, 1, 2, 3]) + i
        agent_data = {SampleBatch.NEXT_OBS: obs_arr}
        data = AgentConnectorDataType(0, 1, agent_data)

        # Feed ViewRequirementAgentConnector 5 samples.
        c([data])

        # Start fresh with 0 agent step.
        self.assertEqual(c.agent_collectors[0][1].agent_steps, 0)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
