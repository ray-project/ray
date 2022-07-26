import gym
import numpy as np
import unittest
import pytest

from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.lambdas import FlattenDataAgentConnector
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline
from ray.rllib.connectors.agent.state_buffer import StateBufferConnector
from ray.rllib.connectors.agent.view_requirement import ViewRequirementAgentConnector
from ray.rllib.connectors.connector import ConnectorContext, get_connector
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    AgentConnectorsOutput,
)

from ray.rllib.utils.test_utils import check


class TestAgentConnector(unittest.TestCase):
    def test_connector_pipeline(self):
        ctx = ConnectorContext()
        connectors = [ClipRewardAgentConnector(ctx, False, 1.0)]
        pipeline = AgentConnectorPipeline(ctx, connectors)
        name, params = pipeline.to_config()
        restored = get_connector(ctx, name, params)
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
        name, params = c.to_config()

        restored = get_connector(ctx, name, params)
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
        name, params = c.to_config()

        self.assertEqual(name, "ClipRewardAgentConnector")
        self.assertAlmostEqual(params["limit"], 2.0)

        restored = get_connector(ctx, name, params)
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

        name, params = c.to_config()
        restored = get_connector(ctx, name, params)
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
            # FlattenDataAgentConnector does NOT touch for_training dict,
            # so simply pass None here.
            AgentConnectorsOutput(None, sample_batch),
        )

        flattened = c([d])
        self.assertEqual(len(flattened), 1)

        batch = flattened[0].data.for_action
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

        c.on_policy_output(ActionConnectorDataType(0, 1, ([1, 2, 3], [], {})))

        with_buffered = c([d])
        self.assertEqual(len(with_buffered), 1)
        self.assertEqual(with_buffered[0].data[SampleBatch.ACTIONS], [1, 2, 3])

    def test_view_requirement_connector(self):
        # TODO: @kourosh remove this test when we have a better way to test
        view_requirements = {
            "obs": ViewRequirement(
                used_for_training=True, used_for_compute_actions=True
            ),
            "prev_actions": ViewRequirement(
                data_col="actions",
                shift=-1,
                used_for_training=True,
                used_for_compute_actions=True,
            ),
        }
        ctx = ConnectorContext(view_requirements=view_requirements)

        c = ViewRequirementAgentConnector(ctx)
        f = FlattenDataAgentConnector(ctx)

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.NEXT_OBS: {
                    "sensor1": [[1, 1], [2, 2]],
                    "sensor2": 8.8,
                },
                SampleBatch.ACTIONS: np.array(0),
            },
        )
        # ViewRequirementAgentConnector then FlattenAgentConnector.
        processed = f(c([d]))

        self.assertTrue("obs" in processed[0].data.for_action)
        self.assertTrue("prev_actions" in processed[0].data.for_action)


@pytest.mark.skip(reason="activate when view_requirement is fully implemented.")
class TestViewRequirementConnector(unittest.TestCase):
    def test_vr_connector_respects_training_or_inference_vr_flags(self):
        """Tests that the connector respects the flags within view_requirements (i.e.
        used_for_training, used_for_compute_actions) under different is_training modes.
        For inference,
            the returned data should be state -> obs
        For training,
            the returned data should be the data itself. The higher level policy
            collector in env_runner will construct the proper data structure.
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
        agent_data = dict(obs=obs_arr)
        data = AgentConnectorDataType(0, 1, agent_data)

        ctx = ConnectorContext(view_requirements=view_rq_dict)

        # TODO @jun What is the expected behavior of this test?
        for_action_expected_list = [
            # is_training = False
            SampleBatch({"both": obs_arr, "only_inference": obs_arr}),
            # is_training = True
            SampleBatch({"both": obs_arr, "only_inference": obs_arr}),
        ]

        for_training_expected_list = [
            # is_training = False
            None,
            # is_training = True
            agent_data,
        ]

        for is_training in [True, False]:
            c = ViewRequirementAgentConnector(ctx)
            c.is_training(is_training)
            processed = c([data])

            for_training = processed[0].data.for_training
            for_training_expected = for_training_expected_list[is_training]
            for_action = processed[0].data.for_action
            for_action_expected = for_action_expected_list[is_training]

            print("-" * 30)
            print(f"is_training = {is_training}")
            print("for action:")
            print(for_action)
            print("for training:")
            print(for_training)

            # TODO @jun is for_training expected to always be equal to data?
            check(for_training, for_training_expected)
            check(for_action, for_action_expected)

    def test_vr_connector_shift_by_one(self):
        """Test that the ViewRequirementConnector can handle shift by one correctly and
        can ignore future refrencing view_requirements to respect causality"""
        view_rq_dict = {
            "state": ViewRequirement("obs"),
            "next_state": ViewRequirement(
                "obs", shift=1, used_for_compute_actions=False
            ),
            "prev_state": ViewRequirement("obs", shift=-1),
        }

        obs_arrs = np.arange(10)[:, None] + 1
        ctx = ConnectorContext(view_requirements=view_rq_dict)
        c = ViewRequirementAgentConnector(ctx)

        for is_training in [True, False]:
            c.is_training(is_training)
            for i, obs_arr in enumerate(obs_arrs):
                data = AgentConnectorDataType(0, 1, dict(obs=obs_arr))
                processed = c([data])
                for_action = processed[0].data.for_action

                self.assertTrue("next_state" not in for_action)
                check(for_action["state"], obs_arrs[i])
                if i == 0:
                    check(for_action["prev_state"], np.array([0]))
                else:
                    check(for_action["prev_state"], obs_arrs[i - 1])

    def test_vr_connector_causal_slice(self):
        """Test that the ViewRequirementConnector can handle slice shifts correctly.

        This includes things like `-2:0:1`. `start:end:step` should be interpreted as
        np.arange(start, end, step). Both start and end have to be specified when using
        this format. If step is not specified it defaults to 1.
        """
        view_rq_dict = {
            "state": ViewRequirement("obs"),
            # shift array should be [-2, -1]
            "prev_states": ViewRequirement("obs", shift="-2:0"),
            # shift array should be [-4, -2]
            "prev_strided_states_even": ViewRequirement("obs", shift="-4:0:2"),
            # shift array should be [-3, -1]
            "prev_strided_states_odd": ViewRequirement("obs", shift="-3:0:2"),
        }

        obs_arrs = np.arange(10)[:, None] + 1
        ctx = ConnectorContext(view_requirements=view_rq_dict)
        c = ViewRequirementAgentConnector(ctx)

        for is_training in [True, False]:
            c.is_training(is_training)
            for i, obs_arr in enumerate(obs_arrs):
                data = AgentConnectorDataType(0, 1, dict(obs=obs_arr))
                processed = c([data])
                for_action = processed[0].data.for_action

                check(for_action["state"], obs_arrs[i])

                # check prev_states
                if i == 0:
                    check(for_action["prev_states"], np.array([[0], [0]]))
                elif i == 1:
                    check(for_action["prev_states"], np.array([[0], [1]]))
                else:
                    check(for_action["prev_states"], obs_arrs[i - 2 : i])

                # check strided states
                if i == 0:
                    # for this case they should all be equal to the padded value
                    check(
                        for_action["prev_states"],
                        for_action["prev_strided_states_even"],
                    )
                    check(
                        for_action["prev_states"], for_action["prev_strided_states_odd"]
                    )

                elif i == 1:
                    check(
                        for_action["prev_state"], for_action["prev_strided_states_even"]
                    )
                    check(
                        for_action["prev_strided_states_odd"],
                        np.array([[0], [1]]),  # [-2, 0]
                    )
                elif i == 2:
                    check(
                        for_action["prev_strided_states_even"],
                        np.array([[0], [1]]),  # [-2, 0]
                    )
                    check(
                        for_action["prev_strided_states_odd"],
                        np.array([[0], [2]]),  # [-1, 1]
                    )
                elif i == 3:
                    check(
                        for_action["prev_strided_states_even"],
                        np.array([[0], [2]]),  # [-1, 1]
                    )
                    check(
                        for_action["prev_strided_states_odd"],
                        np.array([[1], [3]]),  # [0, 2]
                    )
                else:
                    check(
                        for_action["prev_strided_states_even"], obs_arrs[i - 4 : i : 2]
                    )
                    check(
                        for_action["prev_strided_states_even"], obs_arrs[i - 3 : i : 2]
                    )

    def test_vr_connector_with_multiple_buffers(self):
        """Test that the ViewRequirementConnector can handle slice shifts correctly
        when it has multiple buffers to shift."""
        context_len = 5
        # This view requirement simulates the use-case of a decision transformer
        # without reward-to-go.
        view_rq_dict = {
            # obs[t-context_len:t+1]
            "context_obs": ViewRequirement("obs", shift=f"{-context_len}:1"),
            # act[t-context_len:t]
            "context_act": ViewRequirement("act", shift=f"{-context_len}:0"),
        }

        obs_arrs = np.arange(10)[:, None] + 1
        act_arrs = np.arange(10)[:, None] * 100 + 1
        n_steps = obs_arrs.shape[0]
        ctx = ConnectorContext(view_requirements=view_rq_dict)
        c = ViewRequirementAgentConnector(ctx)

        for is_training in [True, False]:
            c.is_training(is_training)
            for i in range(n_steps):
                data = AgentConnectorDataType(
                    0, 1, dict(obs=obs_arrs[i], act=act_arrs[i])
                )
                processed = c([data])
                for_action = processed[0].data.for_action

                if i < context_len:
                    check(
                        for_action["context_obs"],
                        np.concatenate([np.array([[0] * i]), obs_arrs[: i + 1]]),
                    )
                    check(
                        for_action["context_act"],
                        np.concatenate([np.array([[0] * i]), act_arrs[:i]]),
                    )
                else:
                    check(for_action["context_obs"], obs_arrs[i - context_len : i + 1])
                    check(for_action["context_act"], act_arrs[i - context_len : i])


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
