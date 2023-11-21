import gymnasium as gym
import numpy as np
import unittest

from gymnasium.core import ActType, ObsType
from typing import Any, Dict, Optional, SupportsFloat, Tuple

import ray
from ray.rllib.env.single_agent_episode import SingleAgentEpisode

# TODO (simon): Add to the tests `info` and `extra_model_outputs`
#  as soon as #39732 is merged.


class TestEnv(gym.Env):
    def __init__(self):
        self.observation_space = gym.spaces.Discrete(201)
        self.action_space = gym.spaces.Discrete(200)
        self.t = 0

    def reset(
        self, *, seed: Optional[int] = None, options=Optional[Dict[str, Any]]
    ) -> Tuple[ObsType, Dict[str, Any]]:
        self.t = 0
        return 0, {}

    def step(
        self, action: ActType
    ) -> Tuple[ObsType, SupportsFloat, bool, bool, Dict[str, Any]]:
        self.t += 1
        if self.t == 200:
            is_terminated = True
        else:
            is_terminated = False

        return self.t, self.t, is_terminated, False, {}


class TestSingelAgentEpisode(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_init(self):
        """Tests initialization of `SingleAgentEpisode`.

        Three cases are tested:
            1. Empty episode with default starting timestep.
            2. Empty episode starting at `t_started=10`. This is only interesting
                for ongoing episodes, where we do not want to carry on the stale
                entries from the last rollout.
            3. Initialization with pre-collected data.
        """
        # Create empty episode.
        episode = SingleAgentEpisode()
        # Empty episode should have a start point and count of zero.
        self.assertTrue(episode.t_started == episode.t == 0)

        # Create an episode with a specific starting point.
        episode = SingleAgentEpisode(t_started=10)
        self.assertTrue(episode.t == episode.t_started == 10)

        # Sample 100 values and initialize episode with observations and infos.
        env = gym.make("CartPole-v1")
        # Initialize containers.
        observations = []
        rewards = []
        actions = []
        infos = []
        extra_model_outputs = []
        states = np.random.random(10)

        # Initialize observation and info.
        init_obs, init_info = env.reset()
        observations.append(init_obs)
        infos.append(init_info)
        # Run 100 samples.
        for _ in range(100):
            action = env.action_space.sample()
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            observations.append(obs)
            actions.append(action)
            rewards.append(reward)
            infos.append(info)
            extra_model_outputs.append({"extra_1": np.random.random()})

        # Build the episode.
        episode = SingleAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            states=states,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
            extra_model_outputs=extra_model_outputs,
        )
        # The starting point and count should now be at `len(observations) - 1`.
        self.assertTrue(episode.t == episode.t_started == (len(observations) - 1))

    def test_add_initial_observation(self):
        """Tests adding initial observations and infos.

        This test ensures that when initial observation and info are provided
        the length of the lists are correct and the timestep is still at zero,
        as the agent has not stepped, yet.
        """
        # Create empty episode.
        episode = SingleAgentEpisode()
        # Create environment.
        env = gym.make("CartPole-v1")

        # Add initial observations.
        obs, info = env.reset()
        episode.add_initial_observation(initial_observation=obs, initial_info=info)

        # Assert that the observations are added to their list.
        self.assertTrue(len(episode.observations) == 1)
        # Assert that the infos are added to their list.
        self.assertTrue(len(episode.infos) == 1)
        # Assert that the timesteps are still at zero as we have not stepped, yet.
        self.assertTrue(episode.t == episode.t_started == 0)

    def test_add_timestep(self):
        """Tests if adding timestep data to a `SingleAgentEpisode` works.

        Adding timestep data is the central part of collecting episode
        dara. Here it is tested if adding to the internal data lists
        works as intended and the timestep is increased during each step.
        """
        # Create an empty episode and add initial observations.
        episode = SingleAgentEpisode()
        env = gym.make("CartPole-v1")
        # Set the random seed (otherwise the episode will terminate at
        # different points in each test run).
        obs, info = env.reset(seed=0)
        episode.add_initial_observation(initial_observation=obs, initial_info=info)

        # Sample 100 timesteps and add them to the episode.
        for i in range(100):
            action = env.action_space.sample()
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                extra_model_output={"extra": np.random.random(1)},
            )
            if is_terminated or is_truncated:
                break

        # Assert that the episode timestep is at 100.
        self.assertTrue(episode.t == len(episode.observations) - 1 == i + 1)
        # Assert that `t_started` stayed at zero.
        self.assertTrue(episode.t_started == 0)
        # Assert that all lists have the proper lengths.
        self.assertTrue(
            len(episode.actions)
            == len(episode.rewards)
            == len(episode.observations) - 1
            == len(episode.infos) - 1
            == i + 1
        )
        # Assert that the flags are set correctly.
        self.assertTrue(episode.is_terminated == is_terminated)
        self.assertTrue(episode.is_truncated == is_truncated)
        self.assertTrue(episode.is_done == is_terminated or is_truncated)

    def test_create_successor(self):
        """Tests creation of a scucessor of a `SingleAgentEpisode`.

        This test makes sure that when creating a successor the successor's
        data is coherent with the episode that should be succeeded.
        Observation and info are available before each timestep; therefore
        these data is carried over to the successor.
        """

        # Create an empty episode.
        episode_1 = SingleAgentEpisode()
        # Create an environment.
        env = TestEnv()
        # Add initial observation.
        init_obs, init_info = env.reset()
        episode_1.add_initial_observation(
            initial_observation=init_obs, initial_info=init_info
        )
        # Sample 100 steps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode_1.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                extra_model_output={"extra": np.random.random(1)},
            )

        # Assert that the episode has indeed 100 timesteps.
        self.assertTrue(episode_1.t == 100)

        # Create a successor.
        episode_2 = episode_1.create_successor()
        # Assert that it has the same id.
        self.assertTrue(episode_1.id_ == episode_2.id_)
        # Assert that the timestep starts at the end of the last episode.
        self.assertTrue(episode_1.t == episode_2.t == episode_2.t_started)
        # Assert that the last observation of `episode_1` is the first of
        # `episode_2`.
        self.assertTrue(episode_1.observations[-1] == episode_2.observations[0])
        # Assert that the last info of `episode_1` is the first of episode_2`.
        self.assertTrue(episode_1.infos[-1] == episode_2.infos[0])

        # Test immutability.
        action = 100
        obs, reward, is_terminated, is_truncated, info = env.step(action)
        episode_2.add_timestep(
            observation=obs,
            action=action,
            reward=reward,
            info=info,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
            extra_model_output={"extra": np.random.random(1)},
        )
        # Assert that this does not change also the predecessor's data.
        self.assertFalse(len(episode_1.observations) == len(episode_2.observations))

    def test_concat_episode(self):
        """Tests if concatenation of two `SingleAgentEpisode`s works.

        This test ensures that concatenation of two episodes work. Note that
        concatenation should only work for two chunks of the same episode, i.e.
        they have the same `id_` and one should be the successor of the other.
        It is also tested that concatenation fails, if timesteps do not match or
        the episode to which we want to concatenate is already terminated.
        """
        # Create two episodes and fill them with 100 timesteps each.
        env = TestEnv()
        init_obs, init_info = env.reset()
        episode_1 = SingleAgentEpisode()
        episode_1.add_initial_observation(
            initial_observation=init_obs, initial_info=init_info
        )
        # Sample 100 timesteps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode_1.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                extra_model_output={"extra": np.random.random(1)},
            )

        # Create a successor.
        episode_2 = episode_1.create_successor()

        # Now, sample 100 more timesteps.
        for i in range(100, 200):
            action = i
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode_2.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                extra_model_output={"extra": np.random.random(1)},
            )

        # Assert that the second episode's `t_started` is at the first episode's
        # `t`.
        self.assertTrue(episode_1.t == episode_2.t_started)
        # Assert that the second episode's `t` is at 200.
        self.assertTrue(episode_2.t == 200)

        # Manipulate the id of the second episode and make sure an error is
        # thrown during concatenation.
        episode_2.id_ = "wrong"
        with self.assertRaises(AssertionError):
            episode_1.concat_episode(episode_2)
        # Reset the id.
        episode_2.id_ = episode_1.id_
        # Assert that when timesteps do not match an error is thrown.
        episode_2.t += 1
        with self.assertRaises(AssertionError):
            episode_1.concat_episode(episode_2)
        # Reset the timestep.
        episode_2.t -= 1
        # Assert that when the first episode is already done no concatenation can take
        # place.
        episode_1.is_terminated = True
        with self.assertRaises(AssertionError):
            episode_1.concat_episode(episode_2)
        # Reset `is_terminated`.
        episode_1.is_terminated = False

        # Concate the episodes.

        episode_1.concat_episode(episode_2)
        # Assert that the concatenated episode start at `t_started=0`
        # and has 200 sampled steps, i.e. `t=200`.
        self.assertTrue(episode_1.t_started == 0)
        self.assertTrue(episode_1.t == 200)
        # Assert that all lists have appropriate length.
        self.assertTrue(
            len(episode_1.actions)
            == len(episode_1.rewards)
            == len(episode_1.observations) - 1
            == len(episode_1.infos) - 1
            == 200
        )
        # Assert that specific observations in the two episodes match.
        self.assertEqual(episode_2.observations[5], episode_1.observations[105])
        # Assert that they are not the same object.
        # TODO (sven): Do we really need a deepcopy here?
        # self.assertNotEqual(id(episode_2.observations[5]),
        # id(episode_1.observations[105]))

    def test_get_and_from_state(self):
        """Tests, if a `SingleAgentEpisode` can be reconstructed form state.

        This test constructs an episode, stores it to its dictionary state and
        recreates a new episode form this state. Thereby it ensures that all
        atttributes are indeed identical to the primer episode and the data is
        complete.
        """
        # Create an empty episode.
        episode = SingleAgentEpisode()
        # Create an environment.
        env = TestEnv()
        # Add initial observation.
        init_obs, init_info = env.reset()
        episode.add_initial_observation(
            initial_observation=init_obs, initial_info=init_info
        )
        # Sample 100 steps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                extra_model_output={"extra": np.random.random(1)},
            )

        # Get the state and reproduce it from state.
        state = episode.get_state()
        episode_reproduced = SingleAgentEpisode.from_state(state)

        # Assert that the data is complete.
        self.assertEqual(episode.id_, episode_reproduced.id_)
        self.assertEqual(episode.t, episode_reproduced.t)
        self.assertEqual(episode.t_started, episode_reproduced.t_started)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertListEqual(episode.observations, episode_reproduced.observations)
        self.assertListEqual(episode.actions, episode_reproduced.actions)
        self.assertListEqual(episode.rewards, episode_reproduced.rewards)
        self.assertListEqual(episode.infos, episode_reproduced.infos)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertEqual(episode.states, episode_reproduced.states)
        self.assertListEqual(episode.render_images, episode_reproduced.render_images)
        self.assertDictEqual(
            episode.extra_model_outputs, episode_reproduced.extra_model_outputs
        )

        # Assert that reconstruction breaks, if the data is not complete.
        state[1][1].pop()
        with self.assertRaises(AssertionError):
            episode_reproduced = SingleAgentEpisode.from_state(state)

    def test_to_and_from_sample_batch(self):
        """Tests if a `SingelAgentEpisode` can be reconstructed from a `SampleBatch`.

        This tests converst an episode to a `SampleBatch` and reconstructs the
        episode then from this sample batch. It is then tested, if all data is
        complete.
        Note that `extra_model_outputs` are defined by the user and as the format
        in the episode from which a `SampleBatch` was created is unknown this
        reconstruction would only work, if the user does take care of it (as a
        counter example just rempve the index [0] from the `extra_model_output`).
        """
        # Create an empty episode.
        episode = SingleAgentEpisode()
        # Create an environment.
        env = TestEnv()
        # Add initial observation.
        init_obs, init_obs = env.reset()
        episode.add_initial_observation(
            initial_observation=init_obs, initial_info=init_obs
        )
        # Sample 100 steps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                extra_model_output={"extra": np.random.random(1)[0]},
            )

        # Create `SampleBatch`.
        batch = episode.to_sample_batch()
        # Reproduce form `SampleBatch`.
        episode_reproduced = SingleAgentEpisode.from_sample_batch(batch)
        # Assert that the data is complete.
        self.assertEqual(episode.id_, episode_reproduced.id_)
        self.assertEqual(episode.t, episode_reproduced.t)
        self.assertEqual(episode.t_started, episode_reproduced.t_started)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertListEqual(episode.observations, episode_reproduced.observations)
        self.assertListEqual(episode.actions, episode_reproduced.actions)
        self.assertListEqual(episode.rewards, episode_reproduced.rewards)
        self.assertEqual(episode.infos, episode_reproduced.infos)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertEqual(episode.states, episode_reproduced.states)
        self.assertListEqual(episode.render_images, episode_reproduced.render_images)
        self.assertDictEqual(
            episode.extra_model_outputs, episode_reproduced.extra_model_outputs
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
