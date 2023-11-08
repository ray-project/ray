import gymnasium as gym
import numpy as np
import unittest

from typing import List, Optional, Tuple

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.typing import MultiAgentDict

# TODO (simon): Add to the tests `info` and `extra_model_output`.


class MultiAgentTestEnv(MultiAgentEnv):
    def __init__(self):
        self.t = 0
        self._agent_ids = {"agent_" + str(i) for i in range(10)}
        self.observation_space = {
            agent_id: gym.spaces.Discrete(201) for agent_id in self._agent_ids
        }
        self.action_space = {
            agent_id: gym.spaces.Discrete(200) for agent_id in self._agent_ids
        }

        self.agents_alive = set(self._agent_ids)

    def reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[MultiAgentDict, MultiAgentDict]:
        # TODO (simon): Seed does not work. The results are always different.
        super().reset(seed=seed)
        self.t = 0
        # The number of agents that are ready at this timestep.
        num_agents_step = np.random.randint(1, len(self._agent_ids))
        # The agents that are ready.
        agents_step = np.random.choice(
            np.array(list(self._agent_ids)), num_agents_step, replace=False
        )
        # Initialize observations.
        init_obs = {agent_id: 0 for agent_id in agents_step}
        init_info = {agent_id: {} for agent_id in agents_step}

        # Reset all alive agents to all agents.
        self.agents_alive = set(self._agent_ids)

        return init_obs, init_info

    def step(
        self, action_dict: MultiAgentDict
    ) -> Tuple[
        MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict
    ]:
        self.t += 1
        # The number of agents that are ready at this timestep.
        num_agents_step = np.random.randint(1, len(self.agents_alive))
        # The agents that are ready.
        agents_step = np.random.choice(
            np.array(list(self.agents_alive)), num_agents_step, replace=False
        )
        # Initialize observations.
        obs = {agent_id: self.t for agent_id in agents_step}
        info = {agent_id: {} for agent_id in agents_step}
        reward = {agent_id: 1.0 for agent_id in agents_step}
        # Add also agents without observations.
        reward.update(
            {
                agent_id: 1.0
                for agent_id in np.random.choice(
                    np.array(list(self.agents_alive)), 8, replace=False
                )
                if agent_id not in reward
            }
        )

        # Use tha last terminateds/truncateds.
        is_truncated = {"__all__": False}
        is_truncated.update({agent_id: False for agent_id in agents_step})
        is_terminated = {"__all__": False}
        is_terminated.update({agent_id: False for agent_id in agents_step})
        if self.t == 50:
            # Let agent 1 die.
            is_terminated["agent_1"] = True
            is_truncated["agent_1"] = False
            self.agents_alive -= {"agent_1"}
            obs.update({"agent_1": self.t})
            reward.update({"agent_1": 1.0})
            info.update({"agent_1": {}})
        if self.t == 100 and "agent_5":
            # Let agent 5 die.
            is_terminated["agent_5"] = True
            is_truncated["agent_5"] = False
            self.agents_alive -= {"agent_5"}
            obs.update({"agent_5": self.t})
            reward.update({"agent_5": 1.0})
            info.update({"agent_5": {}})
        # Truncate the episode if too long.
        if self.t >= 200:
            is_truncated["__all__"] = True
            is_truncated.update({agent_id: True for agent_id in agents_step})

        return obs, reward, is_terminated, is_truncated, info

    def action_space_sample(self, agent_ids: List[str] = None) -> MultiAgentDict:
        return {
            agent_id: self.action_space[agent_id].sample() for agent_id in agent_ids
        }


class TestMultiAgentEpisode(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_init(self):
        # Create an empty episode.
        episode = MultiAgentEpisode()
        # Empty episode should have a start point and count of zero.
        self.assertTrue(episode.t_started == episode.t == 0)

        # Create an episode with a specific starting point.
        episode = MultiAgentEpisode(t_started=10)
        self.assertTrue(episode.t == episode.t_started == 10)

        # Sample 100 values and intiialiaze episode with observations.
        env = MultiAgentTestEnv()
        # Initialize containers.
        observations = []
        rewards = []
        actions = []
        infos = []
        is_terminateds = []
        is_truncateds = []
        extra_model_outputs = []
        # Initialize observation and info.
        obs, info = env.reset(seed=0)
        observations.append(obs)
        infos.append(info)
        # Run 100 samples.
        for i in range(100):
            agents_stepped = list(obs.keys())
            action = {
                agent_id: i + 1
                for agent_id in agents_stepped
                if agent_id in env.agents_alive
            }
            # action = env.action_space_sample(agents_stepped)
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            observations.append(obs)
            actions.append(action)
            rewards.append(reward)
            infos.append(info)
            is_terminateds.append(is_terminated)
            is_truncateds.append(is_truncated)
            states = {agent_id: np.random.random() for agent_id in action}
            extra_model_outputs.append(
                {
                    agent_id: {"extra_1": np.random.random()}
                    for agent_id in agents_stepped
                }
            )

        episode = MultiAgentEpisode(
            agent_ids=env.get_agent_ids(),
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            states=states,
            is_terminated=is_terminateds,
            is_truncated=is_truncateds,
            extra_model_outputs=extra_model_outputs,
        )

        # The starting point and count should now be at `len(observations) - 1`.+
        self.assertTrue(episode.t == episode.t_started == (len(observations) - 1))
        # Assert that agent 1 and agent 5 are both terminated.
        self.assertTrue(episode.agent_episodes["agent_1"].is_terminated)
        self.assertTrue(episode.agent_episodes["agent_5"].is_terminated)
        # Assert that the other agents are neither terminated nor truncated.
        for agent_id in env.get_agent_ids():
            if agent_id != "agent_1" and agent_id != "agent_5":
                self.assertFalse(episode.agent_episodes[agent_id].is_done)
        # Ensure that all global reward lists match in length the global reward
        # timestep mappings.
        for agent_id in episode._agent_ids:
            self.assertEqual(
                len(episode.global_rewards[agent_id]),
                len(episode.global_rewards_t[agent_id]),
            )

        # Test now intiializing an episode and setting the starting timestep at once.
        episode = MultiAgentEpisode(
            agent_ids=list(env.agents_alive) + ["agent_5"],
            observations=observations[-11:],
            actions=actions[-10:],
            rewards=rewards[-10:],
            infos=infos[-11:],
            t_started=100,
            states=states,
            is_terminated=is_terminateds[-10:],
            is_truncated=is_truncateds[-10:],
            extra_model_outputs=extra_model_outputs[-10:],
        )

        # Assert that the episode starts indeed at 100.
        self.assertEqual(episode.t, episode.t_started, 100)
        # Assert that the highest index in the timestep mapping is indeed 10.
        highest_index = max(
            [
                max(timesteps)
                for timesteps in episode.global_t_to_local_t.values()
                if len(timesteps) > 0
            ]
        )
        self.assertGreaterEqual(10, highest_index)
        # Assert that agent 5 is terminated.
        self.assertTrue(episode.agent_episodes["agent_5"].is_terminated)

        # Now test, if agents that have never stepped are handled correctly.
        # agent 5 will be the agent that never stepped.
        observations = [
            {"agent_1": 0, "agent_2": 0, "agent_3": 0},
            {"agent_1": 1, "agent_3": 1, "agent_4": 1},
            {"agent_2": 2, "agent_4": 2},
        ]
        actions = [
            {"agent_1": 0, "agent_2": 0, "agent_3": 0},
            {"agent_1": 1, "agent_3": 1, "agent_4": 1},
        ]
        rewards = [
            {"agent_1": 1.0, "agent_3": 1.0, "agent_4": 1.0},
            {"agent_2": 1.0, "agent_4": 1.0},
        ]
        infos = [
            {"agent_1": {}, "agent_2": {}, "agent_3": {}},
            {"agent_1": {}, "agent_3": {}, "agent_4": {}},
            {"agent_2": {}, "agent_4": {}},
        ]
        is_terminated = [
            {"__all__": False, "agent_1": False, "agent_3": False, "agent_4": False},
            {"__all__": False, "agent_2": False, "agent_4": False},
        ]
        is_truncated = [
            {"__all__": False, "agent_1": False, "agent_3": False, "agent_4": False},
            {"__all__": False, "agent_2": False, "agent_4": False},
        ]

        episode = MultiAgentEpisode(
            agent_ids=["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"],
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
        )

        # Assert that the length of `SingleAgentEpisode`s are all correct.
        self.assertEqual(len(episode.agent_episodes["agent_1"]), 1)
        self.assertEqual(len(episode.agent_episodes["agent_2"]), 1)
        self.assertEqual(len(episode.agent_episodes["agent_3"]), 1)
        self.assertEqual(len(episode.agent_episodes["agent_4"]), 1)
        # Assert now that applying length on agent 5's episode raises an error.
        with self.assertRaises(AssertionError):
            len(episode.agent_episodes["agent_5"])

    def test_add_initial_observation(self):
        # Generate an enviornment.
        env = MultiAgentTestEnv()
        # Generate an empty multi-agent episode. Note. we have to provide the
        # agent ids.
        episode = MultiAgentEpisode(agent_ids=env.get_agent_ids())

        # Generate initial observations and infos and add them to the episode.
        obs, infos = env.reset(seed=0)
        episode.add_initial_observation(
            initial_observation=obs,
            initial_info=infos,
            initial_state={agent_id: np.random.random(10) for agent_id in obs},
        )

        # Assert that timestep is at zero.
        self.assertTrue(episode.t == episode.t_started == 0)
        # Assert that the agents with initial observations have their single-agent
        # episodes in place.
        for agent_id in obs:
            self.assertGreater(len(episode.agent_episodes[agent_id].observations), 0)
            self.assertGreater(len(episode.agent_episodes[agent_id].infos), 0)
            self.assertIsNotNone(episode.agent_episodes[agent_id].states)
            self.assertEqual(episode.agent_episodes[agent_id].states.shape[0], 10)
            # Furthermore, esnure that all agents have an entry in the global timestep
            # mapping.
            self.assertEqual(len(episode.global_t_to_local_t[agent_id]), 1)
            self.assertEqual(episode.global_t_to_local_t[agent_id][0], 0)

        # TODO (simon): Test the buffers and reward storage.

    def test_add_timestep(self):
        # Create an environment and add the initial observations, infos, and states.
        env = MultiAgentTestEnv()
        episode = MultiAgentEpisode(agent_ids=env.get_agent_ids())

        obs, infos = env.reset(seed=0)
        states = {agent_id: np.random.random(10) for agent_id in obs}
        episode.add_initial_observation(
            initial_observation=obs,
            initial_info=infos,
            initial_state=states,
        )

        # Sample 100 timesteps and add them to the episode.
        for i in range(100):
            action = {
                agent_id: i + 1 for agent_id in obs if agent_id in env.agents_alive
            }
            obs, reward, is_terminated, is_truncated, info = env.step(action)

            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                state={agent_id: np.random.random(10) for agent_id in action},
                extra_model_output={
                    agent_id: {"extra": np.random.random()} for agent_id in action
                },
            )

        # Assert that the timestep is at 100.
        self.assertEqual(episode.t, 100)
        # Ensure that the episode is not done yet.
        self.assertFalse(episode.is_done)
        # Ensure that agent 1 and agent 5 are indeed done.
        self.assertTrue(episode.agent_episodes["agent_1"].is_done)
        self.assertTrue(episode.agent_episodes["agent_5"].is_done)
        # Also ensure that their buffers are all empty:
        for agent_id in ["agent_1", "agent_5"]:
            self.assertTrue(episode.agent_buffers[agent_id]["actions"].empty())
            self.assertTrue(episode.agent_buffers[agent_id]["rewards"].empty())
            self.assertTrue(episode.agent_buffers[agent_id]["states"].empty())
            self.assertTrue(
                episode.agent_buffers[agent_id]["extra_model_outputs"].empty()
            )
        # Ensure that the maximum timestep in the global timestep mapping
        # is 100.
        highest_timestep = max(
            [max(timesteps) for timesteps in episode.global_t_to_local_t.values()]
        )
        self.assertGreaterEqual(100, highest_timestep)

        # Run another 100 timesteps.
        for i in range(100, 200):
            action = {
                agent_id: i + 1 for agent_id in obs if agent_id in env.agents_alive
            }
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                state={agent_id: np.random.random(10) for agent_id in action},
                extra_model_output={
                    agent_id: {"extra": np.random.random()} for agent_id in action
                },
            )

        # Assert that the environment is done.
        self.assertTrue(is_truncated["__all__"])
        # Assert that each agent is done.
        for agent_id in episode._agent_ids:
            self.assertTrue(episode.agent_episodes[agent_id].is_done)
        # Assert that agent 1 and agent 5 have no observations/actions/etc.
        # after the timesteps in which they terminated.
        self.assertGreaterEqual(50, episode.agent_episodes["agent_1"].observations[-1])
        self.assertGreaterEqual(50, episode.agent_episodes["agent_1"].actions[-1])
        self.assertGreaterEqual(100, episode.agent_episodes["agent_5"].observations[-1])
        self.assertGreaterEqual(100, episode.agent_episodes["agent_5"].actions[-1])

        # Now test, if agents that have never stepped are handled correctly.
        # agent 5 will be the agent that never stepped.
        observations = [
            {"agent_1": 0, "agent_2": 0, "agent_3": 0},
            {"agent_1": 1, "agent_3": 1, "agent_4": 1},
            {"agent_2": 2, "agent_4": 2},
        ]
        actions = [
            {"agent_1": 0, "agent_2": 0, "agent_3": 0},
            {"agent_1": 1, "agent_3": 1, "agent_4": 1},
        ]
        rewards = [
            {"agent_1": 1.0, "agent_3": 1.0, "agent_4": 1.0},
            {"agent_2": 1.0, "agent_4": 1.0},
        ]
        infos = [
            {"agent_1": {}, "agent_2": {}, "agent_3": {}},
            {"agent_1": {}, "agent_3": {}, "agent_4": {}},
            {"agent_2": {}, "agent_4": {}},
        ]
        is_terminated = [
            {"__all__": False, "agent_1": False, "agent_3": False, "agent_4": False},
            {"__all__": False, "agent_2": False, "agent_4": False},
        ]
        is_truncated = [
            {"__all__": False, "agent_1": False, "agent_3": False, "agent_4": False},
            {"__all__": False, "agent_2": False, "agent_4": False},
        ]

        episode = MultiAgentEpisode(
            agent_ids=["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"],
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
        )
        # Now test that intermediate rewards will get recorded and actions buffered.
        action = {"agent_2": 3, "agent_4": 3}
        observation = {"agent_1": 3, "agent_2": 3}
        reward = {"agent_1": 1.0, "agent_2": 1.0, "agent_3": 1.0, "agent_5": 1.0}
        infos = {"agent_1": {}, "agent_2": {}}
        is_terminated = {k: False for k in observation.keys()}
        is_terminated.update({"__all__": False})
        is_truncated = {k: False for k in observation.keys()}
        is_truncated.update({"__all__": False})
        episode.add_timestep(
            observation=observation,
            action=action,
            reward=reward,
            info=info,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
        )
        # Assert that the action buffer for agent 4 is full.
        # Note, agent 4 acts, but receives no observation.
        # Note also, all other buffers are always full, due to their defaults.
        self.assertTrue(episode.agent_buffers["agent_4"]["actions"].full())
        # Assert that the reward buffers of agents 3 and 5 are at 1.0.
        self.assertEquals(episode.agent_buffers["agent_3"]["rewards"].get_nowait(), 1.0)
        self.assertEquals(episode.agent_buffers["agent_5"]["rewards"].get_nowait(), 1.0)
        # Now, refill the buffers.
        episode.agent_buffers["agent_3"]["rewards"].put_nowait(1.0)
        episode.agent_buffers["agent_5"]["rewards"].put_nowait(1.0)

    def test_create_successor(self):
        # Create an environment.
        env = MultiAgentTestEnv()
        # Create an empty episode.
        episode_1 = MultiAgentEpisode(agent_ids=env._agent_ids)

        # Generate initial observation and info.
        obs, info = env.reset(seed=0)
        episode_1.add_initial_observation(
            initial_observation=obs,
            initial_info=info,
            initial_state={agent_id: np.random.random() for agent_id in env._agent_ids},
        )
        # Now, generate 100 samples.
        for i in range(100):
            action = {agent_id: i for agent_id in obs}
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            episode_1.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                info=info,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
                state={agent_id: np.random.random() for agent_id in action},
                extra_model_output={
                    agent_id: {"extra": np.random.random(10)} for agent_id in action
                },
            )
        # Assert that the episode has 100 timesteps.
        self.assertEqual(episode_1.t, 100)

        # Create a successor.
        episode_2 = episode_1.create_successor()
        # Assert that it has the same id.
        self.assertEqual(episode_1.id_, episode_2.id_)
        # Assert that the timestep starts at the end of the last episode.
        self.assertTrue(episode_1.t == episode_2.t, episode_2.t_started)
        # Assert that the last observation and info of `episode_1` is the first
        # observation of `episode_2`.
        for agent_id in obs:
            self.assertEqual(
                episode_1.agent_episodes[agent_id].observations[-1],
                episode_2.agent_episodes[agent_id].observations[0],
            )
            self.assertEqual(
                episode_1.agent_episodes[agent_id].infos[-1],
                episode_2.agent_episodes[agent_id].infos[0],
            )
        # Assert that the last states in `episode_1` are identical with the first one
        # in `episode_2`.
        for agent_id in action:
            self.assertEqual(
                episode_1.agent_episodes[agent_id].states,
                episode_2.agent_episodes[agent_id].states,
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
