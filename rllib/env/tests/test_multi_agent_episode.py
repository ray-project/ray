import gymnasium as gym
import numpy as np
import unittest

from typing import List, Optional, Tuple

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.typing import MultiAgentDict


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
        for agent_id, space in self.action_space.items():
            space.seed(seed)
            self.observation_space[agent_id].seed(seed)

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
                len(episode.partial_rewards[agent_id]),
                len(episode.partial_rewards_t[agent_id]),
            )

        # Test now intiializing an episode and setting the starting timestep at once.
        episode = MultiAgentEpisode(
            agent_ids=list(env.agents_alive) + ["agent_5"],
            observations=observations[-11:],
            actions=actions[-10:],
            rewards=rewards[-10:],
            infos=infos[-11:],
            t_started=100,
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
        (
            observations,
            actions,
            rewards,
            is_terminated,
            is_truncated,
            infos,
        ) = self._generate_multi_agent_records()

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
        )

        # Assert that timestep is at zero.
        self.assertTrue(episode.t == episode.t_started == 0)
        # Assert that the agents with initial observations have their single-agent
        # episodes in place.
        for agent_id in obs:
            self.assertGreater(len(episode.agent_episodes[agent_id].observations), 0)
            self.assertGreater(len(episode.agent_episodes[agent_id].infos), 0)
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
        episode.add_initial_observation(
            initial_observation=obs,
            initial_info=infos,
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
        (
            observations,
            actions,
            rewards,
            is_terminated,
            is_truncated,
            infos,
        ) = self._generate_multi_agent_records()

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

        # Now test the buffers.
        for agent_id, agent_buffer in episode_1.agent_buffers.items():
            self.assertDictEqual(agent_buffer, episode_2.agent_buffers[agent_id])
        # Test also the reward histories

        # Now test, if the specific values in the buffers are correct.
        (
            observations,
            actions,
            rewards,
            is_terminated,
            is_truncated,
            infos,
        ) = self._generate_multi_agent_records()

        # Create the episode.
        episode_1 = MultiAgentEpisode(
            agent_ids=["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"],
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
        )

        # Assert that agents 1 and 3's buffers are indeed full.
        for agent_id in ["agent_1", "agent_3"]:
            self.assertEqual(
                actions[1][agent_id],
                episode_1.agent_buffers[agent_id]["actions"].get_nowait(),
            )
            # Put the action back into the buffer.
            episode_1.agent_buffers[agent_id]["actions"].put_nowait(
                actions[1][agent_id]
            )

        # Now step once.
        action = {"agent_2": 3, "agent_4": 3}
        # This time agent 4 should have the buffer full, while agent 1 has emptied
        # its buffer.
        observation = {"agent_1": 3, "agent_2": 3}
        # Agents 1 and 2 add the reward to its timestep, but agent 3 and agent 5
        # add this to the buffer and to the global reward history.
        reward = {"agent_1": 1.0, "agent_2": 1.0, "agent_3": 1.0, "agent_5": 1.0}
        infos = {"agent_1": {}, "agent_2": {}}
        is_terminated = {k: False for k in observation.keys()}
        is_terminated.update({"__all__": False})
        is_truncated = {k: False for k in observation.keys()}
        is_truncated.update({"__all__": False})
        episode_1.add_timestep(
            observation=observation,
            action=action,
            reward=reward,
            info=info,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
        )

        # Check that the partial reward history is correct.
        self.assertEqual(len(episode_1.partial_rewards_t["agent_3"]), 1)
        self.assertEqual(len(episode_1.partial_rewards_t["agent_5"]), 1)
        self.assertEqual(len(episode_1.partial_rewards["agent_3"]), 1)
        self.assertEqual(len(episode_1.partial_rewards["agent_5"]), 1)
        self.assertEqual(len(episode_1.partial_rewards["agent_4"]), 2)
        self.assertListEqual(episode_1.partial_rewards["agent_4"], [0.5, 1.0])
        self.assertListEqual(episode_1.partial_rewards_t["agent_4"], [1, 2])

        # Now check that the reward buffers are full.
        for agent_id in ["agent_3", "agent_5"]:
            self.assertEqual(
                episode_1.agent_buffers[agent_id]["rewards"].get_nowait(), 1.0
            )
            episode_1.agent_buffers[agent_id]["rewards"].put_nowait(reward[agent_id])
            # Check that the reward history is correctly recorded.
            self.assertEqual(episode_1.partial_rewards_t[agent_id][-1], episode_1.t)
            self.assertEqual(episode_1.partial_rewards[agent_id][-1], 1.0)

        # Now create the successor.
        episode_2 = episode_1.create_successor()

        for agent_id, agent_eps in episode_2.agent_episodes.items():
            if len(agent_eps.observations) > 0:
                # The successor's first observations should be the predecessor's last.
                self.assertEqual(
                    agent_eps.observations[0],
                    episode_1.agent_episodes[agent_id].observations[-1],
                )
                # The successor's global timestep mapping should contain exactly these
                # obhservations' timestep.
                self.assertEqual(episode_2.global_t_to_local_t[agent_id][-1], 0)
                self.assertEqual(
                    episode_2.global_t_to_local_t[agent_id][-1] + episode_2.t_started,
                    episode_1.global_t_to_local_t[agent_id][-1],
                )
        # Now test that the partial rewards fit.
        for agent_id in ["agent_3", "agent_5"]:
            self.assertEqual(len(episode_2.partial_rewards_t[agent_id]), 1)
            self.assertEqual(episode_2.partial_rewards_t[agent_id][-1], 3)
            self.assertEqual(
                episode_2.agent_buffers[agent_id]["rewards"].get_nowait(), 1.0
            )
            # Put the values back into the buffer.
            episode_2.agent_buffers[agent_id]["rewards"].put_nowait(1.0)

        # Assert that agent 3's and 4's action buffers are full.
        self.assertTrue(episode_2.agent_buffers["agent_4"]["actions"].full())
        self.assertTrue(episode_2.agent_buffers["agent_3"]["actions"].full())
        self.assertTrue(episode_2.agent_buffers["agent_1"]["actions"].empty())

    def test_getters(self):
        # Generate simple records for a multi agent environment.
        (
            observations,
            actions,
            rewards,
            is_terminateds,
            is_truncateds,
            infos,
        ) = self._generate_multi_agent_records()
        # Define the agent ids.
        agent_ids = ["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"]

        # Define some extra model outputs.
        extra_model_outputs = [
            # Here agent_2 has to buffer.
            {"agent_1": {"extra": 0}, "agent_2": {"extra": 0}, "agent_3": {"extra": 0}},
            {"agent_1": {"extra": 1}, "agent_3": {"extra": 1}, "agent_4": {"extra": 1}},
        ]

        # Create a multi-agent episode.
        episode = MultiAgentEpisode(
            agent_ids=agent_ids,
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            is_terminated=is_terminateds,
            is_truncated=is_truncateds,
            extra_model_outputs=extra_model_outputs,
        )

        # --- observations ---

        # Now get the last observations.
        last_observation = episode.get_observations()
        # Assert that this observation is correct.
        for agent_id, agent_obs in last_observation.items():
            self.assertEqual(agent_obs[0], observations[-1][agent_id])

        last_observations = episode.get_observations(indices=[-1, -2])
        # Assert that the observations are correct.
        self.assertEqual(last_observations["agent_1"][0], observations[-2]["agent_1"])
        self.assertEqual(last_observations["agent_2"][0], observations[-1]["agent_2"])
        self.assertEqual(last_observations["agent_3"][0], observations[-2]["agent_3"])
        # Note, agent 4 has two observations in the last two ones.
        # Note, `get_observations()` returns in the order of the `indices` parameter.
        self.assertEqual(last_observations["agent_4"][1], observations[-2]["agent_4"])
        self.assertEqual(last_observations["agent_4"][0], observations[-1]["agent_4"])

        # Now, test the same when returning a list.
        last_observation = episode.get_observations(as_list=True)
        # Assert that these observations are correct.
        for agent_id, agent_obs in last_observation[0].items():
            self.assertEqual(agent_obs, observations[-1][agent_id])

        last_observations = episode.get_observations(indices=[-1, -2], as_list=True)
        # Assert that the observations are correct.
        self.assertEqual(last_observations[1]["agent_1"], observations[-2]["agent_1"])
        self.assertEqual(last_observations[0]["agent_2"], observations[-1]["agent_2"])
        self.assertEqual(last_observations[1]["agent_3"], observations[-2]["agent_3"])
        # Note, agent 4 has two observations in the last two ones.
        # Note, `get_observations()` returns in the order of the `indices` parameter.
        self.assertEqual(last_observations[1]["agent_4"], observations[-2]["agent_4"])
        self.assertEqual(last_observations[0]["agent_4"], observations[-1]["agent_4"])

        # Now, test if for the local setting the results fit as well.
        last_local_observation = episode.get_observations(global_ts=False)
        # Assert that the observations are correct.
        self.assertEqual(
            last_local_observation["agent_1"][0], observations[-2]["agent_1"]
        )
        self.assertEqual(
            last_local_observation["agent_2"][0], observations[-1]["agent_2"]
        )
        self.assertEqual(
            last_local_observation["agent_3"][0], observations[-2]["agent_3"]
        )
        self.assertEqual(
            last_local_observation["agent_4"][0], observations[-1]["agent_4"]
        )

        # Now return the last two observations per agent.
        last_local_observations = episode.get_observations(
            indices=[-1, -2], global_ts=False
        )
        # Assert that the observations are correct.
        self.assertEqual(
            last_local_observations["agent_1"][0], observations[-2]["agent_1"]
        )
        self.assertEqual(
            last_local_observations["agent_1"][1], observations[-3]["agent_1"]
        )
        self.assertEqual(
            last_local_observations["agent_2"][0], observations[-1]["agent_2"]
        )
        self.assertEqual(
            last_local_observations["agent_2"][1], observations[-3]["agent_2"]
        )
        self.assertEqual(
            last_local_observations["agent_3"][0], observations[-2]["agent_3"]
        )
        self.assertEqual(
            last_local_observations["agent_3"][1], observations[-3]["agent_3"]
        )
        self.assertEqual(
            last_local_observations["agent_4"][0], observations[-1]["agent_4"]
        )
        self.assertEqual(
            last_local_observations["agent_4"][1], observations[-2]["agent_4"]
        )

        # Test with initial observations only.
        episode_init_only = MultiAgentEpisode(agent_ids=agent_ids)
        episode_init_only.add_initial_observation(
            initial_observation=observations[0],
            initial_info=infos[0],
        )
        # Get the last observation for agents and assert that its correct.
        last_observation = episode_init_only.get_observations()
        for agent_id, agent_obs in observations[0].items():
            self.assertEqual(last_observation[agent_id][0], agent_obs)
        # Now the same as list.
        last_observation = episode_init_only.get_observations(as_list=True)
        for agent_id, agent_obs in observations[0].items():
            self.assertEqual(last_observation[0][agent_id], agent_obs)
        # Now locally.
        last_local_observation = episode_init_only.get_observations(global_ts=False)
        for agent_id, agent_obs in observations[0].items():
            self.assertEqual(last_local_observation[agent_id][0], agent_obs)

        # --- actions ---
        last_actions = episode.get_actions()
        self.assertEqual(last_actions["agent_1"][0], actions[-1]["agent_1"])
        self.assertEqual(last_actions["agent_3"][0], actions[-1]["agent_3"])
        self.assertEqual(last_actions["agent_4"][0], actions[-1]["agent_4"])

        last_actions = episode.get_actions(indices=[-1, -2])
        self.assertEqual(last_actions["agent_1"][0], actions[-1]["agent_1"])
        self.assertEqual(last_actions["agent_3"][0], actions[-1]["agent_3"])
        self.assertEqual(last_actions["agent_4"][0], actions[-1]["agent_4"])
        self.assertEqual(last_actions["agent_1"][1], actions[-2]["agent_1"])
        self.assertEqual(last_actions["agent_2"][0], actions[-2]["agent_2"])
        self.assertEqual(last_actions["agent_3"][1], actions[-2]["agent_3"])

        # Now request lists.
        last_actions = episode.get_actions(as_list=True)
        self.assertEqual(last_actions[0]["agent_1"], actions[-1]["agent_1"])
        self.assertEqual(last_actions[0]["agent_3"], actions[-1]["agent_3"])
        self.assertEqual(last_actions[0]["agent_4"], actions[-1]["agent_4"])

        # Request the last two actions and return as a list.
        last_actions = episode.get_actions([-1, -2], as_list=True)
        self.assertEqual(last_actions[0]["agent_1"], actions[-1]["agent_1"])
        self.assertEqual(last_actions[0]["agent_3"], actions[-1]["agent_3"])
        self.assertEqual(last_actions[0]["agent_4"], actions[-1]["agent_4"])
        self.assertEqual(last_actions[1]["agent_1"], actions[-2]["agent_1"])
        self.assertEqual(last_actions[1]["agent_2"], actions[-2]["agent_2"])
        self.assertEqual(last_actions[1]["agent_3"], actions[-2]["agent_3"])

        # Now request the last actions at the local timesteps, i.e. for each agent
        # its last two actions.
        last_actions = episode.get_actions([-1, -2], global_ts=False)
        self.assertEqual(last_actions["agent_1"][0], actions[-1]["agent_1"])
        self.assertEqual(last_actions["agent_3"][0], actions[-1]["agent_3"])
        self.assertEqual(last_actions["agent_4"][0], actions[-1]["agent_4"])
        self.assertEqual(last_actions["agent_1"][1], actions[-2]["agent_1"])
        self.assertEqual(last_actions["agent_2"][0], actions[-2]["agent_2"])
        self.assertEqual(last_actions["agent_3"][1], actions[-2]["agent_3"])

        # --- extra_model_outputs ---
        last_extra_model_outputs = episode.get_extra_model_outputs()
        self.assertDictEqual(
            last_extra_model_outputs["agent_1"][0], extra_model_outputs[-1]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_3"][0], extra_model_outputs[-1]["agent_3"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_4"][0], extra_model_outputs[-1]["agent_4"]
        )

        # Request the last two outputs.
        last_extra_model_outputs = episode.get_extra_model_outputs(indices=[-1, -2])
        self.assertDictEqual(
            last_extra_model_outputs["agent_1"][0], extra_model_outputs[-1]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_3"][0], extra_model_outputs[-1]["agent_3"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_4"][0], extra_model_outputs[-1]["agent_4"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_1"][1], extra_model_outputs[-2]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_2"][0], extra_model_outputs[-2]["agent_2"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_3"][1], extra_model_outputs[-2]["agent_3"]
        )

        # Now request lists.
        last_extra_model_outputs = episode.get_extra_model_outputs(as_list=True)
        self.assertDictEqual(
            last_extra_model_outputs[0]["agent_1"], extra_model_outputs[-1]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs[0]["agent_3"], extra_model_outputs[-1]["agent_3"]
        )
        self.assertDictEqual(
            last_extra_model_outputs[0]["agent_4"], extra_model_outputs[-1]["agent_4"]
        )
        # Request the last two extra model outputs and return as a list.
        last_extra_model_outputs = episode.get_extra_model_outputs(
            [-1, -2], as_list=True
        )
        self.assertDictEqual(
            last_extra_model_outputs[0]["agent_1"], extra_model_outputs[-1]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs[0]["agent_3"], extra_model_outputs[-1]["agent_3"]
        )
        self.assertDictEqual(
            last_extra_model_outputs[0]["agent_4"], extra_model_outputs[-1]["agent_4"]
        )
        self.assertDictEqual(
            last_extra_model_outputs[1]["agent_1"], extra_model_outputs[-2]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs[1]["agent_2"], extra_model_outputs[-2]["agent_2"]
        )
        self.assertDictEqual(
            last_extra_model_outputs[1]["agent_3"], extra_model_outputs[-2]["agent_3"]
        )

        # Now request the last extra model outputs at the local timesteps, i.e.
        # for each agent its last two actions.
        last_extra_model_outputs = episode.get_extra_model_outputs(
            [-1, -2], global_ts=False
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_1"][0], extra_model_outputs[-1]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_3"][0], extra_model_outputs[-1]["agent_3"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_4"][0], extra_model_outputs[-1]["agent_4"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_1"][1], extra_model_outputs[-2]["agent_1"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_2"][0], extra_model_outputs[-2]["agent_2"]
        )
        self.assertDictEqual(
            last_extra_model_outputs["agent_3"][1], extra_model_outputs[-2]["agent_3"]
        )

        # --- rewards ---
        # Start with the case of no partial or buffered rewards.
        last_rewards = episode.get_rewards(partial=False, consider_buffer=False)
        self.assertTrue(
            last_rewards["agent_4"][0], rewards[0]["agent_4"] + rewards[1]["agent_4"]
        )
        self.assertTrue(last_rewards["agent_2"][0], rewards[1]["agent_2"])

        # Now test the same case, but with the last two rewards.
        last_rewards = episode.get_rewards(
            [-1, -2], partial=False, consider_buffer=False
        )
        self.assertTrue(
            last_rewards["agent_4"][0], rewards[0]["agent_4"] + rewards[1]["agent_4"]
        )
        self.assertTrue(last_rewards["agent_2"][0], rewards[1]["agent_2"])
        self.assertTrue(last_rewards["agent_1"][0], rewards[0]["agent_1"])
        self.assertTrue(last_rewards["agent_3"][0], rewards[0]["agent_3"])

        # Now request these rewards as list.
        last_rewards = episode.get_rewards(
            as_list=True, partial=False, consider_buffer=False
        )
        self.assertTrue(
            last_rewards[0]["agent_4"], rewards[0]["agent_4"] + rewards[1]["agent_4"]
        )
        self.assertTrue(last_rewards[0]["agent_2"], rewards[1]["agent_2"])

        # Now test the same case, but with the last two rewards.
        last_rewards = episode.get_rewards(
            [-1, -2], as_list=True, partial=False, consider_buffer=False
        )
        self.assertTrue(
            last_rewards[0]["agent_4"], rewards[0]["agent_4"] + rewards[1]["agent_4"]
        )
        self.assertTrue(last_rewards[0]["agent_2"], rewards[1]["agent_2"])
        self.assertTrue(last_rewards[1]["agent_1"], rewards[0]["agent_1"])
        self.assertTrue(last_rewards[1]["agent_3"], rewards[0]["agent_3"])

        # Now receive the rewards and consider the buffers of agents.
        # last_rewards = episode.get_rewards(partial=False, consider_buffer=True)
        # self.assertTrue(last_rewards["agent_4"][0], rewards[0]["agent_4"] +
        # rewards[1]["agent_4"])
        # self.assertTrue(last_rewards["agent_2"][0], rewards[1]["agent_2"])

        # Create an environment.
        env = MultiAgentTestEnv()
        # Create an empty episode.
        episode_1 = MultiAgentEpisode(agent_ids=env._agent_ids)

        # Generate initial observation and info.
        obs, info = env.reset(seed=0)
        episode_1.add_initial_observation(
            initial_observation=obs,
            initial_info=info,
        )
        # Now, generate 100 samples.
        # np.random.seed(0)
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
                extra_model_output={agent_id: {"extra": 10} for agent_id in action},
            )

        last_rewards = episode_1.get_rewards(partial=False, consider_buffer=True)
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=False, consider_buffer=True
        )
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=False, consider_buffer=True, as_list=True
        )
        self.assertEqual(0, 0)
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=True, consider_buffer=False, as_list=False
        )
        self.assertEqual(0, 0)

    def _generate_multi_agent_records(self):
        # Now test, if the specific values in the buffers are correct.
        observations = [
            {"agent_1": 0, "agent_2": 0, "agent_3": 0},
            # Here agent 2 is stepping, but does not receive a next
            # observation.
            {"agent_1": 1, "agent_3": 1, "agent_4": 1},
            # Here agents 1 and 3 have stepped, but received no next
            # observation. their actions should go into the buffers.
            {"agent_2": 2, "agent_4": 2},
        ]
        actions = [
            # Here agent_2 has to buffer.
            {"agent_1": 0, "agent_2": 0, "agent_3": 0},
            {"agent_1": 1, "agent_3": 1, "agent_4": 1},
        ]
        rewards = [
            # Here agent 4 has to buffer the reward as does not have
            # actions nor observation.
            {"agent_1": 0.5, "agent_3": 0.5, "agent_4": 0.5},
            # Agent 4 should now release the buffer with reward 1.0
            # and add the next reward to it, as it stepped and received
            # a next observation.
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

        return observations, actions, rewards, is_terminated, is_truncated, infos


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
