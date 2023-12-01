import gymnasium as gym
import numpy as np
import unittest

from typing import List, Optional, Tuple

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.typing import MultiAgentDict


class MultiAgentTestEnv(MultiAgentEnv):
    def __init__(self, truncate=True):
        self.t = 0
        self._agent_ids = {"agent_" + str(i) for i in range(10)}
        self.observation_space = {
            agent_id: gym.spaces.Discrete(201) for agent_id in self._agent_ids
        }
        self.action_space = {
            agent_id: gym.spaces.Discrete(200) for agent_id in self._agent_ids
        }

        self._agents_alive = set(self._agent_ids)
        self.truncate = truncate

    def reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[MultiAgentDict, MultiAgentDict]:
        # Call the super's reset function.
        super().reset(seed=seed, options=options)

        # Set the timestep back to zero.
        self.t = 0
        # The number of agents that are ready at this timestep.
        # Note, if we want to use an RNG, we need to use the one from the
        # `gym.Env` otherwise results are not reproducable. This RNG is
        # stored to `self._np_random`.
        num_agents_step = self._np_random.integers(1, len(self._agent_ids))
        # The agents that are ready.
        agents_step = self._np_random.choice(
            np.array(sorted(self._agent_ids)), num_agents_step, replace=False
        )
        # Initialize observations.
        init_obs = {agent_id: 0 for agent_id in agents_step}
        init_info = {agent_id: {} for agent_id in agents_step}

        # Reset all alive agents to all agents.
        self._agents_alive = set(self._agent_ids)

        return init_obs, init_info

    def step(
        self, action_dict: MultiAgentDict
    ) -> Tuple[
        MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict
    ]:
        # Increase the timestep by one.
        self.t += 1
        # The number of agents that are ready at this timestep.
        num_agents_step = self._np_random.integers(1, len(self._agents_alive))
        # The agents that are ready.
        agents_step = self._np_random.choice(
            np.array(sorted(self._agents_alive)), num_agents_step, replace=False
        )
        # Initialize observations.
        obs = {agent_id: self.t for agent_id in agents_step}
        info = {agent_id: {} for agent_id in agents_step}
        reward = {agent_id: 1.0 for agent_id in agents_step}
        # Add also agents without observations.
        reward.update(
            {
                agent_id: 1.0
                for agent_id in self._np_random.choice(
                    np.array(sorted(self._agents_alive)), 8, replace=False
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
            # Ensure that the set of alive agents is updated.
            self._agents_alive -= {"agent_1"}
            # Any terminated agent, terminates with an observation.
            obs.update({"agent_1": self.t})
            reward.update({"agent_1": 1.0})
            info.update({"agent_1": {}})

        if self.t == 100 and "agent_5":
            # Let agent 5 die.
            is_terminated["agent_5"] = True
            is_truncated["agent_5"] = False
            # Ensure that the set of alive agents is updated.
            self._agents_alive -= {"agent_5"}
            # Any terminated agent, terminates with an observation.
            obs.update({"agent_5": self.t})
            reward.update({"agent_5": 1.0})
            info.update({"agent_5": {}})

        # Truncate the episode if too long.
        if self.t >= 200 and self.truncate:
            is_truncated["__all__"] = True
            is_truncated.update({agent_id: True for agent_id in agents_step})

        return obs, reward, is_terminated, is_truncated, info

    def action_space_sample(self, agent_ids: List[str] = None) -> MultiAgentDict:
        # Actually not used at this stage.
        return {
            agent_id: self.action_space[agent_id].sample() for agent_id in agent_ids
        }


# TODO (simon): Test `get_state()` and `from_state()`.
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

        # Sample 100 values and initialize the episode with observations.
        env = MultiAgentTestEnv()
        # Initialize containers.
        observations = []
        rewards = []
        actions = []
        infos = []
        terminateds = {}
        truncateds = {}
        extra_model_outputs = []
        # Initialize observation and info.
        obs, info = env.reset(seed=0)
        observations.append(obs)
        infos.append(info)
        # Run 100 samples.
        for i in range(100):
            agents_to_step_next = [
                aid for aid in obs.keys() if aid in env._agents_alive
            ]
            action = {agent_id: i + 1 for agent_id in agents_to_step_next}
            # action = env.action_space_sample(agents_stepped)
            obs, reward, terminated, truncated, info = env.step(action)
            observations.append(obs)
            actions.append(action)
            rewards.append(reward)
            infos.append(info)
            terminateds.update(terminated)
            truncateds.update(truncated)
            extra_model_outputs.append(
                {agent_id: {"extra_1": 10.5} for agent_id in agents_to_step_next}
            )

        # Now create the episode from the recorded data.
        episode = MultiAgentEpisode(
            agent_ids=list(env.get_agent_ids()),
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=terminateds,
            truncateds=truncateds,
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
            agent_ids=list(env._agents_alive) + ["agent_5"],
            observations=observations[-11:],
            actions=actions[-10:],
            rewards=rewards[-10:],
            infos=infos[-11:],
            t_started=100,
            terminateds=terminateds,
            truncateds=truncateds,
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
        self.assertGreaterEqual(100, highest_index)
        # Assert that agent 5 is terminated.
        self.assertTrue(episode.agent_episodes["agent_5"].is_terminated)

        # Now test, if agents that have never stepped are handled correctly.
        # agent 5 will be the agent that never stepped.
        (
            observations,
            actions,
            rewards,
            terminateds,
            truncateds,
            infos,
        ) = self._mock_multi_agent_records()

        # Create the episode from the mock data.
        episode = MultiAgentEpisode(
            agent_ids=["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"],
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=terminateds,
            truncateds=truncateds,
        )

        # Assert that the length of `SingleAgentEpisode`s are all correct.
        self.assertEqual(len(episode.agent_episodes["agent_1"]), 1)
        self.assertEqual(len(episode.agent_episodes["agent_2"]), 1)
        self.assertEqual(len(episode.agent_episodes["agent_3"]), 1)
        self.assertEqual(len(episode.agent_episodes["agent_4"]), 1)
        # Assert now that applying length on agent 5's episode raises an error.
        with self.assertRaises(AssertionError):
            len(episode.agent_episodes["agent_5"])

        # TODO (simon): Also test the other structs inside the MAE for agent 5 and
        # the other agents.

    def test_add_initial_observation(self):
        # Generate an enviornment.
        env = MultiAgentTestEnv()
        # Generate an empty multi-agent episode. Note. we have to provide the
        # agent ids.
        episode = MultiAgentEpisode(agent_ids=env.get_agent_ids())

        # Generate initial observations and infos and add them to the episode.
        obs, infos = env.reset(seed=0)
        episode.add_env_reset(
            observations=obs,
            infos=infos,
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

    def test_add_env_step(self):
        # Create an environment and add the initial observations, infos, and states.
        env = MultiAgentTestEnv()
        episode = MultiAgentEpisode(agent_ids=env.get_agent_ids())

        obs, infos = env.reset(seed=0)
        episode.add_env_reset(
            observations=obs,
            infos=infos,
        )

        # Sample 100 timesteps and add them to the episode.
        for i in range(100):
            action = {
                agent_id: i + 1 for agent_id in obs if agent_id in env._agents_alive
            }
            obs, reward, terminated, truncated, info = env.step(action)

            episode.add_env_step(
                observations=obs,
                actions=action,
                rewards=reward,
                infos=info,
                terminateds=terminated,
                truncateds=truncated,
                extra_model_outputs={agent_id: {"extra": 10.5} for agent_id in action},
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
                agent_id: i + 1 for agent_id in obs if agent_id in env._agents_alive
            }
            obs, reward, terminated, truncated, info = env.step(action)
            episode.add_env_step(
                observations=obs,
                actions=action,
                rewards=reward,
                infos=info,
                terminateds=terminated,
                truncateds=truncated,
                extra_model_outputs={agent_id: {"extra": 10.5} for agent_id in action},
            )

        # Assert that the environment is done.
        self.assertTrue(truncated["__all__"])
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
            terminated,
            truncated,
            infos,
        ) = self._mock_multi_agent_records()

        episode = MultiAgentEpisode(
            agent_ids=["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"],
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=terminated,
            truncateds=truncated,
        )
        # Now test that intermediate rewards will get recorded and actions buffered.
        action = {"agent_2": 3, "agent_4": 3}
        observation = {"agent_1": 3, "agent_2": 3}
        reward = {"agent_1": 1.0, "agent_2": 1.0, "agent_3": 1.0, "agent_5": 1.0}
        infos = {"agent_1": {}, "agent_2": {}}
        terminated = {k: False for k in observation.keys()}
        terminated.update({"__all__": False})
        truncated = {k: False for k in observation.keys()}
        truncated.update({"__all__": False})
        episode.add_env_step(
            observations=observation,
            actions=action,
            rewards=reward,
            infos=infos,
            terminateds=terminated,
            truncateds=truncated,
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

    def test_cut(self):
        # Create an environment.
        episode_1, _ = self._mock_multi_agent_records_from_env(size=100)

        # Assert that the episode has 100 timesteps.
        self.assertEqual(episode_1.t, 100)

        # Create a successor.
        episode_2 = episode_1.cut()
        # Assert that it has the same id.
        self.assertEqual(episode_1.id_, episode_2.id_)
        # Assert that all `SingleAgentEpisode`s have identical ids.
        for agent_id, agent_eps in episode_1.agent_episodes.items():
            self.assertEqual(agent_eps.id_, episode_2.agent_episodes[agent_id].id_)
        # Assert that the timestep starts at the end of the last episode.
        self.assertEqual(episode_1.t, episode_2.t_started)
        # Assert that the last observation and info of `episode_1` are the first
        # observation and info of `episode_2`.
        for agent_id, agent_eps in episode_1.agent_episodes.items():
            # If agents are done only ensure that the `SingleAgentEpisode` is empty.
            if agent_eps.is_done:
                self.assertEqual(
                    len(episode_2.agent_episodes[agent_id].observations), 0
                )
            else:
                self.assertEqual(
                    agent_eps.observations[-1],
                    episode_2.agent_episodes[agent_id].observations[0],
                )
                self.assertEqual(
                    agent_eps.infos[-1],
                    episode_2.agent_episodes[agent_id].infos[0],
                )

        # Now test the buffers.
        for agent_id, agent_buffer in episode_1.agent_buffers.items():
            # Make sure the action buffers are either both full or both empty.
            self.assertEqual(
                agent_buffer["actions"].full(),
                episode_2.agent_buffers[agent_id]["actions"].full(),
            )
            # If the action buffers are full they should share the same value.
            if agent_buffer["actions"].full():
                self.assertEqual(
                    agent_buffer["actions"].queue[0],
                    episode_2.agent_buffers[agent_id]["actions"].queue[0],
                )
            # If the agent is not done, the buffers should be equal in value.
            if not episode_1.agent_episodes[agent_id].is_done:
                # The other buffers have default values, if the agent is not done.
                # Note, reward buffers could be full of partial rewards.
                self.assertEqual(
                    agent_buffer["rewards"].queue[0],
                    episode_2.agent_buffers[agent_id]["rewards"].queue[0],
                )
                # Here we want to know, if they are both different from `None`.
                self.assertEqual(
                    agent_buffer["extra_model_outputs"].queue[0],
                    episode_2.agent_buffers[agent_id]["extra_model_outputs"].queue[0],
                )
            # If an agent is done the buffers should be empty for both, predecessor
            # and successor.
            else:
                self.assertTrue(agent_buffer["actions"].empty())
                self.assertTrue(agent_buffer["rewards"].empty())
                self.assertTrue(agent_buffer["extra_model_outputs"].empty())
                self.assertTrue(agent_buffer["actions"].empty())
                self.assertTrue(agent_buffer["rewards"].empty())
                self.assertTrue(agent_buffer["extra_model_outputs"].empty())

        # Ensure that the timestep mappings match.
        for agent_id, agent_global_ts in episode_2.global_t_to_local_t.items():
            # If an agent is not done, we write over the timestep from its last
            # observation.
            if not episode_2.agent_episodes[agent_id].is_done:
                self.assertEqual(
                    agent_global_ts[0], episode_1.global_t_to_local_t[agent_id][-1]
                )
            # In the other case this mapping should be empty.
            else:
                self.assertEqual(len(agent_global_ts), 0)

        # Assert that the global action timestep mappings match.
        for agent_id, agent_global_ts in episode_2.global_actions_t.items():
            # If an agent is not done, we write over the timestep from its last
            # action.
            if not episode_2.agent_episodes[agent_id].is_done:
                # If a timestep mapping for actions was copied over the last timestep
                # of the üredecessor and the first of the successor must match.
                if agent_global_ts:
                    self.assertEqual(
                        agent_global_ts[0], episode_1.global_actions_t[agent_id][-1]
                    )
                # If no action timestep mapping was copied over the last action must
                # have been at or before the last observation in the predecessor.
                else:
                    self.assertGreaterEqual(
                        episode_1.global_t_to_local_t[agent_id][-1],
                        episode_1.global_actions_t[agent_id][-1],
                    )
            # In the other case this mapping should be empty.
            else:
                self.assertEqual(len(agent_global_ts), 0)

        # Assert that the partial reward mappings and histories match.
        for agent_id, agent_global_ts in episode_2.partial_rewards_t.items():
            # Ensure that timestep mapping and history have the same length.
            self.assertEqual(
                len(agent_global_ts), len(episode_2.partial_rewards[agent_id])
            )
            # If an agent is not done, we write over the timestep from its last
            # partial rewards.
            if not episode_2.agent_episodes[agent_id].is_done:
                # If there are partial rewards after the last observation ensure
                # they are correct.
                if (
                    episode_1.global_t_to_local_t[agent_id][-1]
                    < episode_1.partial_rewards_t[agent_id][-1]
                ):
                    indices_after_last_obs = episode_1.partial_rewards_t[
                        agent_id
                    ].find_indices_right(episode_1.global_t_to_local_t[agent_id][-1])
                    episode_1_partial_rewards = list(
                        map(
                            episode_1.partial_rewards[agent_id].__getitem__,
                            indices_after_last_obs,
                        )
                    )
                    self.assertEqual(
                        sum(episode_2.partial_rewards[agent_id]),
                        sum(episode_1_partial_rewards),
                    )
                    # Also ensure that the timestep mappings are correct.
                    episode_1_partial_rewards_t = list(
                        map(
                            episode_1.partial_rewards_t[agent_id].__getitem__,
                            indices_after_last_obs,
                        )
                    )
                    self.assertListEqual(
                        episode_2.partial_rewards_t[agent_id],
                        episode_1_partial_rewards_t,
                    )
                # In the other case this mapping should be empty.
                else:
                    self.assertEqual(len(agent_global_ts), 0)
            # In the other case this mapping should be empty.
            else:
                self.assertEqual(len(agent_global_ts), 0)

        # Now test, if the specific values in the buffers are correct.
        (
            observations,
            actions,
            rewards,
            terminateds,
            truncateds,
            infos,
        ) = self._mock_multi_agent_records()

        # Create the episode.
        episode_1 = MultiAgentEpisode(
            agent_ids=["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"],
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=terminateds,
            truncateds=truncateds,
        )

        # Assert that agents 1 and 3's buffers are indeed full.
        for agent_id in ["agent_1", "agent_3"]:
            self.assertEqual(
                actions[1][agent_id],
                episode_1.agent_buffers[agent_id]["actions"].queue[0],
            )
            # # Put the action back into the buffer.
            # episode_1.agent_buffers[agent_id]["actions"].put_nowait(
            #     actions[1][agent_id]
            # )

        # Now step once.
        action = {"agent_2": 3, "agent_4": 3}
        # This time agent 4 should have the buffer full, while agent 1 has emptied
        # its buffer.
        observation = {"agent_1": 3, "agent_2": 3}
        # Agents 1 and 2 add the reward to its timestep, but agent 3 and agent 5
        # add this to the buffer and to the global reward history.
        reward = {"agent_1": 2.0, "agent_2": 2.0, "agent_3": 2.0, "agent_5": 2.0}
        info = {"agent_1": {}, "agent_2": {}}
        terminateds = {k: False for k in observation.keys()}
        terminateds.update({"__all__": False})
        truncateds = {k: False for k in observation.keys()}
        truncateds.update({"__all__": False})
        episode_1.add_env_step(
            observations=observation,
            actions=action,
            rewards=reward,
            infos=info,
            terminateds=terminateds,
            truncateds=truncateds,
        )

        # Check that the partial reward history is correct.
        self.assertEqual(len(episode_1.partial_rewards_t["agent_5"]), 1)
        self.assertEqual(len(episode_1.partial_rewards["agent_5"]), 1)
        self.assertEqual(len(episode_1.partial_rewards_t["agent_3"]), 2)
        self.assertEqual(len(episode_1.partial_rewards["agent_3"]), 2)
        self.assertEqual(len(episode_1.partial_rewards_t["agent_2"]), 2)
        self.assertEqual(len(episode_1.partial_rewards_t["agent_2"]), 2)
        self.assertListEqual(episode_1.partial_rewards["agent_3"], [0.5, 2.0])
        self.assertListEqual(episode_1.partial_rewards_t["agent_3"], [1, 3])
        self.assertListEqual(episode_1.partial_rewards["agent_2"], [1.0, 2.0])
        self.assertListEqual(episode_1.partial_rewards_t["agent_2"], [2, 3])
        self.assertEqual(len(episode_1.partial_rewards["agent_4"]), 2)
        self.assertListEqual(episode_1.partial_rewards["agent_4"], [0.5, 1.0])
        self.assertListEqual(episode_1.partial_rewards_t["agent_4"], [1, 2])

        # Now check that the reward buffers are full.
        for agent_id in ["agent_3", "agent_5"]:
            self.assertEqual(episode_1.agent_buffers[agent_id]["rewards"].queue[0], 2.0)
            # Check that the reward history is correctly recorded.
            self.assertEqual(episode_1.partial_rewards_t[agent_id][-1], episode_1.t)
            self.assertEqual(episode_1.partial_rewards[agent_id][-1], 2.0)

        # Now create the successor.
        episode_2 = episode_1.cut()

        for agent_id, agent_eps in episode_2.agent_episodes.items():
            if len(agent_eps.observations) > 0:
                # The successor's first observations should be the predecessor's last.
                self.assertEqual(
                    agent_eps.observations[0],
                    episode_1.agent_episodes[agent_id].observations[-1],
                )
                # The successor's first entry in the timestep mapping should be the
                # predecessor's last.
                self.assertEqual(
                    episode_2.global_t_to_local_t[agent_id][
                        -1
                    ],  # + episode_2.t_started,
                    episode_1.global_t_to_local_t[agent_id][-1],
                )
        # Now test that the partial rewards fit.
        for agent_id in ["agent_3", "agent_5"]:
            self.assertEqual(len(episode_2.partial_rewards_t[agent_id]), 1)
            self.assertEqual(episode_2.partial_rewards_t[agent_id][-1], 3)
            self.assertEqual(episode_2.agent_buffers[agent_id]["rewards"].queue[0], 2.0)

        # Assert that agent 3's and 4's action buffers are full.
        self.assertTrue(episode_2.agent_buffers["agent_4"]["actions"].full())
        self.assertTrue(episode_2.agent_buffers["agent_3"]["actions"].full())
        # Also assert that agent 1's action b uffer was emptied with the last
        # observations.
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
        ) = self._mock_multi_agent_records()
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
            terminateds=is_terminateds,
            truncateds=is_truncateds,
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
        episode_init_only.add_env_reset(
            observation=observations[0],
            infos=infos[0],
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

        # TODO (simon): Not tested with `global_ts=False`.
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

        # Create an environment.
        env = MultiAgentTestEnv()
        # Create an empty episode.
        episode_1 = MultiAgentEpisode(agent_ids=env._agent_ids)

        # Generate initial observation and info.
        obs, info = env.reset(seed=42)
        episode_1.add_env_reset(
            observations=obs,
            infos=info,
        )
        # Now, generate 100 samples.
        for i in range(100):
            action = {agent_id: i for agent_id in obs}
            obs, reward, terminated, truncated, info = env.step(action)
            episode_1.add_env_step(
                observations=obs,
                actions=action,
                rewards=reward,
                infos=info,
                terminateds=terminated,
                truncateds=truncated,
                extra_model_outputs={agent_id: {"extra": 10} for agent_id in action},
            )

        # First, receive the last rewards without considering buffered values.
        last_rewards = episode_1.get_rewards(partial=False, consider_buffer=False)
        self.assertIn("agent_9", last_rewards)
        self.assertEqual(episode_1.global_t_to_local_t["agent_9"][-1], 100)
        self.assertEqual(episode_1.agent_episodes["agent_9"].rewards[-1], 1.0)
        self.assertEqual(last_rewards["agent_9"][0], 1.0)
        self.assertIn("agent_0", last_rewards)
        self.assertEqual(episode_1.global_t_to_local_t["agent_0"][-1], 100)
        self.assertEqual(episode_1.agent_episodes["agent_0"].rewards[-1], 1.0)
        self.assertEqual(last_rewards["agent_0"][0], 1.0)
        self.assertIn("agent_2", last_rewards)
        self.assertEqual(episode_1.global_t_to_local_t["agent_2"][-1], 100)
        self.assertEqual(episode_1.agent_episodes["agent_2"].rewards[-1], 1.0)
        self.assertEqual(last_rewards["agent_2"][0], 1.0)
        self.assertIn("agent_5", last_rewards)
        self.assertEqual(episode_1.global_t_to_local_t["agent_5"][-1], 100)
        self.assertEqual(episode_1.agent_episodes["agent_5"].rewards[-1], 1.0)
        self.assertEqual(last_rewards["agent_5"][0], 1.0)
        self.assertIn("agent_8", last_rewards)
        self.assertEqual(episode_1.global_t_to_local_t["agent_8"][-1], 100)
        self.assertEqual(episode_1.agent_episodes["agent_8"].rewards[-1], 1.0)
        self.assertEqual(last_rewards["agent_8"][0], 1.0)
        self.assertIn("agent_4", last_rewards)
        self.assertEqual(episode_1.global_t_to_local_t["agent_4"][-1], 100)
        self.assertEqual(episode_1.agent_episodes["agent_4"].rewards[-1], 1.0)
        self.assertEqual(last_rewards["agent_4"][0], 1.0)
        self.assertIn("agent_3", last_rewards)
        self.assertEqual(episode_1.global_t_to_local_t["agent_3"][-1], 100)
        # Agent 3 had a partial reward before the last recorded observation.
        self.assertEqual(episode_1.agent_episodes["agent_3"].rewards[-1], 2.0)
        self.assertEqual(last_rewards["agent_3"][0], 2.0)
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)
        self.assertNotIn("agent_6", last_rewards)
        self.assertNotIn("agent_7", last_rewards)

        # Now return the same as list.
        last_rewards = episode_1.get_rewards(
            partial=False, consider_buffer=False, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        self.assertEqual(last_rewards[0]["agent_9"], 1.0)
        self.assertIn("agent_0", last_rewards[0])
        self.assertEqual(last_rewards[0]["agent_0"], 1.0)
        self.assertIn("agent_2", last_rewards[0])
        self.assertEqual(last_rewards[0]["agent_2"], 1.0)
        self.assertIn("agent_5", last_rewards[0])
        self.assertEqual(last_rewards[0]["agent_5"], 1.0)
        self.assertIn("agent_8", last_rewards[0])
        self.assertEqual(last_rewards[0]["agent_8"], 1.0)
        self.assertIn("agent_4", last_rewards[0])
        self.assertEqual(last_rewards[0]["agent_4"], 1.0)
        self.assertIn("agent_3", last_rewards[0])
        self.assertEqual(last_rewards[0]["agent_3"], 2.0)
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)
        self.assertNotIn("agent_6", last_rewards)
        self.assertNotIn("agent_7", last_rewards)

        # Now request the last two indices.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=False, consider_buffer=False
        )
        self.assertIn("agent_9", last_rewards)
        self.assertListEqual(episode_1.global_t_to_local_t["agent_9"][-2:], [99, 100])
        self.assertListEqual(
            episode_1.agent_episodes["agent_9"].rewards[-2:], last_rewards["agent_9"]
        )
        self.assertIn("agent_5", last_rewards)
        self.assertListEqual(episode_1.global_t_to_local_t["agent_5"][-2:], [99, 100])
        # Agent 5 has already died, so we need to convert back to list.
        self.assertListEqual(
            episode_1.agent_episodes["agent_5"].rewards.tolist()[-2:],
            last_rewards["agent_5"],
        )
        self.assertIn("agent_2", last_rewards)
        self.assertListEqual(episode_1.global_t_to_local_t["agent_2"][-2:], [99, 100])
        self.assertListEqual(
            episode_1.agent_episodes["agent_2"].rewards[-1:-3:-1],
            last_rewards["agent_2"],
        )
        # Agent 2 had no observation at `ts=98`, but partial rewards.
        self.assertGreater(99, episode_1.global_t_to_local_t["agent_2"][-3])
        # Ensure that for agent 2 there had been three partial rewards in between the
        # observation at `ts=95` and the next at `ts=99`.
        self.assertListEqual(
            episode_1.partial_rewards_t["agent_2"][-4:-1], [96, 98, 99]
        )
        self.assertIn("agent_3", last_rewards)
        # Agent 3 had no observation at `ts=99`.
        self.assertListEqual(episode_1.global_t_to_local_t["agent_3"][-2:], [98, 100])
        self.assertEqual(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards["agent_3"][0]
        )
        # Ensure that there was a partial reward at `ts=99`.
        self.assertListEqual(episode_1.partial_rewards_t["agent_3"][-2:], [99, 100])
        self.assertIn("agent_4", last_rewards)
        self.assertListEqual(episode_1.global_t_to_local_t["agent_4"][-2:], [99, 100])
        self.assertListEqual(
            episode_1.agent_episodes["agent_4"].rewards[-2:], last_rewards["agent_4"]
        )
        self.assertIn("agent_8", last_rewards)
        # Ensure that the third-last observation is before `ts=98`.
        self.assertListEqual(
            episode_1.global_t_to_local_t["agent_8"][-3:], [97, 99, 100]
        )
        # Ensure also that at `ts=97` there was a reward.
        self.assertListEqual(episode_1.partial_rewards_t["agent_8"][-3:-1], [98, 99])
        self.assertListEqual([1.0, 2.0], last_rewards["agent_8"])
        self.assertIn("agent_7", last_rewards)
        # Agent 7 has no observation at `ts=100`, but at `ts=98`.
        self.assertListEqual(episode_1.global_t_to_local_t["agent_7"][-2:], [98, 99])
        self.assertEqual(
            episode_1.agent_episodes["agent_7"].rewards[-1], last_rewards["agent_7"][0]
        )
        self.assertIn("agent_0", last_rewards)
        self.assertListEqual(episode_1.global_t_to_local_t["agent_0"][-2:], [99, 100])
        self.assertListEqual(
            episode_1.agent_episodes["agent_0"].rewards[-2:], last_rewards["agent_0"]
        )
        self.assertNotIn("agent_1", last_rewards)
        self.assertNotIn("agent_6", last_rewards)

        # Now request the last two indices as list.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=False, consider_buffer=False, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        self.assertIn("agent_9", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards[0]["agent_9"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_9"].rewards[-2], last_rewards[1]["agent_9"]
        )
        self.assertIn("agent_5", last_rewards[0])
        self.assertIn("agent_5", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards[0]["agent_5"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_5"].rewards[-2], last_rewards[1]["agent_5"]
        )
        self.assertIn("agent_2", last_rewards[0])
        self.assertIn("agent_2", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards[0]["agent_2"]
        )
        self.assertEqual(3.0, last_rewards[1]["agent_2"])
        # Agent 3 has only recorded rewards at `ts=100`.
        self.assertIn("agent_3", last_rewards[0])
        self.assertEqual(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards[0]["agent_3"]
        )
        self.assertIn("agent_4", last_rewards[0])
        self.assertIn("agent_4", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards[0]["agent_4"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_4"].rewards[-2], last_rewards[1]["agent_4"]
        )
        self.assertIn("agent_8", last_rewards[0])
        self.assertIn("agent_8", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards[0]["agent_8"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_8"].rewards[-2], last_rewards[1]["agent_8"]
        )
        # Agent 7 has no observation at `ts=100`.
        self.assertIn("agent_7", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_7"].rewards[-1], last_rewards[1]["agent_7"]
        )
        self.assertIn("agent_0", last_rewards[0])
        self.assertIn("agent_0", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards[0]["agent_0"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_0"].rewards[-2], last_rewards[1]["agent_0"]
        )
        self.assertNotIn("agent_1", last_rewards[0])
        self.assertNotIn("agent_6", last_rewards[0])
        self.assertNotIn("agent_1", last_rewards[1])
        self.assertNotIn("agent_6", last_rewards[1])

        # Second, get the last rewards with a single index, consider all partial
        # rewards after the last recorded observation of an agent, i.e. set
        # `consider_buffer` to `True`.
        last_rewards = episode_1.get_rewards(partial=False, consider_buffer=True)
        self.assertIn("agent_9", last_rewards)
        self.assertEqual(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards["agent_9"][0]
        )
        self.assertIn("agent_0", last_rewards)
        self.assertEqual(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards["agent_0"][0]
        )
        self.assertIn("agent_2", last_rewards)
        self.assertEqual(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards["agent_2"][0]
        )
        self.assertIn("agent_5", last_rewards)
        self.assertEqual(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards["agent_5"][0]
        )
        self.assertIn("agent_8", last_rewards)
        self.assertEqual(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards["agent_8"][0]
        )
        self.assertIn("agent_4", last_rewards)
        self.assertEqual(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards["agent_4"][0]
        )
        self.assertIn("agent_3", last_rewards)
        # Agent 3 had a partial reward before the last recorded observation.
        self.assertEqual(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards["agent_3"][0]
        )
        # Agent 7 has a partial reward at `ts=100` after its last observation at
        # `ts=99`.
        self.assertIn("agent_7", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_7"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1], last_rewards["agent_7"][0]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)
        self.assertNotIn("agent_6", last_rewards)

        # Now request the last rewards as a list while considering the buffer.
        last_rewards = episode_1.get_rewards(
            partial=False, consider_buffer=True, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        self.assertEqual(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards[0]["agent_9"]
        )
        self.assertIn("agent_0", last_rewards[0])
        self.assertEqual(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards[0]["agent_0"]
        )
        self.assertIn("agent_2", last_rewards[0])
        self.assertEqual(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards[0]["agent_2"]
        )
        self.assertIn("agent_5", last_rewards[0])
        self.assertEqual(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards[0]["agent_5"]
        )
        self.assertIn("agent_8", last_rewards[0])
        self.assertEqual(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards[0]["agent_8"]
        )
        self.assertIn("agent_4", last_rewards[0])
        self.assertEqual(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards[0]["agent_4"]
        )
        self.assertIn("agent_3", last_rewards[0])
        # Agent 3 had a partial reward before the last recorded observation.
        self.assertEqual(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards[0]["agent_3"]
        )
        # Agent 7 has a partial reward at `ts=100` after its last observation at
        # `ts=99`.
        self.assertIn("agent_7", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards[0])
        self.assertNotIn("agent_6", last_rewards[0])

        # Now request the last two indices and consider buffered partial rewards after
        # the last observation.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=False, consider_buffer=True
        )
        self.assertIn("agent_9", last_rewards)
        self.assertListEqual(
            episode_1.agent_episodes["agent_9"].rewards[-1:-3:-1],
            last_rewards["agent_9"],
        )
        self.assertIn("agent_0", last_rewards)
        self.assertListEqual(
            episode_1.agent_episodes["agent_0"].rewards[-1:-3:-1],
            last_rewards["agent_0"],
        )
        self.assertIn("agent_2", last_rewards)
        self.assertListEqual(
            episode_1.agent_episodes["agent_2"].rewards[-1:-3:-1],
            last_rewards["agent_2"],
        )
        self.assertIn("agent_5", last_rewards)
        # Agent 5 already died, so we need to convert to list first.
        self.assertListEqual(
            episode_1.agent_episodes["agent_5"].rewards.tolist()[-1:-3:-1],
            last_rewards["agent_5"],
        )
        self.assertIn("agent_8", last_rewards)
        self.assertListEqual(
            episode_1.agent_episodes["agent_8"].rewards[-1:-3:-1],
            last_rewards["agent_8"],
        )
        self.assertIn("agent_4", last_rewards)
        self.assertListEqual(
            episode_1.agent_episodes["agent_4"].rewards[-1:-3:-1],
            last_rewards["agent_4"],
        )
        # Nothing changes for agent 3 as it has an observation at the last requested
        # timestep 100, but not at `ts=99`.
        self.assertIn("agent_3", last_rewards)
        self.assertEqual(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards["agent_3"][0]
        )
        # The entries for agent 6 have changed now b/c it has partial rewards during the
        # requested timesteps 100 and 99.
        self.assertIn("agent_6", last_rewards)
        self.assertListEqual(episode_1.global_t_to_local_t["agent_6"][-2:], [95, 98])
        self.assertListEqual(episode_1.partial_rewards_t["agent_6"][-2:], [99, 100])
        self.assertListEqual(
            episode_1.partial_rewards["agent_6"][-2:], last_rewards["agent_6"]
        )
        # Entries for agent 7 also change b/c this agent has a partial reward at
        # `ts=100` while it has no observation recorded at this timestep.
        self.assertIn("agent_7", last_rewards)
        self.assertListEqual(episode_1.global_t_to_local_t["agent_7"][-2:], [98, 99])
        self.assertListEqual(episode_1.partial_rewards_t["agent_7"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1], last_rewards["agent_7"][0]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_7"].rewards[-1], last_rewards["agent_7"][1]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)

        # Now request the same indices with `consider_buffer=True` and return them as
        # a list.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=False, consider_buffer=True, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        self.assertIn("agent_9", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards[0]["agent_9"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_9"].rewards[-2], last_rewards[1]["agent_9"]
        )
        self.assertIn("agent_0", last_rewards[0])
        self.assertIn("agent_0", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards[0]["agent_0"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_0"].rewards[-2], last_rewards[1]["agent_0"]
        )
        self.assertIn("agent_2", last_rewards[0])
        self.assertIn("agent_2", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards[0]["agent_2"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_2"].rewards[-2], last_rewards[1]["agent_2"]
        )
        self.assertIn("agent_5", last_rewards[0])
        self.assertIn("agent_5", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards[0]["agent_5"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_5"].rewards[-2], last_rewards[1]["agent_5"]
        )
        self.assertIn("agent_8", last_rewards[0])
        self.assertIn("agent_8", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards[0]["agent_8"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_8"].rewards[-2], last_rewards[1]["agent_8"]
        )
        self.assertIn("agent_4", last_rewards[0])
        self.assertIn("agent_4", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards[0]["agent_4"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_4"].rewards[-2], last_rewards[1]["agent_4"]
        )
        # Nothing changes for agent 3 as it has an observation at the last requested
        # timestep 100.
        self.assertIn("agent_3", last_rewards[0])
        self.assertNotIn("agent_3", last_rewards[1])
        self.assertEqual(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards[0]["agent_3"]
        )
        # The entries for agent 6 have changed now b/c it has partial rewards during the
        # requested timesteps 100 and 99.
        self.assertIn("agent_6", last_rewards[0])
        self.assertIn("agent_6", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_6"][-1], last_rewards[0]["agent_6"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_6"][-2], last_rewards[1]["agent_6"]
        )
        # Entries for agent 7 also change b/c this agent has a partial reward at
        # `ts=100` while it has no observation recorded at this timestep.
        self.assertIn("agent_7", last_rewards[0])
        self.assertIn("agent_7", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"]
        )
        self.assertEqual(
            episode_1.agent_episodes["agent_7"].rewards[-1], last_rewards[1]["agent_7"]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards[0])
        self.assertNotIn("agent_1", last_rewards[1])

        # Third, request only partial rewards, i.e. rewards do not get buffered and
        # added up.
        last_rewards = episode_1.get_rewards(partial=True, consider_buffer=False)
        self.assertIn("agent_9", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_9"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_9"][-1], last_rewards["agent_9"][-1]
        )
        self.assertIn("agent_0", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_0"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_0"][-1], last_rewards["agent_0"][-1]
        )
        self.assertIn("agent_2", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_2"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_2"][-1], last_rewards["agent_2"][-1]
        )
        self.assertIn("agent_8", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_8"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_8"][-1], last_rewards["agent_8"][-1]
        )
        self.assertIn("agent_4", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_4"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_4"][-1], last_rewards["agent_4"][-1]
        )
        self.assertIn("agent_3", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_3"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_3"][-1], last_rewards["agent_3"][-1]
        )
        self.assertIn("agent_6", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_6"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_6"][-1], last_rewards["agent_6"][-1]
        )
        self.assertIn("agent_7", last_rewards)
        self.assertEqual(episode_1.partial_rewards_t["agent_7"][-1], 100)
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1], last_rewards["agent_7"][-1]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)

        # Now request all partial rewards at the last timestep and return them as
        # a list.
        last_rewards = episode_1.get_rewards(
            partial=True, consider_buffer=False, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_9"][-1], last_rewards[0]["agent_9"]
        )
        self.assertIn("agent_0", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_0"][-1], last_rewards[0]["agent_0"]
        )
        self.assertIn("agent_2", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_2"][-1], last_rewards[0]["agent_2"]
        )
        self.assertIn("agent_8", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_8"][-1], last_rewards[0]["agent_8"]
        )
        self.assertIn("agent_4", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_4"][-1], last_rewards[0]["agent_4"]
        )
        self.assertIn("agent_3", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_3"][-1], last_rewards[0]["agent_3"]
        )
        self.assertIn("agent_6", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_6"][-1], last_rewards[0]["agent_6"]
        )
        self.assertIn("agent_7", last_rewards[0])
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards[0])

        # Request the last two indices, but consider only partial rewards.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=True, consider_buffer=False
        )
        self.assertIn("agent_9", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_9"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_9"][-1:-3:-1], last_rewards["agent_9"]
        )
        self.assertIn("agent_0", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_0"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_0"][-1:-3:-1], last_rewards["agent_0"]
        )
        self.assertIn("agent_2", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_2"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_2"][-1:-3:-1], last_rewards["agent_2"]
        )
        self.assertIn("agent_8", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_8"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_8"][-1:-3:-1], last_rewards["agent_8"]
        )
        self.assertIn("agent_4", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_4"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_4"][-1:-3:-1], last_rewards["agent_4"]
        )
        self.assertIn("agent_3", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_3"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_3"][-1:-3:-1], last_rewards["agent_3"]
        )
        self.assertIn("agent_6", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_6"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_6"][-1:-3:-1], last_rewards["agent_6"]
        )
        self.assertIn("agent_7", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_7"][-2:], [99, 100])
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1:-3:-1], last_rewards["agent_7"]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)

        # At last, request the last two indices for only partial rewards and return
        # them as list.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=True, consider_buffer=False, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        self.assertIn("agent_9", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_9"][-1], last_rewards[0]["agent_9"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_9"][-2], last_rewards[1]["agent_9"]
        )
        self.assertIn("agent_0", last_rewards[0])
        self.assertIn("agent_0", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_0"][-1], last_rewards[0]["agent_0"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_0"][-2], last_rewards[1]["agent_0"]
        )
        self.assertIn("agent_2", last_rewards[0])
        self.assertIn("agent_2", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_2"][-1], last_rewards[0]["agent_2"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_2"][-2], last_rewards[1]["agent_2"]
        )
        self.assertIn("agent_8", last_rewards[0])
        self.assertIn("agent_8", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_8"][-1], last_rewards[0]["agent_8"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_8"][-2], last_rewards[1]["agent_8"]
        )
        self.assertIn("agent_4", last_rewards[0])
        self.assertIn("agent_4", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_4"][-1], last_rewards[0]["agent_4"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_4"][-2], last_rewards[1]["agent_4"]
        )
        self.assertIn("agent_3", last_rewards[0])
        self.assertIn("agent_3", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_3"][-1], last_rewards[0]["agent_3"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_3"][-2], last_rewards[1]["agent_3"]
        )
        self.assertIn("agent_6", last_rewards[0])
        self.assertIn("agent_6", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_6"][-1], last_rewards[0]["agent_6"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_6"][-2], last_rewards[1]["agent_6"]
        )
        self.assertIn("agent_7", last_rewards[0])
        self.assertIn("agent_7", last_rewards[1])
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"]
        )
        self.assertEqual(
            episode_1.partial_rewards["agent_7"][-2], last_rewards[1]["agent_7"]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards[0])
        self.assertNotIn("agent_1", last_rewards[1])

        # Now, test with `global_ts=False`, i.e. on local level.
        # Begin with `partial=False` and `consider_buffer=False`

        # --- is_terminated, is_truncated ---

    def test_concat_episode(self):
        # Generate a multi-agent episode and environment and sample 100 steps.
        # Note, we do not want the test environment to truncate at step 200.
        episode_1, env = self._mock_multi_agent_records_from_env(
            size=100, truncate=False
        )
        # Now, create a successor episode.
        episode_2 = episode_1.cut()
        # Generate 100 more samples from the environment and store it in the episode.
        episode_2, env = self._mock_multi_agent_records_from_env(
            size=100, episode=episode_2, env=env, init=False
        )
        # Make sure that the successor is now at 200 timesteps.
        self.assertTrue(episode_2.t, 200)

        # Now concatenate the two episodes.
        episode_1.concat_episode(episode_2)
        # Assert that episode 1 has now 200 timesteps.
        self.assertTrue(episode_1.t, 200)
        # Ensure that `episode_1` is done if `episode_2` is.
        self.assertEqual(episode_1.is_terminated, episode_2.is_terminated)
        self.assertEqual(episode_1.is_truncated, episode_2.is_truncated)
        # Assert that for all agents the last observation is at the correct timestep
        # and of correct value.
        for agent_id, agent_eps in episode_1.agent_episodes.items():
            # Also ensure that the global timestep mapping is correct.
            # Note, for done agents there was only an empty timestep mapping in
            # `episode_2`.
            if not agent_eps.is_done:
                self.assertEqual(
                    episode_1.global_t_to_local_t[agent_id][-1],
                    episode_2.global_t_to_local_t[agent_id][-1],
                )
                self.assertEqual(
                    agent_eps.observations[-1],
                    episode_2.agent_episodes[agent_id].observations[-1],
                )
                self.assertEqual(
                    agent_eps.actions[-1],
                    episode_2.agent_episodes[agent_id].actions[-1],
                )
                self.assertEqual(
                    agent_eps.rewards[-1],
                    episode_2.agent_episodes[agent_id].rewards[-1],
                )
                self.assertEqual(
                    agent_eps.infos[-1], episode_2.agent_episodes[agent_id].infos[-1]
                )
                # Note, our test environment produces always these extra model outputs.
                self.assertEqual(
                    agent_eps.extra_model_outputs["extra"][-1],
                    episode_2.agent_episodes[agent_id].extra_model_outputs["extra"][-1],
                )

        # Ensure that all global timestep mappings have no duplicates after
        # concatenation and matches the observation lengths.
        for agent_id, agent_map in episode_1.global_t_to_local_t.items():
            self.assertEqual(len(agent_map), len(set(agent_map)))
            self.assertEqual(
                len(agent_map), len(episode_1.agent_episodes[agent_id].observations)
            )

        # Now assert that all buffers remained the same.
        for agent_id, agent_buffer in episode_1.agent_buffers.items():
            # Make sure the actions buffers are either both full or both empty.
            self.assertEqual(
                agent_buffer["actions"].full(),
                episode_2.agent_buffers[agent_id]["actions"].full(),
            )

            # If the buffer is full ensure `episode_2`'s buffer is identical in value.
            if agent_buffer["actions"].full():
                self.assertEqual(
                    agent_buffer["actions"].queue[0],
                    episode_2.agent_buffers[agent_id]["actions"].queue[0],
                )
            # If the agent is not done, the buffers should be equal in value.
            if not episode_1.agent_episodes[agent_id].is_done:
                self.assertEqual(
                    agent_buffer["rewards"].queue[0],
                    episode_2.agent_buffers[agent_id]["rewards"].queue[0],
                )
                self.assertEqual(
                    agent_buffer["extra_model_outputs"].queue[0],
                    episode_2.agent_buffers[agent_id]["extra_model_outputs"].queue[0],
                )
            # If the agent is done, then all buffers should be empty in both episodes.
            else:
                self.assertTrue(agent_buffer["actions"].empty())
                self.assertTrue(agent_buffer["rewards"].empty())
                self.assertTrue(agent_buffer["extra_model_outputs"].empty())
                self.assertTrue(episode_2.agent_buffers[agent_id]["actions"].empty())
                self.assertTrue(episode_2.agent_buffers[agent_id]["rewards"].empty())
                self.assertTrue(
                    episode_2.agent_buffers[agent_id]["extra_model_outputs"].empty()
                )

        # Ensure that all global action timestep mappings do not contain duplicates.
        for agent_id, agent_action_ts_map in episode_1.global_actions_t.items():
            self.assertEqual(len(agent_action_ts_map), len(set(agent_action_ts_map)))
            # Also ensure that the timestep mapping are at least as long as action
            # history.
            self.assertGreaterEqual(
                len(agent_action_ts_map),
                len(episode_1.agent_episodes[agent_id].actions),
            )
            # Done agents might not have these lists in `episode_2`.
            if not episode_1.agent_episodes[agent_id].is_done:
                # Ensure for agents that are not done that the last entry matches.
                self.assertEqual(
                    agent_action_ts_map[-1],
                    episode_2.global_actions_t[agent_id][-1],
                )

        # Ensure that partial reward timestep mapping have no duplicates and that
        # partial rewards match.
        for agent_id, agent_partial_rewards_t in episode_1.partial_rewards_t.items():
            self.assertEqual(
                len(agent_partial_rewards_t), len(set(agent_partial_rewards_t))
            )
            # Done agents might not have these lists in `episode_2`.
            if not episode_1.agent_episodes[agent_id].is_done:
                # Ensure for agents that are not done that the last entries match.
                self.assertEqual(
                    agent_partial_rewards_t[-1],
                    episode_2.partial_rewards_t[agent_id][-1],
                )
                self.assertEqual(
                    episode_1.partial_rewards[agent_id][-1],
                    episode_2.partial_rewards[agent_id][-1],
                )
            # Partial reward timestep mappings and partial reward list should have
            # identical length.
            self.assertEqual(
                len(agent_partial_rewards_t), len(episode_1.partial_rewards[agent_id])
            )

        # Assert that the getters work.
        last_observation = episode_1.get_observations()
        # Ensure that the last observation is indeed the test environment's
        # one at that timestep (200).
        for agent_id, agent_obs in last_observation.items():
            self.assertEqual(episode_2.t, agent_obs[0])
            # Assert that the timestep mapping did record the last timestep for this
            # observation.
            self.assertEqual(episode_2.t, episode_1.global_t_to_local_t[agent_id][-1])

        # Ensure that the last action is recorded and extracted correctly.
        last_action = episode_1.get_actions()
        # Note, the last action at a timestep has a value equal to the timestep.
        for agent_id, agent_action in last_action.items():
            # If the buffer is full and the last action timestep is the last timestep
            # the action must be in the buffer.
            if (
                episode_1.agent_buffers[agent_id]["actions"].full()
                and episode_1.global_actions_t[agent_id][-1] == episode_1.t
            ):
                self.assertEqual(
                    episode_1.agent_buffers[agent_id]["actions"].queue[0],
                    agent_action[0],
                )
            # In the other case the action was recorded in the `SingleAgentEpisode`
            # of this agent.
            else:
                # Make sure that the correct timestep is recorded.
                self.assertEqual(episode_1.global_actions_t[agent_id][-1], episode_1.t)
                # Ensure that the correct action is recorded.
                self.assertEqual(
                    episode_1.agent_episodes[agent_id].actions[-1], agent_action[0]
                )
            # For any action the value
            self.assertEqual(episode_1.global_actions_t[agent_id][-1], agent_action[0])

        # Get the last reward. Use only the recorded ones from the
        # `SingleAgentEpisode`s.
        last_reward = episode_1.get_rewards(partial=False, consider_buffer=False)
        for agent_id, agent_reward in last_reward.items():
            # Ensure that the last recorded reward is the one in the getter result.
            self.assertEqual(
                episode_1.agent_episodes[agent_id].rewards[-1], agent_reward[0]
            )
        # Assert also that the partial rewards are correctly extracted.
        # Note, `partial=True` does not look into the reward histories in the agents'
        # `SingleAgentEpisode`s, but instead into the global reward histories on
        # top level, where all partial rewards are recorded.
        last_reward = episode_1.get_rewards(partial=True)
        for agent_id, agent_reward in last_reward.items():
            # Ensure that the reward is the last in the global history.
            self.assertEqual(episode_1.partial_rewards[agent_id][-1], agent_reward[0])
            # Assert also that the correct timestep was recorded.
            self.assertEqual(episode_1.partial_rewards_t[agent_id][-1], episode_2.t)
        # Now get all recorded rewards from the `SingleAgentEpisode`s plus
        # anything after the last record therein, i.e. from the buffer.
        # Note, this could also include partial rewards for agents that never
        # stepped, yet. Partial rewards will be accumulated until the next index
        # requested in `get_rewards()`.
        last_reward = episode_1.get_rewards(partial=False, consider_buffer=True)
        for agent_id, agent_reward in last_reward.items():
            # Make sure that if the buffer is full the returned reward equals the
            # buffer.
            if episode_1.agent_buffers[agent_id]["actions"].full():
                self.assertEqual(
                    episode_1.agent_buffers[agent_id]["rewards"].queue[0],
                    agent_reward[0],
                )
            # If the buffer is empty, the reward must come from the `
            # SingleAgentEpisode`'s reward history. Ensure that these two values
            # are identical.
            else:
                self.assertEqual(
                    episode_1.agent_episodes[agent_id].rewards[-1], agent_reward[0]
                )

        # Assert that the concatenated episode is immutable in its objects.
        buffered_action = episode_2.agent_buffers["agent_4"]["actions"].get_nowait()
        episode_2.agent_buffers["agent_4"]["actions"].put_nowait(10000)
        self.assertNotEqual(
            episode_2.agent_buffers["agent_4"]["actions"].queue[0], buffered_action
        )
        self.assertEqual(
            episode_1.agent_buffers["agent_4"]["actions"].queue[0], buffered_action
        )

    def test_get_return(self):
        # Generate an empty episode and ensure that the return is zero.
        episode = MultiAgentEpisode()
        # Ensure that the return of an empty episode cannopt be determined.
        with self.assertRaises(AssertionError):
            episode.get_return()

        # Now sample 100 timesteps.
        episode, env = self._mock_multi_agent_records_from_env()
        # Ensure that the return is now at least zero.
        self.assertGreaterEqual(episode.get_return(), 0.0)
        # Assert that the return is indeed the sum of all agent returns.
        agent_returns = sum(
            agent_eps.get_return() for agent_eps in episode.agent_episodes.values()
        )
        self.assertTrue(episode.get_return(), agent_returns)
        # Assert that adding the buffered rewards to the agent returns
        # gives the expected result when considering the buffer in
        # `get_return()`.
        buffered_rewards = sum(
            agent_buffer["rewards"].queue[0]
            for agent_buffer in episode.agent_buffers.values()
            if agent_buffer["rewards"].full()
        )
        self.assertTrue(
            episode.get_return(consider_buffer=True), agent_returns + buffered_rewards
        )

    def test_len(self):
        # Generate an empty episode and ensure that `len()` raises an error.
        episode = MultiAgentEpisode()
        # Now raise an error.
        with self.assertRaises(AssertionError):
            len(episode)

        # Create an episode and environment and sample 100 timesteps.
        episode, env = self._mock_multi_agent_records_from_env()
        # Assert that the length is indeed 100.
        self.assertEqual(len(episode), 100)
        # Now, build a successor.
        successor = episode.cut()
        # Sample another 100 timesteps.
        successor, env = self._mock_multi_agent_records_from_env(
            episode=successor, env=env, init=False
        )
        # Ensure that the length of the successor is 100.
        self.assertTrue(len(successor), 100)
        # Now concatenate the two episodes.
        episode.concat_episode(successor)
        # Assert that the length is now 100.
        self.assertTrue(len(episode), 200)

    def test_to_sample_batch(self):
        # Generate an environment and episode and sample 100 timesteps.
        episode, env = self._mock_multi_agent_records_from_env()

        # Now convert to sample batch.
        batch = episode.to_sample_batch()

        # Assert that the timestep in the `MultiAgentBatch` is identical
        # to the episode timestep.
        self.assertEqual(len(batch), len(episode))
        # Assert that all agents are present in the multi-agent batch.
        # Note, all agents have collected samples.
        for agent_id in episode._agent_ids:
            self.assertTrue(agent_id in batch.policy_batches)
        # Assert that the recorded history length is correct.
        for agent_id, agent_eps in episode.agent_episodes.items():
            self.assertEqual(len(agent_eps), len(batch[agent_id]))
        # Assert that terminated agents are terminated in the sample batch.
        for agent_id in ["agent_1", "agent_5"]:
            self.assertTrue(batch[agent_id]["terminateds"][-1])

        # Now test that when creating a successor its sample batch will
        # contain the correct values.
        successor = episode.cut()
        # Run 100 more timesteps for the successor.
        successor, env = self._mock_multi_agent_records_from_env(
            episode=successor, env=env, init=False
        )
        # Convert this episode to a `MultiAgentBatch`.
        batch = successor.to_sample_batch()
        # Assert that the number of timesteps match between episode and batch.
        # Note, the successor starts at `ts=100`.
        self.assertEqual(len(batch), len(successor))
        # Assert that all agents that were not done, yet, are present in the batch.
        for agent_id in env._agents_alive:
            self.assertTrue(agent_id in batch.policy_batches)
        # Ensure that the timesteps for each agent matches the it's batch length.
        for agent_id, agent_eps in successor.agent_episodes.items():
            # Note, we take over agent_ids
            if not agent_eps.is_done:
                self.assertEqual(len(agent_eps), len(batch[agent_id]))
        # Assert that now all agents are truncated b/c the environment truncated
        # them.
        for agent_id in batch.policy_batches:
            self.assertTrue(batch[agent_id]["truncateds"][-1])

        # Test now that when we concatenate the same logic holds.
        episode.concat_episode(successor)
        # Convert the concatenated episode to a sample batch now.
        batch = episode.to_sample_batch()
        # Assert that the length of episode and batch match.
        self.assertEqual(len(batch), len(episode))
        # Assert that all agents are present in the multi-agent batch.
        # Note, in the concatenated episode - in contrast to the successor
        # - we have all agents stepped.
        for agent_id in episode._agent_ids:
            self.assertTrue(agent_id in batch.policy_batches)
        # Assert that the recorded history length is correct.
        for agent_id, agent_eps in episode.agent_episodes.items():
            self.assertEqual(len(agent_eps), len(batch[agent_id]))
        # Assert that terminated agents are terminated in the sample batch.
        for agent_id in ["agent_1", "agent_5"]:
            self.assertTrue(batch[agent_id]["terminateds"][-1])
        # Assert that all the other agents are truncated by the environment.
        for agent_id in env._agents_alive:
            self.assertTrue(batch[agent_id]["truncateds"][-1])

        # Finally, test that an empty episode, gives an empty batch.
        episode = MultiAgentEpisode(agent_ids=env.get_agent_ids())
        # Convert now to sample batch.
        batch = episode.to_sample_batch()
        # Ensure that this batch is empty.
        self.assertEqual(len(batch), 0)

    def _mock_multi_agent_records_from_env(
        self,
        size: int = 100,
        episode: MultiAgentEpisode = None,
        env: gym.Env = None,
        init: bool = True,
        truncate: bool = True,
        seed: Optional[int] = 42,
    ) -> Tuple[MultiAgentEpisode, gym.Env]:
        # If the environment does not yet exist, create one.
        if not env:
            env = MultiAgentTestEnv(truncate=truncate)

        # If no episode is given, construct one.
        if episode is None:
            # We give it the `agent_ids` to make it create all objects.
            episode = MultiAgentEpisode(agent_ids=env._agent_ids)

        # We initialize the episode, if requested.
        if init:
            obs, info = env.reset(seed=seed)
            episode.add_env_reset(observations=obs, infos=info)
        # In the other case wer need at least the last observations for the next
        # actions.
        else:
            obs = {
                agent_id: agent_obs
                for agent_id, agent_obs in episode.get_observations().items()
                if episode.agent_buffers[agent_id]["actions"].empty()
            }

        # Sample `size` many records.
        for i in range(env.t, env.t + size):
            action = {agent_id: i + 1 for agent_id in obs}
            obs, reward, terminated, truncated, info = env.step(action)
            episode.add_env_step(
                observations=obs,
                actions=action,
                rewards=reward,
                infos=info,
                terminateds=terminated,
                truncateds=truncated,
                extra_model_outputs={agent_id: {"extra": 10} for agent_id in action},
            )

        # Return both, epsiode and environment.
        return episode, env

    def _mock_multi_agent_records(self):
        # Create some simple observations, actions, rewards, infos and
        # extra model outputs.
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
        # Let no agent terminate or being truncated.
        terminateds = {
            "__all__": False,
            "agent_1": False,
            "agent_3": False,
            "agent_4": False,
        }
        truncateds = {
            "__all__": False,
            "agent_1": False,
            "agent_3": False,
            "agent_4": False,
        }

        # Return all observations.
        return observations, actions, rewards, terminateds, truncateds, infos


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
