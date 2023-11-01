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
        states = []
        # Initialize observation and info.
        obs, info = env.reset(seed=0)
        observations.append(obs)
        infos.append(info)
        # Run 100 samples.
        for i in range(100):
            agents_stepped = list(obs.keys())
            action = {agent_id: i + 1 for agent_id in agents_stepped}
            # action = env.action_space_sample(agents_stepped)
            obs, reward, is_terminated, is_truncated, info = env.step(action)
            observations.append(obs)
            actions.append(action)
            rewards.append(reward)
            infos.append(info)
            is_terminateds.append(is_terminated)
            is_truncateds.append(is_truncated)
            states.append({agent_id: np.random.random() for agent_id in agents_stepped})
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
            if i == 99:
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
            elif i == 49:
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
            else:
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
