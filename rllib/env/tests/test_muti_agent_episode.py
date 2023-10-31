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

        self.last_terminateds = {"__all__": False}
        self.last_terminateds.update({agent_id: False for agent_id in self._agent_ids})
        self.last_truncateds = {"__all__": False}
        self.agents_alive = self._agent_ids
        self.last_truncateds.update({agent_id: False for agent_id in self._agent_ids})

    def reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[MultiAgentDict, MultiAgentDict]:
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
        is_truncated = self.last_truncateds
        is_terminated = self.last_terminateds
        if self.t >= 50:
            # Let agent 1 die.
            is_terminated["agent_1"] = True
        if self.t >= 100:
            # Let agent 5 die.
            is_terminated["agent_5"] = True

        # Truncate the episode if too long.
        if self.t >= 200:
            is_truncated["__all__"] = True

        # Keep the last changes (e.g. an agent died).
        self.last_terminateds = is_terminated
        self.last_truncateds = is_truncated
        self.agents_alive = [
            agent_id
            for agent_id in is_terminated
            if agent_id != "__all__"
            and not is_terminated[agent_id]
            and not is_truncated[agent_id]
        ]

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
            is_terminated=is_terminated,
            is_truncated=is_truncated,
            extra_model_outputs=extra_model_outputs,
        )

        # The starting point and count should now be at `len(observations) - 1`.+
        self.assertTrue(episode.t == episode.t_started == (len(observations) - 1))
        # Assert that agent 1 and agent 5 are both terminated.
        self.assertTrue(episode.agent_episodes["agent_1"].is_terminated)
        self.assertTrue(episode.agent_episodes["agent_5"].is_terminated)
        # Assert that the other agents are all truncated.
        for agent_id in env.get_agent_ids():
            if agent_id != "agent_1" and agent_id != "agent_5":
                self.assertFalse(episode.agent_episodes[agent_id].is_truncated)
                self.assertFalse(episode.agent_episodes[agent_id].is_terminated)

        # Test now intiializing an episode and setting the starting timestep at once.
        episode = MultiAgentEpisode(
            agent_ids=env.get_agent_ids(),
            observations=observations[-11:],
            actions=actions[-10:],
            rewards=rewards[-10:],
            infos=infos[-11:],
            t_started=100,
            states=states,
            is_terminated=is_terminated,
            is_truncated=is_truncated,
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
        # Assert that agent 1 and agent 5 are both terminated.
        self.assertTrue(episode.agent_episodes["agent_1"].is_terminated)
        self.assertTrue(episode.agent_episodes["agent_5"].is_terminated)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
