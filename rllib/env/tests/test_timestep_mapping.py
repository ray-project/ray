import ray
import unittest

from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.tests.test_multi_agent_episode import MultiAgentTestEnv
from ray.rllib.env.timestep_mapping import TimestepMappingWithInfiniteLookback


class Test_TimestepMapping(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_init(self):
        # Generate empty mapping.
        ts_map = TimestepMappingWithInfiniteLookback()
        self.assertEqual(ts_map.lookback, 0)
        self.assertEqual(ts_map.t_started, 0)
        self.assertEqual(len(ts_map), 0)

        # Now initialize with a lookback.
        timesteps = list(range(10))
        ts_map = TimestepMappingWithInfiniteLookback(timesteps, lookback=3, t_started=4)
        self.assertEqual(ts_map.lookback, 3)
        self.assertEqual(ts_map.t_started, 4)
        self.assertEqual(len(ts_map), 7)

    def test_get_local_timesteps(self):
        # Generate empty mapping.
        ts_map = TimestepMappingWithInfiniteLookback()
        local_ts = ts_map.get_local_timesteps()
        self.assertListEqual(local_ts, [])

        local_ts = ts_map.get_local_timesteps(slice(-10, None))
        self.assertListEqual(local_ts, [])

        timesteps = list(range(0, 10))
        ts_map = TimestepMappingWithInfiniteLookback(timesteps, lookback=9, t_started=9)
        local_ts = ts_map.get_local_timesteps(0, t=9)
        self.assertEqual(local_ts, 0)

        local_ts = ts_map.get_local_timesteps(-1, t=9)
        self.assertEqual(local_ts, 8)

        local_ts = ts_map.get_local_timesteps(slice(-10, None), t=9)

    def test_with_sae(self):
        # Create a multi-agent test environment.
        env = MultiAgentTestEnv()

        observations = []
        actions = []
        rewards = []
        infos = []

        # Reset the environment.
        obs, info = env.reset(seed=0)
        observations.append(obs)
        infos.append(info)

        # Sample 100 timesteps.
        for i in range(100):
            # Action is simply the timestep.
            action = {agent_id: i + 1 for agent_id in obs}
            obs, reward, terminated, truncated, info = env.step(action)

            observations.append(obs)
            actions.append(action)
            rewards.append(reward)
            infos.append(info)

        # Generate the multi-agent episode.
        episode = MultiAgentEpisode(
            agent_ids=env.get_agent_ids(),
            observations=observations,
            actions=actions,
            rewards=rewards,
            terminateds=terminated,
            truncateds=truncated,
        )

        # Get observations.
        obs = episode.get_observations(-10)
        obs = episode.get_observations(0)

        # This errors out. Check "agent_6" with index 65.
        # The latter comes from
        # `episode.global_t_to_local_t["agent_6"].
        # get_local_timesteps(-1, t=episode.t, shift=-1)`
        # Get actions.
        actions = episode.get_actions(-1)

        # Just for the debugger; rmeove when ready.
        print(obs)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
