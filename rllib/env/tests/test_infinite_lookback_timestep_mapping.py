import ray
import unittest

from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.tests.test_multi_agent_episode import MultiAgentTestEnv
from ray.rllib.env.utils.infinite_lookback_timestep_mapping import (
    InfiniteLookbackTimestepMapping
)
from ray.rllib.utils.test_utils import check


class TestInfiniteLookbackTimestepMapping(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_init(self):
        # Generate empty mapping.
        ts_map = InfiniteLookbackTimestepMapping()
        check(ts_map.lookback, 0)
        check(ts_map.t_started, 0)
        check(len(ts_map), 0)
        # Make sure no call to `get_local_timestep` returns anything.
        check(ts_map.get_local_timesteps(1), None)
        check(ts_map.get_local_timesteps(2), None)
        check(ts_map.get_local_timesteps(-1), None)
        check(ts_map.get_local_timesteps(-1, neg_timesteps_left_of_zero=True), None)
        check(ts_map.get_local_timesteps(1000), None)
        check(ts_map.get_local_timesteps(slice(0, 40)), [])
        check(ts_map.get_local_timesteps(slice(-100, 40)), [])
        check(ts_map.get_local_timesteps(
            slice(-100, 40), neg_timesteps_left_of_zero=True
        ), [])
        check(ts_map.get_local_timesteps([1, 4, 50, 1000, -1]), [])

        # Now initialize with lookbacks.
        timesteps = list(range(10))
        ts_map = InfiniteLookbackTimestepMapping(timesteps, lookback=3, t_started=4)
        check(ts_map.lookback, 3)
        check(ts_map.t_started, 4)
        check(len(ts_map), 7)

        timesteps = list(range(45, 55))
        ts_map = InfiniteLookbackTimestepMapping(timesteps, lookback=5, t_started=50)
        check(ts_map.lookback, 5)
        check(ts_map.t_started, 50)
        check(len(ts_map), 5)

    def test_get_local_timesteps(self):
        # Generate empty mapping.
        ts_map = InfiniteLookbackTimestepMapping()
        check(ts_map.get_local_timesteps(), [])
        check(ts_map.get_local_timesteps(slice(-10, None)), [])
        check(ts_map.get_local_timesteps([-100, -200, -10000]), [])

        # local ts:  0  1  2  3  4  5  6  7  8   9
        #           [0, 1, 2, 3, 4, 5, 6, 7, 8, (9)]
        timesteps = list(range(10))
        ts_map = InfiniteLookbackTimestepMapping(timesteps, lookback=9, t_started=9)
        check(ts_map.get_local_timesteps(0), 9)
        check(ts_map.get_local_timesteps(-1), 9)
        check(ts_map.get_local_timesteps(-1, neg_timesteps_left_of_zero=True), 8)
        check(ts_map.get_local_timesteps(-2, neg_timesteps_left_of_zero=True), 7)
        check(ts_map.get_local_timesteps(1), None)

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
