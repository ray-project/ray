import gymnasium as gym
import numpy as np
import unittest

from typing import List, Optional, Tuple

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.typing import MultiAgentDict


class MultiAgentTestEnv(MultiAgentEnv):
    def __init__(self, truncate=True):
        super().__init__()
        self.t = 0
        self._agent_ids = {"agent_" + str(i) for i in range(10)}
        self.observation_space = gym.spaces.Dict(
            {agent_id: gym.spaces.Discrete(201) for agent_id in self._agent_ids}
        )
        self.action_space = gym.spaces.Dict(
            {agent_id: gym.spaces.Discrete(200) for agent_id in self._agent_ids}
        )

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
        # `gym.Env` otherwise results are not reproducible. This RNG is
        # stored to `self._np_random`.
        num_agents_step = self._np_random.integers(1, len(self._agent_ids) + 1)
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
        num_agents_step = self._np_random.integers(1, len(self._agents_alive) + 1)
        # The agents that are ready.
        agents_step = self._np_random.choice(
            np.array(sorted(self._agents_alive)), num_agents_step, replace=False
        )
        # If we are about to truncate, we need to make sure to provide each still-alive
        # agent with an obs, otherwise, the final obs would be missing and we would
        # receive an error in ma-episode.
        if self.t >= 200 and self.truncate:
            agents_step = self._agents_alive

        # Initialize observations.
        obs = {agent_id: self.t for agent_id in agents_step}
        info = {agent_id: {} for agent_id in agents_step}
        reward = {agent_id: 1.0 for agent_id in agents_step}
        # Add also agents without observations.
        reward.update(
            {
                agent_id: 1.0
                for agent_id in self._np_random.choice(
                    np.array(sorted(self._agents_alive)), num_agents_step, replace=False
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
        self.assertTrue(episode.env_t_started == episode.env_t == 0)

        # Create an episode with a specific starting point, but no data.
        episode = MultiAgentEpisode(env_t_started=10)
        self.assertTrue(episode.env_t == episode.env_t_started == 10)

        # Generate a simple multi-agent episode and check all internals after
        # construction.
        observations = [{"a0": 0, "a1": 0}, {"a1": 1}, {"a1": 2}, {"a1": 3}]
        actions = [{"a0": 0, "a1": 0}, {"a1": 1}, {"a1": 2}]
        rewards = [{"a0": 0.1, "a1": 0.1}, {"a1": 0.2}, {"a1": 0.3}]
        episode = MultiAgentEpisode(
            observations=observations, actions=actions, rewards=rewards
        )
        check(episode.agent_episodes["a0"].observations.data, [0])
        check(episode.agent_episodes["a1"].observations.data, [0, 1, 2, 3])
        check(episode.agent_episodes["a0"].actions.data, [])
        check(episode.agent_episodes["a1"].actions.data, [0, 1, 2])
        check(episode.agent_episodes["a0"].rewards.data, [])
        check(episode.agent_episodes["a1"].rewards.data, [0.1, 0.2, 0.3])
        check(episode._agent_buffered_actions, {"a0": 0})
        check(episode._agent_buffered_rewards, {"a0": 0.1})
        check(episode._agent_buffered_extra_model_outputs, {"a0": {}})
        check(episode.env_t_to_agent_t["a0"].data, [0, "S", "S", "S"])
        check(episode.env_t_to_agent_t["a1"].data, [0, 1, 2, 3])
        check(episode.env_t_to_agent_t["a0"].lookback, 3)
        check(episode.env_t_to_agent_t["a1"].lookback, 3)

        # One of the agents doesn't step after reset.
        observations = [{"a0": 0}, {"a1": 1}, {"a0": 2, "a1": 2}, {"a1": 3}, {"a1": 4}]
        actions = [{"a0": 0}, {"a1": 1}, {"a0": 2, "a1": 2}, {"a1": 3}]
        rewards = [{"a0": 0.1}, {"a1": 0.2}, {"a0": 0.3, "a1": 0.3}, {"a1": 0.4}]
        episode = MultiAgentEpisode(
            observations=observations, actions=actions, rewards=rewards
        )
        check(episode.agent_episodes["a0"].observations.data, [0, 2])
        check(episode.agent_episodes["a1"].observations.data, [1, 2, 3, 4])
        check(episode.agent_episodes["a0"].actions.data, [0])
        check(episode.agent_episodes["a1"].actions.data, [1, 2, 3])
        check(episode.agent_episodes["a0"].rewards.data, [0.1])
        check(episode.agent_episodes["a1"].rewards.data, [0.2, 0.3, 0.4])
        check(episode._agent_buffered_actions, {"a0": 2})
        check(episode._agent_buffered_rewards, {"a0": 0.3})
        check(episode._agent_buffered_extra_model_outputs, {"a0": {}})
        check(episode.env_t_to_agent_t["a0"].data, [0, "S", 1, "S", "S"])
        check(episode.env_t_to_agent_t["a1"].data, ["S", 0, 1, 2, 3])
        check(episode.env_t_to_agent_t["a0"].lookback, 4)
        check(episode.env_t_to_agent_t["a1"].lookback, 4)

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

        agent_0_steps = []
        agent_0_num_steps = 0

        # Initialize observation and info.
        obs, info = env.reset(seed=0)

        # If "agent_0" is part of the reset obs, it steps in the first ts.
        agent_0_steps.append(
            agent_0_num_steps if "agent_0" in obs else episode.SKIP_ENV_TS_TAG
        )
        if "agent_0" in obs:
            agent_0_num_steps += 1

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

            # If "agent_0" is part of the reset obs, it steps in the first ts.
            agent_0_steps.append(
                agent_0_num_steps if "agent_0" in obs else episode.SKIP_ENV_TS_TAG
            )
            if "agent_0" in obs:
                agent_0_num_steps += 1

            observations.append(obs)
            actions.append(action)
            rewards.append(reward)
            infos.append(info)
            terminateds.update(terminated)
            truncateds.update(truncated)
            extra_model_outputs.append(
                {agent_id: {"extra_1": 10.5} for agent_id in agents_to_step_next}
            )

        # Now create the episode from the recorded data. Pretend that the given data
        # is all part of the lookback buffer and the episode (chunk) started at the
        # end of that lookback buffer data.
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=terminateds,
            truncateds=truncateds,
            extra_model_outputs=extra_model_outputs,
            env_t_started=len(rewards),
            len_lookback_buffer="auto",  # default
        )

        # The starting point and count should now be at `len(observations) - 1`.+
        self.assertTrue(episode.env_t == episode.env_t_started == len(rewards))
        # Assert that agent 1 and agent 5 are both terminated.
        self.assertTrue(episode.agent_episodes["agent_1"].is_terminated)
        self.assertTrue(episode.agent_episodes["agent_5"].is_terminated)
        # Assert that the other agents are neither terminated nor truncated.
        for agent_id in env.get_agent_ids():
            if agent_id != "agent_1" and agent_id != "agent_5":
                self.assertFalse(episode.agent_episodes[agent_id].is_done)

        # Assert that the agent_0 env_t_to_agent_t mapping is correct:
        check(episode.env_t_to_agent_t["agent_0"].data, agent_0_steps)

        # Test now initializing an episode and setting its starting timestep.
        episode = MultiAgentEpisode(
            observations=observations[-11:],
            actions=actions[-10:],
            rewards=rewards[-10:],
            infos=infos[-11:],
            terminateds=terminateds,
            truncateds=truncateds,
            extra_model_outputs=extra_model_outputs[-10:],
            env_t_started=100,
            len_lookback_buffer="auto",  # default: all data goes into lookback buffers
        )

        # Assert that the episode starts indeed at 100.
        check(episode.env_t, episode.env_t_started, 100)
        # B/c all data went into lookback buffers, all single-agent episodes and
        # the multi-agent episode itself should have len=0.
        check(len(episode), 0)
        for agent_id in episode.agent_ids:
            check(len(episode.agent_episodes[agent_id]), 0)
            check(len(episode.agent_episodes[agent_id].observations), 1)
            check(len(episode.agent_episodes[agent_id].actions), 0)
            check(len(episode.agent_episodes[agent_id].rewards), 0)
            check(episode.agent_episodes[agent_id].is_truncated, False)
            check(episode.agent_episodes[agent_id].is_finalized, False)
        check(episode.agent_episodes["agent_5"].is_terminated, True)
        check(
            episode.env_t_to_agent_t["agent_5"].data,
            ["S", 0, 1, "S", 2, 3, 4, 5, 6, 7, 8],
        )

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
            # agent_ids=["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"],
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=terminateds,
            truncateds=truncateds,
            len_lookback_buffer=0,
        )

        # Assert that the length of `SingleAgentEpisode`s are all correct.
        check(len(episode.agent_episodes["agent_1"]), 1)
        check(len(episode.agent_episodes["agent_2"]), 1)
        check(len(episode.agent_episodes["agent_3"]), 1)
        check(len(episode.agent_episodes["agent_4"]), 1)
        # check(len(episode.agent_episodes["agent_5"]), 0)

        # TODO (simon): Also test the other structs inside the MAE for agent 5 and
        #  the other agents.

    def test_add_env_reset(self):
        # Generate an environment.
        env = MultiAgentTestEnv()
        # Generate an empty multi-agent episode. Note. we have to provide the
        # agent ids.
        episode = MultiAgentEpisode(
            observation_space=env.observation_space,
            action_space=env.action_space,
        )

        # Generate initial observations and infos and add them to the episode.
        obs, infos = env.reset(seed=0)
        episode.add_env_reset(
            observations=obs,
            infos=infos,
        )

        # Assert that timestep is at zero.
        self.assertTrue(episode.env_t == episode.env_t_started == 0)
        # Assert that the agents with initial observations have their single-agent
        # episodes in place.
        for agent_id in env.get_agent_ids():
            # Ensure that all agents have a single env_ts=0 -> agent_ts=0
            # entry in their env- to agent-timestep mappings.
            if agent_id in obs:
                self.assertGreater(
                    len(episode.agent_episodes[agent_id].observations), 0
                )
                self.assertGreater(len(episode.agent_episodes[agent_id].infos), 0)
                check(episode.env_t_to_agent_t[agent_id].data, [0])
            # Agents that have no reset obs, will not step in next ts -> They should NOT
            # have a single agent episod yet and their mappings should be empty.
            else:
                self.assertTrue(agent_id not in episode.agent_episodes)
                check(episode.env_t_to_agent_t[agent_id].data, [])

        # TODO (simon): Test the buffers and reward storage.

    def test_add_env_step(self):
        # Create an environment and add the initial observations and infos.
        env = MultiAgentTestEnv()
        episode = MultiAgentEpisode()

        agent_0_steps = []
        agent_0_num_steps = 0

        obs, infos = env.reset(seed=10)
        episode.add_env_reset(
            observations=obs,
            infos=infos,
        )

        # If "agent_0" is part of the reset obs, it steps in the first ts.
        agent_0_steps.append(
            agent_0_num_steps if "agent_0" in obs else episode.SKIP_ENV_TS_TAG
        )
        if "agent_0" in obs:
            agent_0_num_steps += 1

        # Sample 100 timesteps and add them to the episode.
        for i in range(100):
            action = {
                agent_id: i + 1 for agent_id in obs if agent_id in env._agents_alive
            }
            obs, reward, terminated, truncated, info = env.step(action)

            # If "agent_0" is part of the reset obs, it steps in the first ts.
            agent_0_steps.append(
                agent_0_num_steps if "agent_0" in obs else episode.SKIP_ENV_TS_TAG
            )
            if "agent_0" in obs:
                agent_0_num_steps += 1

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
        check(episode.env_t, 100)
        # Ensure that the episode is not done yet.
        self.assertFalse(episode.is_done)
        # Ensure that agent 1 and agent 5 are indeed done.
        self.assertTrue(episode.agent_episodes["agent_1"].is_done)
        self.assertTrue(episode.agent_episodes["agent_5"].is_done)
        # Also ensure that their buffers are all empty:
        for agent_id in ["agent_1", "agent_5"]:
            self.assertTrue(agent_id not in episode._agent_buffered_actions)
            self.assertTrue(agent_id not in episode._agent_buffered_rewards)
            self.assertTrue(agent_id not in episode._agent_buffered_extra_model_outputs)

        # Check validity of agent_0's env_t_to_agent_t mapping.
        check(episode.env_t_to_agent_t["agent_0"].data, agent_0_steps)

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
        for agent_id in episode.agent_ids:
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
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=terminated,
            truncateds=truncated,
            # len_lookback_buffer=0,
            # agent_t_started={},
        )
        # Now test that intermediate rewards will get recorded and actions buffered.
        action = {"agent_2": 3, "agent_4": 3}
        reward = {"agent_1": 1.0, "agent_2": 1.0, "agent_3": 1.0, "agent_5": 1.0}

        observation = {"agent_1": 3, "agent_2": 3}
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
        self.assertTrue(episode._agent_buffered_actions["agent_4"] is not None)
        # Assert that the reward buffers of agents 3 and 5 are at 1.0.
        check(episode._agent_buffered_rewards["agent_3"], 2.2)
        check(episode._agent_buffered_rewards["agent_5"], 1.0)

    def test_get_observations(self):
        # Generate simple records for a multi agent environment.
        (
            observations,
            actions,
            rewards,
            is_terminateds,
            is_truncateds,
            infos,
        ) = self._mock_multi_agent_records()
        # Create a multi-agent episode.
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=is_terminateds,
            truncateds=is_truncateds,
            len_lookback_buffer="auto",  # default: use all data as lookback
        )

        # Get last observations for the multi-agent episode.
        obs = episode.get_observations(indices=-1)
        check(obs, {"agent_2": 2, "agent_4": 2})

        # Return last two observations for the entire env.
        # Also, we flip the indices here and require -1 before -2, this
        # should reflect in the returned results.
        obs = episode.get_observations(indices=[-1, -2])
        # Note, agent 4 has two observations in the last two ones.
        # Note, `get_observations()` returns in the order of the `indices` arg.
        check(obs, {"agent_1": [1], "agent_2": [2], "agent_3": [1], "agent_4": [2, 1]})

        # Return last two observations for the entire env using slice.
        obs = episode.get_observations(slice(-2, None))
        check(
            obs,
            {"agent_1": [1], "agent_2": [2], "agent_3": [1], "agent_4": [1, 2]},
        )

        # Return last four observations for the entire env using slice
        # and `fill`.
        obs = episode.get_observations(slice(-5, None), fill=-10)
        check(
            obs,
            {
                # All first two ts should be 0s (fill before episode even
                # started).
                # 3rd items are the reset obs for agents
                "agent_1": [-10, -10, 0, 1, -10],  # ag1 stepped the first 2 ts
                "agent_2": [-10, -10, 0, -10, 2],  # ag2 stepped first and last ts
                "agent_3": [-10, -10, 0, 1, -10],  # ag3 same as ag1
                "agent_4": [-10, -10, -10, 1, 2],  # ag4 steps in last 2 ts
            },
        )

        # Use `fill` to look into the future (ts=100 and 101).
        obs = episode.get_observations(slice(100, 102), fill=9.9)
        check(
            obs,
            {
                "agent_1": [9.9, 9.9],
                "agent_2": [9.9, 9.9],
                "agent_3": [9.9, 9.9],
                "agent_4": [9.9, 9.9],
            },
        )

        # Return two observations in lookback buffers for the entire env using
        # `neg_indices_left_of_zero=True` and an index list.
        # w/ fill
        obs = episode.get_observations(
            indices=[-2, -1],
            fill=-10,
            neg_indices_left_of_zero=True,
        )
        check(
            obs,
            {
                "agent_1": [0, 1],
                "agent_2": [0, -10],
                "agent_3": [0, 1],
                "agent_4": [-10, 1],
            },
        )
        # Same, but w/o fill
        obs = episode.get_observations(indices=[-2, -1], neg_indices_left_of_zero=True)
        check(
            obs,
            {"agent_1": [0, 1], "agent_2": [0], "agent_3": [0, 1], "agent_4": [1]},
        )

        # Get last observations for each individual agent.
        obs = episode.get_observations(indices=-1, env_steps=False)
        check(obs, {"agent_1": 1, "agent_2": 2, "agent_3": 1, "agent_4": 2})

        # Same, but with `agent_ids` filters.
        obs = episode.get_observations(-1, env_steps=False, agent_ids="agent_1")
        check(obs, {"agent_1": 1})
        obs = episode.get_observations(-1, env_steps=False, agent_ids=["agent_2"])
        check(obs, {"agent_2": 2})
        obs = episode.get_observations(-1, env_steps=False, agent_ids=("agent_3",))
        check(obs, {"agent_3": 1})
        obs = episode.get_observations(-1, env_steps=False, agent_ids={"agent_4"})
        check(obs, {"agent_4": 2})
        obs = episode.get_observations(
            -1, env_steps=False, agent_ids=["agent_1", "agent_2"]
        )
        check(obs, {"agent_1": 1, "agent_2": 2})
        obs = episode.get_observations(-2, env_steps=True, agent_ids={"agent_4"})
        check(obs, {"agent_4": 1})
        obs = episode.get_observations([-1, -2], env_steps=True, agent_ids={"agent_4"})
        check(obs, {"agent_4": [2, 1]})

        # Return the last two observations for each individual agent.
        obs = episode.get_observations(indices=[-1, -2], env_steps=False)
        check(
            obs,
            {
                "agent_1": [1, 0],
                "agent_2": [2, 0],
                "agent_3": [1, 0],
                "agent_4": [2, 1],
            },
        )

        # Now, test the same when returning a list.
        obs = episode.get_observations(return_list=True)
        check(obs, [{"agent_2": 2, "agent_4": 2}])
        # Expect error when calling with env_steps=False.
        with self.assertRaises(ValueError):
            episode.get_observations(env_steps=False, return_list=True)
        # List of indices.
        obs = episode.get_observations(indices=[-1, -2], return_list=True)
        check(
            obs,
            [
                {"agent_2": 2, "agent_4": 2},
                {"agent_1": 1, "agent_3": 1, "agent_4": 1},
            ],
        )
        # Slice of indices w/ fill.
        obs = episode.get_observations(
            slice(-1, 1),
            return_list=True,
            fill=-8,
            neg_indices_left_of_zero=True,
        )
        check(
            obs,
            [
                {"agent_1": 1, "agent_2": -8, "agent_3": 1, "agent_4": 1},
                {"agent_1": -8, "agent_2": 2, "agent_3": -8, "agent_4": 2},
            ],
        )

        # B/c we have lookback="auto" in the ma episode, all data we sent into
        # the c"tor was pushed into the lookback buffers and only the last
        # observations are outside these buffers and will be returned here.
        obs = episode.get_observations(env_steps=False)
        check(
            obs,
            {"agent_1": [1], "agent_2": [2], "agent_3": [1], "agent_4": [2]},
        )

        # Test with initial observations only.
        episode = MultiAgentEpisode()
        episode.add_env_reset(
            observations=observations[0],
            infos=infos[0],
        )
        # Get the last observation for agents and assert that they are correct.
        obs = episode.get_observations()
        for agent_id, agent_obs in observations[0].items():
            check(obs[agent_id][0], agent_obs)
        # Now the same as list.
        obs = episode.get_observations(return_list=True)
        for agent_id, agent_obs in observations[0].items():
            check(obs[0][agent_id], agent_obs)
        # Now by agent steps.
        obs = episode.get_observations(env_steps=False)
        for agent_id, agent_obs in observations[0].items():
            check(obs[agent_id][0], agent_obs)

    def test_get_infos(self):
        # Generate simple records for a multi agent environment.
        (
            observations,
            actions,
            rewards,
            is_terminateds,
            is_truncateds,
            infos,
        ) = self._mock_multi_agent_records()

        # Create a multi-agent episode.
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=is_terminateds,
            truncateds=is_truncateds,
            len_lookback_buffer="auto",  # default: use all data as lookback
        )

        # Get last infos for the multi-agent episode.
        inf = episode.get_infos(indices=-1)
        check(inf, infos[-1])

        # Return last two infos for the entire env.
        # Also, we flip the indices here and require -1 before -2, this
        # should reflect in the returned results.
        inf = episode.get_infos(indices=[-1, -2])
        # Note, agent 4 has two infos in the last two ones.
        # Note, `get_infos()` returns in the order of the `indices` arg.
        check(
            inf,
            {
                "agent_1": [{"a1_i1": 1.1}],
                "agent_2": [{"a2_i2": 2.2}],
                "agent_3": [{"a3_i1": 3.1}],
                "agent_4": [{"a4_i2": 4.2}, {"a4_i1": 4.1}],
            },
        )

        # Return last two infos for the entire env using slice.
        inf = episode.get_infos(slice(-2, None))
        check(
            inf,
            {
                "agent_1": [{"a1_i1": 1.1}],
                "agent_2": [{"a2_i2": 2.2}],
                "agent_3": [{"a3_i1": 3.1}],
                "agent_4": [{"a4_i1": 4.1}, {"a4_i2": 4.2}],
            },
        )

        # Return last four infos for the entire env using slice
        # and `fill`.
        inf = episode.get_infos(slice(-5, None), fill={"4": "2"})
        check(
            inf,
            {
                # All first two ts should be 0s (fill before episode even
                # started).
                # 3rd items are the reset obs for agents
                "agent_1": [
                    {"4": "2"},
                    {"4": "2"},
                    {"a1_i0": 1},
                    {"a1_i1": 1.1},
                    {"4": "2"},
                ],  # ag1 stepped the first 2 ts
                "agent_2": [
                    {"4": "2"},
                    {"4": "2"},
                    {"a2_i0": 2},
                    {"4": "2"},
                    {"a2_i2": 2.2},
                ],  # ag2 stepped first and last ts
                "agent_3": [
                    {"4": "2"},
                    {"4": "2"},
                    {"a3_i0": 3},
                    {"a3_i1": 3.1},
                    {"4": "2"},
                ],  # ag3 same as ag1
                "agent_4": [
                    {"4": "2"},
                    {"4": "2"},
                    {"4": "2"},
                    {"a4_i1": 4.1},
                    {"a4_i2": 4.2},
                ],  # ag4 steps in last 2 ts
            },
        )

        # Use `fill` (but as a non-dict, just to check) to look into the future
        # (ts=100 and 101).
        inf = episode.get_infos(slice(100, 102), fill=9.9)
        check(
            inf,
            {
                "agent_1": [9.9, 9.9],
                "agent_2": [9.9, 9.9],
                "agent_3": [9.9, 9.9],
                "agent_4": [9.9, 9.9],
            },
        )

        # Return two infos in lookback buffers for the entire env using
        # `neg_indices_left_of_zero=True` and an index list.
        # w/ fill
        inf = episode.get_infos(
            indices=[-2, -1],
            fill=-10,
            neg_indices_left_of_zero=True,
        )
        check(
            inf,
            {
                "agent_1": [{"a1_i0": 1}, {"a1_i1": 1.1}],
                "agent_2": [{"a2_i0": 2}, -10],
                "agent_3": [{"a3_i0": 3}, {"a3_i1": 3.1}],
                "agent_4": [-10, {"a4_i1": 4.1}],
            },
        )
        # Same, but w/o fill
        inf = episode.get_infos(indices=[-2, -1], neg_indices_left_of_zero=True)
        check(
            inf,
            {
                "agent_1": [{"a1_i0": 1}, {"a1_i1": 1.1}],
                "agent_2": [{"a2_i0": 2}],
                "agent_3": [{"a3_i0": 3}, {"a3_i1": 3.1}],
                "agent_4": [{"a4_i1": 4.1}],
            },
        )

        # Get last infos for each individual agent.
        inf = episode.get_infos(indices=-1, env_steps=False)
        check(
            inf,
            {
                "agent_1": {"a1_i1": 1.1},
                "agent_2": {"a2_i2": 2.2},
                "agent_3": {"a3_i1": 3.1},
                "agent_4": {"a4_i2": 4.2},
            },
        )

        # Same, but with `agent_ids` filters.
        inf = episode.get_infos(-1, env_steps=False, agent_ids="agent_1")
        check(inf, {"agent_1": {"a1_i1": 1.1}})
        inf = episode.get_infos(-1, env_steps=False, agent_ids=["agent_2"])
        check(inf, {"agent_2": {"a2_i2": 2.2}})
        inf = episode.get_infos(-1, env_steps=False, agent_ids=("agent_3",))
        check(inf, {"agent_3": {"a3_i1": 3.1}})
        inf = episode.get_infos(-1, env_steps=False, agent_ids={"agent_4"})
        check(inf, {"agent_4": {"a4_i2": 4.2}})
        inf = episode.get_infos(-1, env_steps=False, agent_ids=["agent_1", "agent_2"])
        check(inf, {"agent_1": {"a1_i1": 1.1}, "agent_2": {"a2_i2": 2.2}})
        inf = episode.get_infos(-2, env_steps=True, agent_ids={"agent_4"})
        check(inf, {"agent_4": {"a4_i1": 4.1}})
        inf = episode.get_infos([-1, -2], env_steps=True, agent_ids={"agent_4"})
        check(inf, {"agent_4": [{"a4_i2": 4.2}, {"a4_i1": 4.1}]})

        # Return the last two infos for each individual agent.
        inf = episode.get_infos(indices=[-1, -2], env_steps=False)
        check(
            inf,
            {
                "agent_1": [{"a1_i1": 1.1}, {"a1_i0": 1}],
                "agent_2": [{"a2_i2": 2.2}, {"a2_i0": 2}],
                "agent_3": [{"a3_i1": 3.1}, {"a3_i0": 3}],
                "agent_4": [{"a4_i2": 4.2}, {"a4_i1": 4.1}],
            },
        )

        # Now, test the same when returning a list.
        inf = episode.get_infos(return_list=True)
        check(inf, [{"agent_2": {"a2_i2": 2.2}, "agent_4": {"a4_i2": 4.2}}])
        # Expect error when calling with env_steps=False.
        with self.assertRaises(ValueError):
            episode.get_infos(env_steps=False, return_list=True)
        # List of indices.
        inf = episode.get_infos(indices=[-1, -2], return_list=True)
        check(
            inf,
            [
                {"agent_2": {"a2_i2": 2.2}, "agent_4": {"a4_i2": 4.2}},
                {
                    "agent_1": {"a1_i1": 1.1},
                    "agent_3": {"a3_i1": 3.1},
                    "agent_4": {"a4_i1": 4.1},
                },
            ],
        )
        # Slice of indices w/ fill.
        inf = episode.get_infos(
            slice(-1, 1),
            return_list=True,
            fill=-8,
            neg_indices_left_of_zero=True,
        )
        check(
            inf,
            [
                {
                    "agent_1": {"a1_i1": 1.1},
                    "agent_2": -8,
                    "agent_3": {"a3_i1": 3.1},
                    "agent_4": {"a4_i1": 4.1},
                },
                {
                    "agent_1": -8,
                    "agent_2": {"a2_i2": 2.2},
                    "agent_3": -8,
                    "agent_4": {"a4_i2": 4.2},
                },
            ],
        )

        # B/c we have lookback="auto" in the ma episode, all data we sent into
        # the c"tor was pushed into the lookback buffers and only the last
        # infos are outside these buffers and will be returned here.
        inf = episode.get_infos(env_steps=False)
        check(
            inf,
            {
                "agent_1": [{"a1_i1": 1.1}],
                "agent_2": [{"a2_i2": 2.2}],
                "agent_3": [{"a3_i1": 3.1}],
                "agent_4": [{"a4_i2": 4.2}],
            },
        )

        # Test with initial infos only.
        episode = MultiAgentEpisode()
        episode.add_env_reset(
            observations=observations[0],
            infos=infos[0],
        )
        # Get the last infos for agents and assert that they are correct.
        inf = episode.get_infos()
        for agent_id, agent_inf in infos[0].items():
            check(inf[agent_id][0], agent_inf)
        # Now the same as list.
        inf = episode.get_infos(return_list=True)
        for agent_id, agent_inf in infos[0].items():
            check(inf[0][agent_id], agent_inf)
        # Now by agent steps.
        inf = episode.get_infos(env_steps=False)
        for agent_id, agent_inf in infos[0].items():
            check(inf[agent_id][0], agent_inf)

    def test_get_actions(self):
        # Generate a simple multi-agent episode.
        observations = [
            {"a0": 0, "a1": 0},
            {"a0": 1, "a1": 1},
            {"a1": 2},
            {"a1": 3},
            {"a1": 4},
        ]
        actions = [{"a0": 0, "a1": 0}, {"a0": 1, "a1": 1}, {"a1": 2}, {"a1": 3}]
        rewards = [{"a0": 1, "a1": 1}, {"a0": 2, "a1": 2}, {"a1": 3}, {"a1": 4}]
        episode = MultiAgentEpisode(
            observations=observations, actions=actions, rewards=rewards
        )
        # Access single indices, env steps.
        for i in range(-1, -5, -1):
            act = episode.get_actions(i)
            check(act, actions[i])
        # Access list of indices, env steps.
        act = episode.get_actions([-1, -2])
        check(act, {"a0": [], "a1": [3, 2]})
        act = episode.get_actions([-2, -3])
        check(act, {"a0": [1], "a1": [2, 1]})
        act = episode.get_actions([-3, -4])
        check(act, {"a0": [1, 0], "a1": [1, 0]})
        # Access slices of indices, env steps.
        act = episode.get_actions(slice(-1, -3, -1))
        check(act, {"a0": [], "a1": [3, 2]})
        act = episode.get_actions(slice(-2, -4, -1))
        check(act, {"a0": [1], "a1": [2, 1]})
        act = episode.get_actions(slice(-3, -5, -1))
        check(act, {"a0": [1, 0], "a1": [1, 0]})
        act = episode.get_actions(slice(-3, -6, -1), fill="skip")
        check(act, {"a0": [1, 0, "skip"], "a1": [1, 0, "skip"]})
        act = episode.get_actions(slice(1, -6, -1), fill="s")
        check(
            act,
            {"a0": ["s", "s", "s", "s", 1, 0, "s"], "a1": ["s", "s", 3, 2, 1, 0, "s"]},
        )
        act = episode.get_actions(slice(0, -5, -1), fill="s")
        check(
            act,
            {"a0": ["s", "s", "s", 1, 0], "a1": ["s", 3, 2, 1, 0]},
        )
        # Access single indices, agent steps.
        act = episode.get_actions(-1, env_steps=False)
        check(act, {"a0": 1, "a1": 3})
        act = episode.get_actions(-2, env_steps=False)
        check(act, {"a0": 0, "a1": 2})
        act = episode.get_actions(-3, env_steps=False, agent_ids="a1")
        check(act, {"a1": 1})
        act = episode.get_actions(-3, env_steps=False, fill="skip")
        check(act, {"a0": "skip", "a1": 1})
        act = episode.get_actions(-4, env_steps=False, agent_ids="a1")
        check(act, {"a1": 0})
        act = episode.get_actions(-4, env_steps=False, fill="skip")
        check(act, {"a0": "skip", "a1": 0})

        # Generate simple records for a multi agent environment.
        (
            observations,
            actions,
            rewards,
            is_terminateds,
            is_truncateds,
            infos,
        ) = self._mock_multi_agent_records()
        # Create a multi-agent episode.
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=is_terminateds,
            truncateds=is_truncateds,
            len_lookback_buffer="auto",  # default: use all data as lookback
        )

        # Get last actions for the multi-agent episode.
        act = episode.get_actions(indices=-1)
        check(act, {"agent_1": 1, "agent_3": 1, "agent_4": 1})

        # Return last two actions for the entire env.
        # Also, we flip the indices here and require -1 before -2, this
        # should reflect in the returned results.
        act = episode.get_actions(indices=[-1, -2])
        check(
            act,
            {"agent_1": [1, 0], "agent_2": [0], "agent_3": [1, 0], "agent_4": [1]},
        )

        # Return last two actions for the entire env using slice.
        act = episode.get_actions(slice(-2, None))
        check(
            act,
            {"agent_1": [0, 1], "agent_2": [0], "agent_3": [0, 1], "agent_4": [1]},
        )

        # Return last four actions for the entire env using slice
        # and `fill`.
        act = episode.get_actions(slice(-5, None), fill=-10)
        check(
            act,
            {
                # All first three ts should be 0s (fill before episode even
                # started).
                # 4th items are the 1st actions (after reset obs) for agents
                "agent_1": [-10, -10, -10, 0, 1],  # ag1 stepped the first 2 ts
                "agent_2": [-10, -10, -10, 0, -10],  # ag2 stepped first and last ts
                "agent_3": [-10, -10, -10, 0, 1],  # ag3 same as ag1
                "agent_4": [-10, -10, -10, -10, 1],  # ag4 steps in last 2 ts
            },
        )

        # Use `fill` to look into the future (ts=100 and 101).
        act = episode.get_actions(slice(100, 102), fill=9.9)
        check(
            act,
            {
                "agent_1": [9.9, 9.9],
                "agent_2": [9.9, 9.9],
                "agent_3": [9.9, 9.9],
                "agent_4": [9.9, 9.9],
            },
        )

        # Return two actions in lookback buffers for the entire env using
        # `neg_indices_left_of_zero=True` and an index list.
        # w/ fill
        act = episode.get_actions(
            indices=[-2, -1],
            fill=-10,
            neg_indices_left_of_zero=True,
        )
        check(
            act,
            {
                "agent_1": [-10, 0],
                "agent_2": [-10, 0],
                "agent_3": [-10, 0],
                "agent_4": [-10, -10],
            },
        )
        # Same, but w/o fill (should produce error as the lookback is only 1 long).
        with self.assertRaises(IndexError):
            episode.get_actions(indices=[-2, -1], neg_indices_left_of_zero=True)

        # Get last actions for each individual agent.
        act = episode.get_actions(indices=-1, env_steps=False)
        check(act, {"agent_1": 1, "agent_2": 0, "agent_3": 1, "agent_4": 1})

        # Same, but with `agent_ids` filters.
        act = episode.get_actions(-1, env_steps=False, agent_ids="agent_1")
        check(act, {"agent_1": 1})
        act = episode.get_actions(-1, env_steps=False, agent_ids=["agent_2"])
        check(act, {"agent_2": 0})
        act = episode.get_actions(-1, env_steps=False, agent_ids=("agent_3",))
        check(act, {"agent_3": 1})
        act = episode.get_actions(-1, env_steps=False, agent_ids={"agent_4"})
        check(act, {"agent_4": 1})
        act = episode.get_actions(-1, env_steps=False, agent_ids=["agent_1", "agent_2"])
        check(act, {"agent_1": 1, "agent_2": 0})
        act = episode.get_actions(-2, env_steps=True, agent_ids={"agent_4"})
        check(act, {"agent_4": 1})
        act = episode.get_actions([-1, -2], env_steps=True, agent_ids={"agent_4"})
        check(act, {"agent_4": [1]})
        # Agent 4 has only acted 2x, so there is no (local) ts=-2 for it.
        with self.assertRaises(IndexError):
            episode.get_actions([-1, -2], env_steps=False, agent_ids={"agent_4"})
        act = episode.get_actions([-2], env_steps=False, agent_ids="agent_4", fill=-10)
        check(act, {"agent_4": [-10]})

        # Now, test the same when returning a list.
        # B/c we have lookback="auto" in the ma episode, all data we sent into
        # the c"tor was pushed into the lookback buffers and thus all
        # actions are in these buffers (and won't get returned here).
        act = episode.get_actions(return_list=True)
        self.assertTrue(act == [])
        # Expect error when calling with env_steps=False.
        with self.assertRaises(ValueError):
            episode.get_actions(env_steps=False, return_list=True)
        # List of indices.
        act = episode.get_actions(indices=[-1, -2], return_list=True)
        check(act, [actions[-1], actions[-2]])
        # Slice of indices w/ fill.
        # From the last ts in lookback buffer to first actual ts (empty as all data is
        # in lookback buffer, but we fill).
        act = episode.get_actions(
            slice(-1, 1), return_list=True, fill=-8, neg_indices_left_of_zero=True
        )
        check(
            act,
            [
                {"agent_1": 1, "agent_2": -8, "agent_3": 1, "agent_4": 1},
                {"agent_1": -8, "agent_2": -8, "agent_3": -8, "agent_4": -8},
            ],
        )

        # B/c we have lookback="auto" in the ma episode, all data we sent into
        # the c"tor was pushed into the lookback buffers and thus all
        # actions are in these buffers.
        act = episode.get_actions(env_steps=False)
        self.assertTrue(act == {})

        # Test with initial actions only.
        episode = MultiAgentEpisode()
        episode.add_env_reset(observations=observations[0], infos=infos[0])
        # Get the last action for agents and assert that it's correct.
        act = episode.get_actions()
        check(act, {})
        # Now the same as list.
        act = episode.get_actions(return_list=True)
        self.assertTrue(act == [])
        # Now agent steps.
        act = episode.get_actions(env_steps=False)
        self.assertTrue(act == {})

    def test_get_rewards(self):
        # Generate a simple multi-agent episode.
        observations = [
            {"a0": 0, "a1": 0},
            {"a0": 1, "a1": 1},
            {"a1": 2},
            {"a1": 3},
            {"a1": 4},
        ]
        actions = [{"a0": 0, "a1": 0}, {"a0": 1, "a1": 1}, {"a1": 2}, {"a1": 3}]
        rewards = [
            {"a0": 0.0, "a1": 0.0},
            {"a0": 1.0, "a1": 1.0},
            {"a1": 2.0},
            {"a1": 3.0},
        ]
        episode = MultiAgentEpisode(
            observations=observations, actions=actions, rewards=rewards
        )
        # Access single indices, env steps.
        for i in range(-1, -5, -1):
            rew = episode.get_rewards(i)
            check(rew, rewards[i])
        # Access list of indices, env steps.
        rew = episode.get_rewards([-1, -2])
        check(rew, {"a0": [], "a1": [3, 2]})
        rew = episode.get_rewards([-2, -3])
        check(rew, {"a0": [1], "a1": [2, 1]})
        rew = episode.get_rewards([-3, -4])
        check(rew, {"a0": [1, 0], "a1": [1, 0]})
        # Access slices of indices, env steps.
        rew = episode.get_rewards(slice(-1, -3, -1))
        check(rew, {"a0": [], "a1": [3, 2]})
        rew = episode.get_rewards(slice(-2, -4, -1))
        check(rew, {"a0": [1], "a1": [2, 1]})
        rew = episode.get_rewards(slice(-3, -5, -1))
        check(rew, {"a0": [1, 0], "a1": [1, 0]})
        rew = episode.get_rewards(slice(-3, -6, -1), fill=-10.0)
        check(rew, {"a0": [1, 0, -10.0], "a1": [1, 0, -10.0]})
        rew = episode.get_rewards(slice(1, -6, -1), fill=-1)
        check(
            rew,
            {"a0": [-1, -1, -1, -1, 1, 0, -1], "a1": [-1, -1, 3, 2, 1, 0, -1]},
        )
        rew = episode.get_rewards(slice(0, -5, -1), fill=-2)
        check(
            rew,
            {"a0": [-2, -2, -2, 1, 0], "a1": [-2, 3, 2, 1, 0]},
        )
        # Access single indices, agent steps.
        rew = episode.get_rewards(-1, env_steps=False)
        check(rew, {"a0": 1, "a1": 3})
        rew = episode.get_rewards(-2, env_steps=False)
        check(rew, {"a0": 0, "a1": 2})
        rew = episode.get_rewards(-3, env_steps=False, agent_ids="a1")
        check(rew, {"a1": 1})
        rew = episode.get_rewards(-3, env_steps=False, fill=-4)
        check(rew, {"a0": -4, "a1": 1})
        rew = episode.get_rewards(-4, env_steps=False, agent_ids="a1")
        check(rew, {"a1": 0})
        rew = episode.get_rewards(-4, env_steps=False, fill=-5)
        check(rew, {"a0": -5, "a1": 0})

        # Generate simple records for a multi agent environment.
        (
            observations,
            actions,
            rewards,
            is_terminateds,
            is_truncateds,
            infos,
        ) = self._mock_multi_agent_records()
        # Create a multi-agent episode.
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=is_terminateds,
            truncateds=is_truncateds,
            len_lookback_buffer="auto",  # default: use all data as lookback
        )

        # Get last rewards for the multi-agent episode.
        rew = episode.get_rewards(indices=-1)
        check(rew, {"agent_1": 1.1, "agent_3": 1.2, "agent_4": 1.3})

        # Return last two rewards for the entire env.
        # Also, we flip the indices here and require -1 before -2, this
        # should reflect in the returned results.
        rew = episode.get_rewards(indices=[-1, -2])
        check(
            rew,
            {
                "agent_1": [1.1, 0.5],
                "agent_2": [0.6],
                "agent_3": [1.2, 0.7],
                "agent_4": [1.3],
            },
        )

        # Return last two rewards for the entire env using slice.
        rew = episode.get_rewards(slice(-2, None))
        check(
            rew,
            {
                "agent_1": [0.5, 1.1],
                "agent_2": [0.6],
                "agent_3": [0.7, 1.2],
                "agent_4": [1.3],
            },
        )

        # Return last four rewards for the entire env using slice
        # and `fill`.
        rew = episode.get_rewards(slice(-5, None), fill=-10)
        check(
            rew,
            {
                # All first three ts should be 0s (fill before episode even
                # started).
                # 4th items are the 1st rewards (after reset obs) for agents
                "agent_1": [-10, -10, -10, 0.5, 1.1],  # ag1 stepped the first 2 ts
                "agent_2": [-10, -10, -10, 0.6, -10],  # ag2 stepped first ts
                "agent_3": [-10, -10, -10, 0.7, 1.2],  # ag3 same as ag1
                "agent_4": [-10, -10, -10, -10, 1.3],  # ag4 steps in last 2 ts
            },
        )

        # Use `fill` to look into the future (ts=100 and 101).
        rew = episode.get_rewards(slice(100, 102), fill=9.9)
        check(
            rew,
            {
                "agent_1": [9.9, 9.9],
                "agent_2": [9.9, 9.9],
                "agent_3": [9.9, 9.9],
                "agent_4": [9.9, 9.9],
            },
        )

        # Return two rewards in lookback buffers for the entire env using
        # `neg_indices_left_of_zero=True` and an index list.
        # w/ fill
        rew = episode.get_rewards(
            indices=[-2, -1],
            fill=-10,
            neg_indices_left_of_zero=True,
        )
        check(
            rew,
            {
                "agent_1": [-10, 0.5],
                "agent_2": [-10, 0.6],
                "agent_3": [-10, 0.7],
                "agent_4": [-10, -10],
            },
        )
        # Same, but w/o fill (should produce error as the lookback is only 1 long).
        with self.assertRaises(IndexError):
            episode.get_rewards(indices=[-2, -1], neg_indices_left_of_zero=True)

        # Get last rewards for each individual agent.
        rew = episode.get_rewards(indices=-1, env_steps=False)
        check(rew, {"agent_1": 1.1, "agent_2": 0.6, "agent_3": 1.2, "agent_4": 1.3})

        # Same, but with `agent_ids` filters.
        rew = episode.get_rewards(-1, env_steps=False, agent_ids="agent_1")
        check(rew, {"agent_1": 1.1})
        rew = episode.get_rewards(-1, env_steps=False, agent_ids=["agent_2"])
        check(rew, {"agent_2": 0.6})
        rew = episode.get_rewards(-1, env_steps=False, agent_ids=("agent_3",))
        check(rew, {"agent_3": 1.2})
        rew = episode.get_rewards(-1, env_steps=False, agent_ids={"agent_4"})
        check(rew, {"agent_4": 1.3})
        rew = episode.get_rewards(-1, env_steps=False, agent_ids=["agent_1", "agent_2"])
        check(rew, {"agent_1": 1.1, "agent_2": 0.6})
        rew = episode.get_rewards(-2, env_steps=True, agent_ids={"agent_3"})
        check(rew, {"agent_3": 0.7})
        rew = episode.get_rewards(-2, env_steps=True, agent_ids={"agent_4"})
        check(rew, {})
        rew = episode.get_rewards([-1, -2], env_steps=True, agent_ids={"agent_3"})
        check(rew, {"agent_3": [1.2, 0.7]})
        rew = episode.get_rewards([-1, -2], env_steps=True, agent_ids={"agent_4"})
        check(rew, {"agent_4": [1.3]})

        # Agent 4 has only acted 2x, so there is no (local) ts=-2 for it.
        with self.assertRaises(IndexError):
            episode.get_rewards([-1, -2], env_steps=False, agent_ids={"agent_4"})
        rew = episode.get_rewards([-2], env_steps=False, agent_ids="agent_4", fill=-10)
        check(rew, {"agent_4": [-10]})

        # Now, test the same when returning a list.
        # B/c we have lookback="auto" in the ma episode, all data we sent into
        # the c"tor was pushed into the lookback buffers and thus all
        # rewards are in these buffers (and won't get returned here).
        rew = episode.get_rewards(return_list=True)
        self.assertTrue(rew == [])
        # Expect error when calling with combination of env_steps=False, but
        # return_list=True.
        with self.assertRaises(ValueError):
            episode.get_rewards(env_steps=False, return_list=True)
        # List of indices.
        rew = episode.get_rewards(indices=[-1, -2], return_list=True)
        check(rew, [rewards[-1], rewards[-2]])
        # Slice of indices w/ fill.
        # From the last ts in lookback buffer to first actual ts (empty as all data is
        # in lookback buffer).
        rew = episode.get_rewards(
            slice(-1, 1),
            return_list=True,
            fill=-8,
            neg_indices_left_of_zero=True,
        )
        check(
            rew,
            [
                {"agent_1": 1.1, "agent_2": -8, "agent_3": 1.2, "agent_4": 1.3},
                {"agent_1": -8, "agent_2": -8, "agent_3": -8, "agent_4": -8},
            ],
        )

        # B/c we have lookback="auto" in the ma episode, all data we sent into
        # the c"tor was pushed into the lookback buffers and thus all
        # rewards are in these buffers.
        rew = episode.get_rewards(env_steps=False)
        self.assertTrue(rew == {})

        # Test with initial rewards only.
        episode = MultiAgentEpisode()
        episode.add_env_reset(observations=observations[0], infos=infos[0])
        # Get the last action for agents and assert that it's correct.
        rew = episode.get_rewards()
        check(rew, {})
        # Now the same as list.
        rew = episode.get_rewards(return_list=True)
        self.assertTrue(rew == [])
        # Now agent steps.
        rew = episode.get_rewards(env_steps=False)
        self.assertTrue(rew == {})

    def test_other_getters(self):
        # TODO (simon): Revisit this test and the MultiAgentEpisode.episode_concat API.
        return

        (
            observations,
            actions,
            rewards,
            is_terminateds,
            is_truncateds,
            infos,
        ) = self._mock_multi_agent_records()
        # Define some extra model outputs.
        extra_model_outputs = [
            # Here agent_2 has to buffer.
            {"agent_1": {"extra": 0}, "agent_2": {"extra": 0}, "agent_3": {"extra": 0}},
            {"agent_1": {"extra": 1}, "agent_3": {"extra": 1}, "agent_4": {"extra": 1}},
        ]

        # Create a multi-agent episode.
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            infos=infos,
            terminateds=is_terminateds,
            truncateds=is_truncateds,
            # extra_model_outputs=extra_model_outputs,
            # len_lookback_buffer=0,
        )

        # --- extra_model_outputs ---
        last_extra_model_outputs = episode.get_extra_model_outputs("extra")
        check(
            last_extra_model_outputs["agent_1"][0],
            extra_model_outputs[-1]["agent_1"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_3"][0],
            extra_model_outputs[-1]["agent_3"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_4"][0],
            extra_model_outputs[-1]["agent_4"]["extra"],
        )

        # Request the last two outputs.
        last_extra_model_outputs = episode.get_extra_model_outputs(
            "extra", indices=[-1, -2]
        )
        check(
            last_extra_model_outputs["agent_1"][0],
            extra_model_outputs[-1]["agent_1"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_3"][0],
            extra_model_outputs[-1]["agent_3"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_4"][0],
            extra_model_outputs[-1]["agent_4"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_1"][1],
            extra_model_outputs[-2]["agent_1"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_2"][0],
            extra_model_outputs[-2]["agent_2"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_3"][1],
            extra_model_outputs[-2]["agent_3"]["extra"],
        )

        # Now request lists.
        # last_extra_model_outputs = episode.get_extra_model_outputs(
        #    "extra", as_list=True
        # )
        # check(
        #    last_extra_model_outputs[0]["agent_1"],
        #    extra_model_outputs[-1]["agent_1"]["extra"],
        # )
        # check(
        #    last_extra_model_outputs[0]["agent_3"],
        #    extra_model_outputs[-1]["agent_3"]["extra"],
        # )
        # check(
        #    last_extra_model_outputs[0]["agent_4"],
        #    extra_model_outputs[-1]["agent_4"]["extra"],
        # )
        # Request the last two extra model outputs and return as a list.
        # last_extra_model_outputs = episode.get_extra_model_outputs(
        #    "extra", [-1, -2], as_list=True
        # )
        # check(
        #    last_extra_model_outputs[0]["agent_1"],
        #    extra_model_outputs[-1]["agent_1"]["extra"],
        # )
        # check(
        #    last_extra_model_outputs[0]["agent_3"],
        #    extra_model_outputs[-1]["agent_3"]["extra"],
        # )
        # check(
        #    last_extra_model_outputs[0]["agent_4"],
        #    extra_model_outputs[-1]["agent_4"]["extra"],
        # )
        # check(
        #    last_extra_model_outputs[1]["agent_1"],
        #    extra_model_outputs[-2]["agent_1"]["extra"],
        # )
        # check(
        #    last_extra_model_outputs[1]["agent_2"],
        #    extra_model_outputs[-2]["agent_2"]["extra"],
        # )
        # check(
        #    last_extra_model_outputs[1]["agent_3"],
        #    extra_model_outputs[-2]["agent_3"]["extra"],
        # )

        # Now request the last extra model outputs at the local timesteps, i.e.
        # for each agent its last two actions.
        last_extra_model_outputs = episode.get_extra_model_outputs(
            "extra", [-1, -2], env_steps=False
        )
        check(
            last_extra_model_outputs["agent_1"][0],
            extra_model_outputs[-1]["agent_1"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_3"][0],
            extra_model_outputs[-1]["agent_3"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_4"][0],
            extra_model_outputs[-1]["agent_4"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_1"][1],
            extra_model_outputs[-2]["agent_1"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_2"][0],
            extra_model_outputs[-2]["agent_2"]["extra"],
        )
        check(
            last_extra_model_outputs["agent_3"][1],
            extra_model_outputs[-2]["agent_3"]["extra"],
        )

        # TODO (simon): Not tested with `env_steps=False`.
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
        episode_1 = MultiAgentEpisode(agent_ids=env.agent_ids)

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
        check(episode_1.global_t_to_local_t["agent_9"][-1], 100)
        check(episode_1.agent_episodes["agent_9"].rewards[-1], 1.0)
        check(last_rewards["agent_9"][0], 1.0)
        self.assertIn("agent_0", last_rewards)
        check(episode_1.global_t_to_local_t["agent_0"][-1], 100)
        check(episode_1.agent_episodes["agent_0"].rewards[-1], 1.0)
        check(last_rewards["agent_0"][0], 1.0)
        self.assertIn("agent_2", last_rewards)
        check(episode_1.global_t_to_local_t["agent_2"][-1], 100)
        check(episode_1.agent_episodes["agent_2"].rewards[-1], 1.0)
        check(last_rewards["agent_2"][0], 1.0)
        self.assertIn("agent_5", last_rewards)
        check(episode_1.global_t_to_local_t["agent_5"][-1], 100)
        check(episode_1.agent_episodes["agent_5"].rewards[-1], 1.0)
        check(last_rewards["agent_5"][0], 1.0)
        self.assertIn("agent_8", last_rewards)
        check(episode_1.global_t_to_local_t["agent_8"][-1], 100)
        check(episode_1.agent_episodes["agent_8"].rewards[-1], 1.0)
        check(last_rewards["agent_8"][0], 1.0)
        self.assertIn("agent_4", last_rewards)
        check(episode_1.global_t_to_local_t["agent_4"][-1], 100)
        check(episode_1.agent_episodes["agent_4"].rewards[-1], 1.0)
        check(last_rewards["agent_4"][0], 1.0)
        self.assertIn("agent_3", last_rewards)
        check(episode_1.global_t_to_local_t["agent_3"][-1], 100)
        # Agent 3 had a partial reward before the last recorded observation.
        check(episode_1.agent_episodes["agent_3"].rewards[-1], 2.0)
        check(last_rewards["agent_3"][0], 2.0)
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)
        self.assertNotIn("agent_6", last_rewards)
        self.assertNotIn("agent_7", last_rewards)

        # Now return the same as list.
        last_rewards = episode_1.get_rewards(
            partial=False, consider_buffer=False, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        check(last_rewards[0]["agent_9"], 1.0)
        self.assertIn("agent_0", last_rewards[0])
        check(last_rewards[0]["agent_0"], 1.0)
        self.assertIn("agent_2", last_rewards[0])
        check(last_rewards[0]["agent_2"], 1.0)
        self.assertIn("agent_5", last_rewards[0])
        check(last_rewards[0]["agent_5"], 1.0)
        self.assertIn("agent_8", last_rewards[0])
        check(last_rewards[0]["agent_8"], 1.0)
        self.assertIn("agent_4", last_rewards[0])
        check(last_rewards[0]["agent_4"], 1.0)
        self.assertIn("agent_3", last_rewards[0])
        check(last_rewards[0]["agent_3"], 2.0)
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
            episode_1.agent_episodes["agent_5"].rewards[-2:],
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
        check(
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
        check(
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
        check(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards[0]["agent_9"]
        )
        check(
            episode_1.agent_episodes["agent_9"].rewards[-2], last_rewards[1]["agent_9"]
        )
        self.assertIn("agent_5", last_rewards[0])
        self.assertIn("agent_5", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards[0]["agent_5"]
        )
        check(
            episode_1.agent_episodes["agent_5"].rewards[-2], last_rewards[1]["agent_5"]
        )
        self.assertIn("agent_2", last_rewards[0])
        self.assertIn("agent_2", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards[0]["agent_2"]
        )
        check(3.0, last_rewards[1]["agent_2"])
        # Agent 3 has only recorded rewards at `ts=100`.
        self.assertIn("agent_3", last_rewards[0])
        check(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards[0]["agent_3"]
        )
        self.assertIn("agent_4", last_rewards[0])
        self.assertIn("agent_4", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards[0]["agent_4"]
        )
        check(
            episode_1.agent_episodes["agent_4"].rewards[-2], last_rewards[1]["agent_4"]
        )
        self.assertIn("agent_8", last_rewards[0])
        self.assertIn("agent_8", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards[0]["agent_8"]
        )
        check(
            episode_1.agent_episodes["agent_8"].rewards[-2], last_rewards[1]["agent_8"]
        )
        # Agent 7 has no observation at `ts=100`.
        self.assertIn("agent_7", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_7"].rewards[-1], last_rewards[1]["agent_7"]
        )
        self.assertIn("agent_0", last_rewards[0])
        self.assertIn("agent_0", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards[0]["agent_0"]
        )
        check(
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
        check(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards["agent_9"][0]
        )
        self.assertIn("agent_0", last_rewards)
        check(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards["agent_0"][0]
        )
        self.assertIn("agent_2", last_rewards)
        check(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards["agent_2"][0]
        )
        self.assertIn("agent_5", last_rewards)
        check(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards["agent_5"][0]
        )
        self.assertIn("agent_8", last_rewards)
        check(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards["agent_8"][0]
        )
        self.assertIn("agent_4", last_rewards)
        check(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards["agent_4"][0]
        )
        self.assertIn("agent_3", last_rewards)
        # Agent 3 had a partial reward before the last recorded observation.
        check(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards["agent_3"][0]
        )
        # Agent 7 has a partial reward at `ts=100` after its last observation at
        # `ts=99`.
        self.assertIn("agent_7", last_rewards)
        check(episode_1.partial_rewards_t["agent_7"][-1], 100)
        check(episode_1.partial_rewards["agent_7"][-1], last_rewards["agent_7"][0])
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)
        self.assertNotIn("agent_6", last_rewards)

        # Now request the last rewards as a list while considering the buffer.
        last_rewards = episode_1.get_rewards(
            partial=False, consider_buffer=True, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        check(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards[0]["agent_9"]
        )
        self.assertIn("agent_0", last_rewards[0])
        check(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards[0]["agent_0"]
        )
        self.assertIn("agent_2", last_rewards[0])
        check(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards[0]["agent_2"]
        )
        self.assertIn("agent_5", last_rewards[0])
        check(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards[0]["agent_5"]
        )
        self.assertIn("agent_8", last_rewards[0])
        check(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards[0]["agent_8"]
        )
        self.assertIn("agent_4", last_rewards[0])
        check(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards[0]["agent_4"]
        )
        self.assertIn("agent_3", last_rewards[0])
        # Agent 3 had a partial reward before the last recorded observation.
        check(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards[0]["agent_3"]
        )
        # Agent 7 has a partial reward at `ts=100` after its last observation at
        # `ts=99`.
        self.assertIn("agent_7", last_rewards[0])
        check(episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"])
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
            episode_1.agent_episodes["agent_5"].rewards[-1:-3:-1],
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
        check(
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
        check(episode_1.partial_rewards["agent_7"][-1], last_rewards["agent_7"][0])
        check(
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
        check(
            episode_1.agent_episodes["agent_9"].rewards[-1], last_rewards[0]["agent_9"]
        )
        check(
            episode_1.agent_episodes["agent_9"].rewards[-2], last_rewards[1]["agent_9"]
        )
        self.assertIn("agent_0", last_rewards[0])
        self.assertIn("agent_0", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_0"].rewards[-1], last_rewards[0]["agent_0"]
        )
        check(
            episode_1.agent_episodes["agent_0"].rewards[-2], last_rewards[1]["agent_0"]
        )
        self.assertIn("agent_2", last_rewards[0])
        self.assertIn("agent_2", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_2"].rewards[-1], last_rewards[0]["agent_2"]
        )
        check(
            episode_1.agent_episodes["agent_2"].rewards[-2], last_rewards[1]["agent_2"]
        )
        self.assertIn("agent_5", last_rewards[0])
        self.assertIn("agent_5", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_5"].rewards[-1], last_rewards[0]["agent_5"]
        )
        check(
            episode_1.agent_episodes["agent_5"].rewards[-2], last_rewards[1]["agent_5"]
        )
        self.assertIn("agent_8", last_rewards[0])
        self.assertIn("agent_8", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_8"].rewards[-1], last_rewards[0]["agent_8"]
        )
        check(
            episode_1.agent_episodes["agent_8"].rewards[-2], last_rewards[1]["agent_8"]
        )
        self.assertIn("agent_4", last_rewards[0])
        self.assertIn("agent_4", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_4"].rewards[-1], last_rewards[0]["agent_4"]
        )
        check(
            episode_1.agent_episodes["agent_4"].rewards[-2], last_rewards[1]["agent_4"]
        )
        # Nothing changes for agent 3 as it has an observation at the last requested
        # timestep 100.
        self.assertIn("agent_3", last_rewards[0])
        self.assertNotIn("agent_3", last_rewards[1])
        check(
            episode_1.agent_episodes["agent_3"].rewards[-1], last_rewards[0]["agent_3"]
        )
        # The entries for agent 6 have changed now b/c it has partial rewards during the
        # requested timesteps 100 and 99.
        self.assertIn("agent_6", last_rewards[0])
        self.assertIn("agent_6", last_rewards[1])
        check(episode_1.partial_rewards["agent_6"][-1], last_rewards[0]["agent_6"])
        check(episode_1.partial_rewards["agent_6"][-2], last_rewards[1]["agent_6"])
        # Entries for agent 7 also change b/c this agent has a partial reward at
        # `ts=100` while it has no observation recorded at this timestep.
        self.assertIn("agent_7", last_rewards[0])
        self.assertIn("agent_7", last_rewards[1])
        check(episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"])
        check(
            episode_1.agent_episodes["agent_7"].rewards[-1], last_rewards[1]["agent_7"]
        )
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards[0])
        self.assertNotIn("agent_1", last_rewards[1])

        # Third, request only partial rewards, i.e. rewards do not get buffered and
        # added up.
        last_rewards = episode_1.get_rewards(partial=True, consider_buffer=False)
        self.assertIn("agent_9", last_rewards)
        check(episode_1.partial_rewards_t["agent_9"][-1], 100)
        check(episode_1.partial_rewards["agent_9"][-1], last_rewards["agent_9"][-1])
        self.assertIn("agent_0", last_rewards)
        check(episode_1.partial_rewards_t["agent_0"][-1], 100)
        check(episode_1.partial_rewards["agent_0"][-1], last_rewards["agent_0"][-1])
        self.assertIn("agent_2", last_rewards)
        check(episode_1.partial_rewards_t["agent_2"][-1], 100)
        check(episode_1.partial_rewards["agent_2"][-1], last_rewards["agent_2"][-1])
        self.assertIn("agent_8", last_rewards)
        check(episode_1.partial_rewards_t["agent_8"][-1], 100)
        check(episode_1.partial_rewards["agent_8"][-1], last_rewards["agent_8"][-1])
        self.assertIn("agent_4", last_rewards)
        check(episode_1.partial_rewards_t["agent_4"][-1], 100)
        check(episode_1.partial_rewards["agent_4"][-1], last_rewards["agent_4"][-1])
        self.assertIn("agent_3", last_rewards)
        check(episode_1.partial_rewards_t["agent_3"][-1], 100)
        check(episode_1.partial_rewards["agent_3"][-1], last_rewards["agent_3"][-1])
        self.assertIn("agent_6", last_rewards)
        check(episode_1.partial_rewards_t["agent_6"][-1], 100)
        check(episode_1.partial_rewards["agent_6"][-1], last_rewards["agent_6"][-1])
        self.assertIn("agent_7", last_rewards)
        check(episode_1.partial_rewards_t["agent_7"][-1], 100)
        check(episode_1.partial_rewards["agent_7"][-1], last_rewards["agent_7"][-1])
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)

        # Now request all partial rewards at the last timestep and return them as
        # a list.
        last_rewards = episode_1.get_rewards(
            partial=True, consider_buffer=False, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        check(episode_1.partial_rewards["agent_9"][-1], last_rewards[0]["agent_9"])
        self.assertIn("agent_0", last_rewards[0])
        check(episode_1.partial_rewards["agent_0"][-1], last_rewards[0]["agent_0"])
        self.assertIn("agent_2", last_rewards[0])
        check(episode_1.partial_rewards["agent_2"][-1], last_rewards[0]["agent_2"])
        self.assertIn("agent_8", last_rewards[0])
        check(episode_1.partial_rewards["agent_8"][-1], last_rewards[0]["agent_8"])
        self.assertIn("agent_4", last_rewards[0])
        check(episode_1.partial_rewards["agent_4"][-1], last_rewards[0]["agent_4"])
        self.assertIn("agent_3", last_rewards[0])
        check(episode_1.partial_rewards["agent_3"][-1], last_rewards[0]["agent_3"])
        self.assertIn("agent_6", last_rewards[0])
        check(episode_1.partial_rewards["agent_6"][-1], last_rewards[0]["agent_6"])
        self.assertIn("agent_7", last_rewards[0])
        check(episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"])
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards[0])

        # Request the last two indices, but consider only partial rewards.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=True, consider_buffer=False
        )
        self.assertIn("agent_9", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_9"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_9"][-1:-3:-1], last_rewards["agent_9"])
        self.assertIn("agent_0", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_0"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_0"][-1:-3:-1], last_rewards["agent_0"])
        self.assertIn("agent_2", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_2"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_2"][-1:-3:-1], last_rewards["agent_2"])
        self.assertIn("agent_8", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_8"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_8"][-1:-3:-1], last_rewards["agent_8"])
        self.assertIn("agent_4", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_4"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_4"][-1:-3:-1], last_rewards["agent_4"])
        self.assertIn("agent_3", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_3"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_3"][-1:-3:-1], last_rewards["agent_3"])
        self.assertIn("agent_6", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_6"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_6"][-1:-3:-1], last_rewards["agent_6"])
        self.assertIn("agent_7", last_rewards)
        self.assertListEqual(episode_1.partial_rewards_t["agent_7"][-2:], [99, 100])
        check(episode_1.partial_rewards["agent_7"][-1:-3:-1], last_rewards["agent_7"])
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards)

        # At last, request the last two indices for only partial rewards and return
        # them as list.
        last_rewards = episode_1.get_rewards(
            [-1, -2], partial=True, consider_buffer=False, as_list=True
        )
        self.assertIn("agent_9", last_rewards[0])
        self.assertIn("agent_9", last_rewards[1])
        check(episode_1.partial_rewards["agent_9"][-1], last_rewards[0]["agent_9"])
        check(episode_1.partial_rewards["agent_9"][-2], last_rewards[1]["agent_9"])
        self.assertIn("agent_0", last_rewards[0])
        self.assertIn("agent_0", last_rewards[1])
        check(episode_1.partial_rewards["agent_0"][-1], last_rewards[0]["agent_0"])
        check(episode_1.partial_rewards["agent_0"][-2], last_rewards[1]["agent_0"])
        self.assertIn("agent_2", last_rewards[0])
        self.assertIn("agent_2", last_rewards[1])
        check(episode_1.partial_rewards["agent_2"][-1], last_rewards[0]["agent_2"])
        check(episode_1.partial_rewards["agent_2"][-2], last_rewards[1]["agent_2"])
        self.assertIn("agent_8", last_rewards[0])
        self.assertIn("agent_8", last_rewards[1])
        check(episode_1.partial_rewards["agent_8"][-1], last_rewards[0]["agent_8"])
        check(episode_1.partial_rewards["agent_8"][-2], last_rewards[1]["agent_8"])
        self.assertIn("agent_4", last_rewards[0])
        self.assertIn("agent_4", last_rewards[1])
        check(episode_1.partial_rewards["agent_4"][-1], last_rewards[0]["agent_4"])
        check(episode_1.partial_rewards["agent_4"][-2], last_rewards[1]["agent_4"])
        self.assertIn("agent_3", last_rewards[0])
        self.assertIn("agent_3", last_rewards[1])
        check(episode_1.partial_rewards["agent_3"][-1], last_rewards[0]["agent_3"])
        check(episode_1.partial_rewards["agent_3"][-2], last_rewards[1]["agent_3"])
        self.assertIn("agent_6", last_rewards[0])
        self.assertIn("agent_6", last_rewards[1])
        check(episode_1.partial_rewards["agent_6"][-1], last_rewards[0]["agent_6"])
        check(episode_1.partial_rewards["agent_6"][-2], last_rewards[1]["agent_6"])
        self.assertIn("agent_7", last_rewards[0])
        self.assertIn("agent_7", last_rewards[1])
        check(episode_1.partial_rewards["agent_7"][-1], last_rewards[0]["agent_7"])
        check(episode_1.partial_rewards["agent_7"][-2], last_rewards[1]["agent_7"])
        # Assert that all the other agents are not in the returned rewards.
        self.assertNotIn("agent_1", last_rewards[0])
        self.assertNotIn("agent_1", last_rewards[1])

        # Now, test with `global_ts=False`, i.e. on local level.
        # Begin with `partial=False` and `consider_buffer=False`

        # --- is_terminated, is_truncated ---

    def test_cut(self):
        # Generate a simple multi-agent episode and check all internals after
        # construction.
        observations = [{"a0": 0, "a1": 0}, {"a1": 1}, {"a1": 2}, {"a1": 3}]
        actions = [{"a0": 0, "a1": 0}, {"a1": 1}, {"a1": 2}]
        rewards = [{"a0": 0.1, "a1": 0.1}, {"a1": 0.2}, {"a1": 0.3}]
        episode_1 = MultiAgentEpisode(
            observations=observations, actions=actions, rewards=rewards
        )

        episode_2 = episode_1.cut()
        check(episode_1.id_, episode_2.id_)
        check(len(episode_1), 0)
        check(len(episode_2), 0)
        # Assert that all `SingleAgentEpisode`s have identical ids.
        for agent_id, agent_eps in episode_2.agent_episodes.items():
            check(agent_eps.id_, episode_1.agent_episodes[agent_id].id_)
        # Assert that the timestep starts at the end of the last episode.
        check(episode_1.env_t_started, 0)
        check(episode_1.env_t, episode_2.env_t_started)
        check(episode_2.env_t_started, episode_2.env_t)
        # Make sure our mappings have been adjusted properly. We expect the mapping for
        # a0 to have this agent's last obs added to the mapping's lookback buffer, such
        # that we can add the buffered action to the new episode without problems.
        check(episode_2.env_t_to_agent_t["a0"].data, [0, "S", "S", "S"])
        check(episode_2.env_t_to_agent_t["a0"].lookback, 3)
        check(episode_2.env_t_to_agent_t["a1"].data, [0, 1, 2, 3])
        check(episode_2.env_t_to_agent_t["a1"].lookback, 3)
        # Check all other internals of the cut episode chunk.
        check(episode_2.agent_episodes["a0"].observations.data, [0])
        check(episode_2.agent_episodes["a0"].observations.lookback, 0)
        check(episode_2.agent_episodes["a0"].actions.data, [])
        check(episode_2.agent_episodes["a0"].actions.lookback, 0)

        # Test getting data from the cut chunk via the getter APIs.
        check(episode_2.get_observations(-1), {"a1": 3})
        check(episode_2.get_observations(-1, env_steps=False), {"a0": 0, "a1": 3})
        check(episode_2.get_observations([-2, -1]), {"a1": [2, 3]})
        check(episode_2.get_observations(slice(-3, None)), {"a1": [1, 2, 3]})
        check(
            episode_2.get_observations(slice(-4, None)), {"a0": [0], "a1": [0, 1, 2, 3]}
        )
        # Episode was just cut -> There can't be any actions in it yet (only in the
        # lookback buffer).
        check(episode_2.get_actions(), {})
        check(episode_2.get_actions(-1), {"a1": 2})
        check(episode_2.get_actions(-2), {"a1": 1})
        check(episode_2.get_actions([-3]), {"a0": [0], "a1": [0]})
        with self.assertRaises(IndexError):
            episode_2.get_actions([-4])
        # Don't expect index error if slice is given.
        check(episode_2.get_actions(slice(-4, -3)), {})

        episode_2.add_env_step(
            actions={"a1": 4},
            rewards={"a1": 0.4},
            observations={"a0": 1, "a1": 4},
        )
        # Check everything again, but this time with the additional timestep taken.
        check(len(episode_2), 1)
        check(episode_2.env_t_to_agent_t["a0"].data, [0, "S", "S", "S", 1])
        check(episode_2.env_t_to_agent_t["a0"].lookback, 3)
        check(episode_2.env_t_to_agent_t["a1"].data, [0, 1, 2, 3, 4])
        check(episode_2.env_t_to_agent_t["a1"].lookback, 3)
        check(episode_2.agent_episodes["a0"].observations.data, [0, 1])
        check(episode_2.agent_episodes["a0"].observations.lookback, 0)
        # Action was "logged" -> Buffer should now be completely empty.
        check(episode_2.agent_episodes["a0"].actions.data, [0])
        check(episode_2._agent_buffered_actions, {})
        check(episode_2.agent_episodes["a0"].actions.lookback, 0)
        check(episode_2.get_observations(-1), {"a0": 1, "a1": 4})
        check(episode_2.get_observations(-1, env_steps=False), {"a0": 1, "a1": 4})
        check(episode_2.get_observations([-2, -1]), {"a0": [1], "a1": [3, 4]})
        check(episode_2.get_observations(slice(-3, None)), {"a0": [1], "a1": [2, 3, 4]})
        check(
            episode_2.get_observations(slice(-4, None)), {"a0": [1], "a1": [1, 2, 3, 4]}
        )
        # Episode was just cut -> There can't be any actions in it yet (only in the
        # lookback buffer).
        check(episode_2.get_actions(), {"a1": [4]})
        check(episode_2.get_actions(-1), {"a1": 4})
        check(episode_2.get_actions(-2), {"a1": 2})
        check(episode_2.get_actions([-3]), {"a1": [1]})
        check(episode_2.get_actions([-4]), {"a0": [0], "a1": [0]})
        with self.assertRaises(IndexError):
            episode_2.get_actions([-5])
        # Don't expect index error if slice is given.
        check(episode_2.get_actions(slice(-5, -4)), {})

        # Create an environment.
        episode_1, _ = self._mock_multi_agent_records_from_env(size=100)

        # Assert that the episode has 100 timesteps.
        check(episode_1.env_t, 100)

        # Create a successor.
        episode_2 = episode_1.cut()
        # Assert that it has the same id.
        check(episode_1.id_, episode_2.id_)
        check(len(episode_1), 100)
        check(len(episode_2), 0)
        # Assert that all `SingleAgentEpisode`s have identical ids.
        for agent_id, agent_eps in episode_2.agent_episodes.items():
            check(agent_eps.id_, episode_1.agent_episodes[agent_id].id_)
        # Assert that the timestep starts at the end of the last episode.
        check(episode_1.env_t_started, 0)
        check(episode_2.env_t, episode_2.env_t_started)
        check(episode_1.env_t, episode_2.env_t_started)

        # TODO (sven): Revisit this test and the MultiAgentEpisode.episode_concat API.
        return

        # Assert that the last observation and info of `episode_1` are the first
        # observation and info of `episode_2`.
        for agent_id, agent_obs in episode_1.get_observations(
            -1, env_steps=False
        ).items():
            # If agents are done only ensure that the `SingleAgentEpisode` does not
            # exist in episode_2.
            if episode_1.agent_episodes[agent_id].is_done:
                self.assertTrue(agent_id not in episode_2.agent_episodes)
            else:
                check(
                    agent_obs,
                    episode_2.get_observations(
                        -1,
                        neg_indices_left_of_zero=True,
                        env_steps=False,
                        agent_ids=agent_id,
                    ),
                )
                agent_infos = episode_1.get_infos(-1, env_steps=False)
                check(
                    agent_infos,
                    episode_2.get_infos(0, agent_ids=agent_id),
                )

        # Now test the buffers.
        for agent_id, agent_buffer in episode_1.agent_buffers.items():
            # Make sure the action buffers are either both full or both empty.
            check(
                agent_buffer["actions"].full(),
                episode_2.agent_buffers[agent_id]["actions"].full(),
            )
            # If the action buffers are full they should share the same value.
            if agent_buffer["actions"].full():
                check(
                    agent_buffer["actions"].queue[0],
                    episode_2.agent_buffers[agent_id]["actions"].queue[0],
                )
            # If the agent is not done, the buffers should be equal in value.
            if not episode_1.agent_episodes[agent_id].is_done:
                # The other buffers have default values, if the agent is not done.
                # Note, reward buffers could be full of partial rewards.
                check(
                    agent_buffer["rewards"].queue[0],
                    episode_2.agent_buffers[agent_id]["rewards"].queue[0],
                )
                # Here we want to know, if they are both different from `None`.
                check(
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
                check(agent_global_ts[0], episode_1.global_t_to_local_t[agent_id][-1])
            # In the other case this mapping should be empty.
            else:
                check(len(agent_global_ts), 0)

        # Assert that the global action timestep mappings match.
        for agent_id, agent_global_ts in episode_2.global_actions_t.items():
            # If an agent is not done, we write over the timestep from its last
            # action.
            if not episode_2.agent_episodes[agent_id].is_done:
                # If a timestep mapping for actions was copied over the last timestep
                # of the üredecessor and the first of the successor must match.
                if agent_global_ts:
                    check(agent_global_ts[0], episode_1.global_actions_t[agent_id][-1])
                # If no action timestep mapping was copied over the last action must
                # have been at or before the last observation in the predecessor.
                else:
                    self.assertGreaterEqual(
                        episode_1.global_t_to_local_t[agent_id][-1],
                        episode_1.global_actions_t[agent_id][-1],
                    )
            # In the other case this mapping should be empty.
            else:
                check(len(agent_global_ts), 0)

        # Assert that the partial reward mappings and histories match.
        for agent_id, agent_global_ts in episode_2.partial_rewards_t.items():
            # Ensure that timestep mapping and history have the same length.
            check(len(agent_global_ts), len(episode_2.partial_rewards[agent_id]))
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
                    check(
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
                    check(len(agent_global_ts), 0)
            # In the other case this mapping should be empty.
            else:
                check(len(agent_global_ts), 0)

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
            check(
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
        check(len(episode_1.partial_rewards_t["agent_5"]), 1)
        check(len(episode_1.partial_rewards["agent_5"]), 1)
        check(len(episode_1.partial_rewards_t["agent_3"]), 2)
        check(len(episode_1.partial_rewards["agent_3"]), 2)
        check(len(episode_1.partial_rewards_t["agent_2"]), 2)
        check(len(episode_1.partial_rewards_t["agent_2"]), 2)
        self.assertListEqual(episode_1.partial_rewards["agent_3"], [0.5, 2.0])
        self.assertListEqual(episode_1.partial_rewards_t["agent_3"], [1, 3])
        self.assertListEqual(episode_1.partial_rewards["agent_2"], [1.0, 2.0])
        self.assertListEqual(episode_1.partial_rewards_t["agent_2"], [2, 3])
        check(len(episode_1.partial_rewards["agent_4"]), 2)
        self.assertListEqual(episode_1.partial_rewards["agent_4"], [0.5, 1.0])
        self.assertListEqual(episode_1.partial_rewards_t["agent_4"], [1, 2])

        # Now check that the reward buffers are full.
        for agent_id in ["agent_3", "agent_5"]:
            check(episode_1.agent_buffers[agent_id]["rewards"].queue[0], 2.0)
            # Check that the reward history is correctly recorded.
            check(episode_1.partial_rewards_t[agent_id][-1], episode_1.t)
            check(episode_1.partial_rewards[agent_id][-1], 2.0)

        # Now create the successor.
        episode_2 = episode_1.cut()

        for agent_id, agent_eps in episode_2.agent_episodes.items():
            if len(agent_eps.observations) > 0:
                # The successor's first observations should be the predecessor's last.
                check(
                    agent_eps.observations[0],
                    episode_1.agent_episodes[agent_id].observations[-1],
                )
                # The successor's first entry in the timestep mapping should be the
                # predecessor's last.
                check(
                    episode_2.global_t_to_local_t[agent_id][
                        -1
                    ],  # + episode_2.t_started,
                    episode_1.global_t_to_local_t[agent_id][-1],
                )
        # Now test that the partial rewards fit.
        for agent_id in ["agent_3", "agent_5"]:
            check(len(episode_2.partial_rewards_t[agent_id]), 1)
            check(episode_2.partial_rewards_t[agent_id][-1], 3)
            check(episode_2.agent_buffers[agent_id]["rewards"].queue[0], 2.0)

        # Assert that agent 3's and 4's action buffers are full.
        self.assertTrue(episode_2.agent_buffers["agent_4"]["actions"].full())
        self.assertTrue(episode_2.agent_buffers["agent_3"]["actions"].full())
        # Also assert that agent 1's action b uffer was emptied with the last
        # observations.
        self.assertTrue(episode_2.agent_buffers["agent_1"]["actions"].empty())

    def test_concat_episode(self):
        # TODO (sven): Revisit this test and the MultiAgentEpisode.episode_concat API.
        return

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
        check(episode_1.is_terminated, episode_2.is_terminated)
        check(episode_1.is_truncated, episode_2.is_truncated)
        # Assert that for all agents the last observation is at the correct timestep
        # and of correct value.
        for agent_id, agent_eps in episode_1.agent_episodes.items():
            # Also ensure that the global timestep mapping is correct.
            # Note, for done agents there was only an empty timestep mapping in
            # `episode_2`.
            if not agent_eps.is_done:
                check(
                    episode_1.global_t_to_local_t[agent_id][-1],
                    episode_2.global_t_to_local_t[agent_id][-1],
                )
                check(
                    agent_eps.observations[-1],
                    episode_2.agent_episodes[agent_id].observations[-1],
                )
                check(
                    agent_eps.actions[-1],
                    episode_2.agent_episodes[agent_id].actions[-1],
                )
                check(
                    agent_eps.rewards[-1],
                    episode_2.agent_episodes[agent_id].rewards[-1],
                )
                check(agent_eps.infos[-1], episode_2.agent_episodes[agent_id].infos[-1])
                # Note, our test environment produces always these extra model outputs.
                check(
                    agent_eps.extra_model_outputs["extra"][-1],
                    episode_2.agent_episodes[agent_id].extra_model_outputs["extra"][-1],
                )

        # Ensure that all global timestep mappings have no duplicates after
        # concatenation and matches the observation lengths.
        for agent_id, agent_map in episode_1.global_t_to_local_t.items():
            check(len(agent_map), len(set(agent_map)))
            check(len(agent_map), len(episode_1.agent_episodes[agent_id].observations))

        # Now assert that all buffers remained the same.
        for agent_id, agent_buffer in episode_1.agent_buffers.items():
            # Make sure the actions buffers are either both full or both empty.
            check(
                agent_buffer["actions"].full(),
                episode_2.agent_buffers[agent_id]["actions"].full(),
            )

            # If the buffer is full ensure `episode_2`'s buffer is identical in value.
            if agent_buffer["actions"].full():
                check(
                    agent_buffer["actions"].queue[0],
                    episode_2.agent_buffers[agent_id]["actions"].queue[0],
                )
            # If the agent is not done, the buffers should be equal in value.
            if not episode_1.agent_episodes[agent_id].is_done:
                check(
                    agent_buffer["rewards"].queue[0],
                    episode_2.agent_buffers[agent_id]["rewards"].queue[0],
                )
                check(
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
            check(len(agent_action_ts_map), len(set(agent_action_ts_map)))
            # Also ensure that the timestep mapping are at least as long as action
            # history.
            self.assertGreaterEqual(
                len(agent_action_ts_map),
                len(episode_1.agent_episodes[agent_id].actions),
            )
            # Done agents might not have these lists in `episode_2`.
            if not episode_1.agent_episodes[agent_id].is_done:
                # Ensure for agents that are not done that the last entry matches.
                check(
                    agent_action_ts_map[-1],
                    episode_2.global_actions_t[agent_id][-1],
                )

        # Ensure that partial reward timestep mapping have no duplicates and that
        # partial rewards match.
        for agent_id, agent_partial_rewards_t in episode_1.partial_rewards_t.items():
            check(len(agent_partial_rewards_t), len(set(agent_partial_rewards_t)))
            # Done agents might not have these lists in `episode_2`.
            if not episode_1.agent_episodes[agent_id].is_done:
                # Ensure for agents that are not done that the last entries match.
                check(
                    agent_partial_rewards_t[-1],
                    episode_2.partial_rewards_t[agent_id][-1],
                )
                check(
                    episode_1.partial_rewards[agent_id][-1],
                    episode_2.partial_rewards[agent_id][-1],
                )
            # Partial reward timestep mappings and partial reward list should have
            # identical length.
            check(
                len(agent_partial_rewards_t), len(episode_1.partial_rewards[agent_id])
            )

        # Assert that the getters work.
        last_observation = episode_1.get_observations()
        # Ensure that the last observation is indeed the test environment's
        # one at that timestep (200).
        for agent_id, agent_obs in last_observation.items():
            check(episode_2.t, agent_obs[0])
            # Assert that the timestep mapping did record the last timestep for this
            # observation.
            check(episode_2.t, episode_1.global_t_to_local_t[agent_id][-1])

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
                check(
                    episode_1.agent_buffers[agent_id]["actions"].queue[0],
                    agent_action[0],
                )
            # In the other case the action was recorded in the `SingleAgentEpisode`
            # of this agent.
            else:
                # Make sure that the correct timestep is recorded.
                check(episode_1.global_actions_t[agent_id][-1], episode_1.t)
                # Ensure that the correct action is recorded.
                check(episode_1.agent_episodes[agent_id].actions[-1], agent_action[0])
            # For any action the value
            check(episode_1.global_actions_t[agent_id][-1], agent_action[0])

        # Get the last reward. Use only the recorded ones from the
        # `SingleAgentEpisode`s.
        last_reward = episode_1.get_rewards(partial=False, consider_buffer=False)
        for agent_id, agent_reward in last_reward.items():
            # Ensure that the last recorded reward is the one in the getter result.
            check(episode_1.agent_episodes[agent_id].rewards[-1], agent_reward[0])
        # Assert also that the partial rewards are correctly extracted.
        # Note, `partial=True` does not look into the reward histories in the agents'
        # `SingleAgentEpisode`s, but instead into the global reward histories on
        # top level, where all partial rewards are recorded.
        last_reward = episode_1.get_rewards(partial=True)
        for agent_id, agent_reward in last_reward.items():
            # Ensure that the reward is the last in the global history.
            check(episode_1.partial_rewards[agent_id][-1], agent_reward[0])
            # Assert also that the correct timestep was recorded.
            check(episode_1.partial_rewards_t[agent_id][-1], episode_2.t)
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
                check(
                    episode_1.agent_buffers[agent_id]["rewards"].queue[0],
                    agent_reward[0],
                )
            # If the buffer is empty, the reward must come from the `
            # SingleAgentEpisode`'s reward history. Ensure that these two values
            # are identical.
            else:
                check(episode_1.agent_episodes[agent_id].rewards[-1], agent_reward[0])

        # Assert that the concatenated episode is immutable in its objects.
        buffered_action = episode_2.agent_buffers["agent_4"]["actions"].get_nowait()
        episode_2.agent_buffers["agent_4"]["actions"].put_nowait(10000)
        self.assertNotEqual(
            episode_2.agent_buffers["agent_4"]["actions"].queue[0], buffered_action
        )
        check(episode_1.agent_buffers["agent_4"]["actions"].queue[0], buffered_action)

    def test_get_return(self):
        # Generate an empty episode and ensure that the return is zero.
        episode = MultiAgentEpisode()
        # Now sample 100 timesteps.
        episode, env = self._mock_multi_agent_records_from_env()
        ret = episode.get_return()
        # Ensure that the return is now at least zero.
        self.assertGreaterEqual(ret, 0.0)
        # Assert that the return is indeed the sum of all agent returns.
        agent_returns = sum(
            agent_eps.get_return() for agent_eps in episode.agent_episodes.values()
        )
        self.assertTrue(ret, agent_returns)

        # Assert that adding the buffered rewards to the agent returns
        # gives the expected result when considering the buffer in
        # `get_return()`.
        buffered_rewards = sum(episode._agent_buffered_rewards.values())
        self.assertTrue(
            episode.get_return(consider_buffer=True), agent_returns + buffered_rewards
        )

    def test_len(self):
        # TODO (simon): Revisit this test and the MultiAgentEpisode.episode_concat API.
        return

        # Generate an empty episode and ensure that `len()` raises an error.
        episode = MultiAgentEpisode()
        # Now raise an error.
        with self.assertRaises(AssertionError):
            len(episode)

        # Create an episode and environment and sample 100 timesteps.
        episode, env = self._mock_multi_agent_records_from_env()
        # Assert that the length is indeed 100.
        check(len(episode), 100)

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

    def test_get_sample_batch(self):
        # TODO (simon): Revisit this test and the MultiAgentEpisode.episode_concat API.
        return

        # Generate an environment and episode and sample 100 timesteps.
        episode, env = self._mock_multi_agent_records_from_env()

        # Now convert to sample batch.
        batch = episode.get_sample_batch()

        # Assert that the timestep in the `MultiAgentBatch` is identical
        # to the episode timestep.
        check(len(batch), len(episode))
        # Assert that all agents are present in the multi-agent batch.
        # Note, all agents have collected samples.
        for agent_id in episode.agent_ids:
            self.assertTrue(agent_id in batch.policy_batches)
        # Assert that the recorded history length is correct.
        for agent_id, agent_eps in episode.agent_episodes.items():
            check(len(agent_eps), len(batch[agent_id]))
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
        batch = successor.get_sample_batch()
        # Assert that the number of timesteps match between episode and batch.
        # Note, the successor starts at `ts=100`.
        check(len(batch), len(successor))
        # Assert that all agents that were not done, yet, are present in the batch.
        for agent_id in env._agents_alive:
            self.assertTrue(agent_id in batch.policy_batches)
        # Ensure that the timesteps for each agent matches the it's batch length.
        for agent_id, agent_eps in successor.agent_episodes.items():
            # Note, we take over agent_ids
            if not agent_eps.is_done:
                check(len(agent_eps), len(batch[agent_id]))
        # Assert that now all agents are truncated b/c the environment truncated
        # them.
        for agent_id in batch.policy_batches:
            self.assertTrue(batch[agent_id]["truncateds"][-1])

        # Test now that when we concatenate the same logic holds.
        episode.concat_episode(successor)
        # Convert the concatenated episode to a sample batch now.
        batch = episode.get_sample_batch()
        # Assert that the length of episode and batch match.
        check(len(batch), len(episode))
        # Assert that all agents are present in the multi-agent batch.
        # Note, in the concatenated episode - in contrast to the successor
        # - we have all agents stepped.
        for agent_id in episode.agent_ids:
            self.assertTrue(agent_id in batch.policy_batches)
        # Assert that the recorded history length is correct.
        for agent_id, agent_eps in episode.agent_episodes.items():
            check(len(agent_eps), len(batch[agent_id]))
        # Assert that terminated agents are terminated in the sample batch.
        for agent_id in ["agent_1", "agent_5"]:
            self.assertTrue(batch[agent_id]["terminateds"][-1])
        # Assert that all the other agents are truncated by the environment.
        for agent_id in env._agents_alive:
            self.assertTrue(batch[agent_id]["truncateds"][-1])

        # Finally, test that an empty episode, gives an empty batch.
        episode = MultiAgentEpisode(agent_ids=env.get_agent_ids())
        # Convert now to sample batch.
        batch = episode.get_sample_batch()
        # Ensure that this batch is empty.
        check(len(batch), 0)

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
        env = env or MultiAgentTestEnv(truncate=truncate)

        # If no episode is given, construct one.
        # We give it the `agent_ids` to make it create all objects.
        episode = episode or MultiAgentEpisode()

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
                if episode._agent_buffered_actions[agent_id]
            }

        # Sample `size` many records.
        done_agents = set()
        for i in range(env.t, env.t + size):
            action = {
                agent_id: i + 1 for agent_id in obs if agent_id not in done_agents
            }
            obs, reward, terminated, truncated, info = env.step(action)
            done_agents |= {a for a, v in terminated.items() if v is True}
            done_agents |= {a for a, v in truncated.items() if v is True}
            episode.add_env_step(
                observations=obs,
                actions=action,
                rewards=reward,
                infos=info,
                terminateds=terminated,
                truncateds=truncated,
                extra_model_outputs={agent_id: {"extra": 10} for agent_id in action},
            )

        # Return both, episode and environment.
        return episode, env

    @staticmethod
    def _mock_multi_agent_records():
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
            {"agent_1": 0.5, "agent_2": 0.6, "agent_3": 0.7},
            # Agent 4 should now release the buffer with reward 1.0
            # and add the next reward to it, as it stepped and received
            # a next observation.
            {"agent_1": 1.1, "agent_3": 1.2, "agent_4": 1.3},
        ]
        infos = [
            {"agent_1": {"a1_i0": 1}, "agent_2": {"a2_i0": 2}, "agent_3": {"a3_i0": 3}},
            {
                "agent_1": {"a1_i1": 1.1},
                "agent_3": {"a3_i1": 3.1},
                "agent_4": {"a4_i1": 4.1},
            },
            {"agent_2": {"a2_i2": 2.2}, "agent_4": {"a4_i2": 4.2}},
        ]
        # Let no agent be terminated or truncated.
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

        return observations, actions, rewards, terminateds, truncateds, infos


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
