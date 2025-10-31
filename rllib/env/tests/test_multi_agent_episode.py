import unittest
from typing import Optional, Tuple

import gymnasium as gym
import numpy as np

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
        check(episode._hanging_actions_end, {"a0": 0})
        check(episode._hanging_rewards_end, {"a0": 0.1})
        check(episode._hanging_extra_model_outputs_end, {"a0": {}})
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
        check(episode._hanging_actions_end, {"a0": 2})
        check(episode._hanging_rewards_end, {"a0": 0.3})
        check(episode._hanging_extra_model_outputs_end, {"a0": {}})
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
        for agent_id in env.agents:
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
            check(episode.agent_episodes[agent_id].is_numpy, False)
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
        for agent_id in env.agents:
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
            self.assertTrue(agent_id not in episode._hanging_actions_end)
            self.assertTrue(agent_id not in episode._hanging_rewards_end)
            self.assertTrue(agent_id not in episode._hanging_extra_model_outputs_end)

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
        # Assert that the action cache for agent 4 is used.
        # Note, agent 4 acts, but receives no observation.
        # Note also, all other caches are always used, due to their defaults.
        self.assertTrue(episode._hanging_actions_end["agent_4"] is not None)
        # Assert that the reward caches of agents 3 and 5 are there.
        # For agent_5 (b/c it has never done anything), we add to the begin cache.
        check(episode._hanging_rewards_end["agent_3"], 2.2)
        check(episode._hanging_rewards_begin["agent_5"], 1.0)

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
        # `neg_index_as_lookback=True` and an index list.
        # w/ fill
        obs = episode.get_observations(
            indices=[-2, -1],
            fill=-10,
            neg_index_as_lookback=True,
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
        obs = episode.get_observations(indices=[-2, -1], neg_index_as_lookback=True)
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
            neg_index_as_lookback=True,
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
        # `neg_index_as_lookback=True` and an index list.
        # w/ fill
        inf = episode.get_infos(
            indices=[-2, -1],
            fill=-10,
            neg_index_as_lookback=True,
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
        inf = episode.get_infos(indices=[-2, -1], neg_index_as_lookback=True)
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
            neg_index_as_lookback=True,
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
        """Tests whether the `MultiAgentEpisode.get_actions()` API works as expected."""
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
        # Access >=0 integer indices (expect index error as everything is in
        # lookback buffer).
        for i in range(0, 5):
            with self.assertRaises(IndexError):
                episode.get_actions(i)
        # Access <= -5 integer indices (expect index error as this goes beyond length of
        # lookback buffer).
        for i in range(-5, -10, -1):
            with self.assertRaises(IndexError):
                episode.get_actions(i)
        # Access list of indices, env steps.
        act = episode.get_actions([-1, -2])
        check(act, {"a1": [3, 2]})
        act = episode.get_actions([-2, -3])
        check(act, {"a0": [1], "a1": [2, 1]})
        act = episode.get_actions([-3, -4])
        check(act, {"a0": [1, 0], "a1": [1, 0]})
        # Access slices of indices, env steps.
        act = episode.get_actions(slice(-1, -3, -1))
        check(act, {"a1": [3, 2]})
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

        episode.add_env_step(
            observations={"a0": 5, "a1": 5}, actions={"a1": 4}, rewards={"a1": 4}
        )
        check(episode.get_actions(0), {"a1": 4})
        check(episode.get_actions(-1), {"a1": 4})
        check(episode.get_actions(-2), {"a1": 3})
        episode.add_env_step(
            observations={"a1": 6},
            actions={"a0": 5, "a1": 5},
            rewards={"a0": 5, "a1": 5},
        )
        check(episode.get_actions(0), {"a1": 4})
        check(episode.get_actions(1), {"a0": 5, "a1": 5})
        check(episode.get_actions(-1), {"a0": 5, "a1": 5})

        # Generate a simple multi-agent episode, where a hanging action is at the end.
        observations = [
            {"a0": 0, "a1": 0},
            {"a0": 0, "a1": 1},
            {"a0": 2},
        ]
        actions = [{"a0": 0, "a1": 0}, {"a0": 1, "a1": 1}]
        rewards = [{"a0": 0.0, "a1": 0.0}, {"a0": 0.1, "a1": 0.1}]
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            len_lookback_buffer=0,
        )
        # Test, whether the hanging action of a1 at the end gets returned properly
        # for idx=-1.
        act = episode.get_actions(-1)
        check(act, {"a0": 1, "a1": 1})
        act = episode.get_actions(-2)
        check(act, {"a0": 0, "a1": 0})
        act = episode.get_actions(0)
        check(act, {"a0": 0, "a1": 0})
        act = episode.get_actions(1)
        check(act, {"a0": 1, "a1": 1})
        with self.assertRaises(IndexError):
            episode.get_actions(2)
        with self.assertRaises(IndexError):
            episode.get_actions(-3)

        # Generate a simple multi-agent episode, where one agent is done.
        # observations = [
        #    {"a0": 0, "a1": 0},
        #    {"a0": 1, "a1": 1},
        #    {"a0": 2},
        # ]
        # actions = [{"a0": 0, "a1": 0}, {"a0": 1}]
        # rewards = [{"a0": 1, "a1": 1}, {"a0": 2}]
        # terminateds = {"a1": True}
        # episode = MultiAgentEpisode(
        #    observations=observations,
        #    actions=actions,
        #    rewards=rewards,
        #    terminateds=terminateds,
        #    len_lookback_buffer=0,
        # )
        episode = MultiAgentEpisode()
        episode.add_env_reset(observations={"a0": 0, "a1": 0})
        episode.add_env_step(
            observations={"a0": 1, "a1": 1},
            actions={"a0": 0, "a1": 0},
            rewards={"a0": 0.0, "a1": 0.0},
            terminateds={"a1": True},
        )
        episode.add_env_step(
            observations={"a0": 2}, actions={"a0": 1}, rewards={"a0": 1.0}
        )
        act = episode.get_actions(-1)
        check(act, {"a0": 1})

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
        # `neg_index_as_lookback=True` and an index list.
        # w/ fill
        act = episode.get_actions(
            indices=[-2, -1],
            fill=-10,
            neg_index_as_lookback=True,
        )
        check(
            act,
            {
                "agent_1": [0, 1],
                "agent_2": [0, -10],
                "agent_3": [0, 1],
                "agent_4": [-10, 1],
            },
        )
        # Same, but w/o fill.
        act = episode.get_actions(indices=[-2, -1], neg_index_as_lookback=True)
        check(
            act,
            {
                "agent_1": [0, 1],
                "agent_2": [0],
                "agent_3": [0, 1],
                "agent_4": [1],
            },
        )

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
        check(act, {})
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
        # Expect error when calling with env_steps=False AND return_list=True.
        with self.assertRaises(ValueError):
            episode.get_actions(env_steps=False, return_list=True)
        # List of indices.
        act = episode.get_actions(indices=[-1, -2], return_list=True)
        check(act, [actions[-1], actions[-2]])
        # Slice of indices w/ fill.
        # From the last ts in lookback buffer to first actual ts (empty as all data is
        # in lookback buffer, but we fill).
        act = episode.get_actions(
            slice(-1, 1), return_list=True, fill=-8, neg_index_as_lookback=True
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
        # `neg_index_as_lookback=True` and an index list.
        # w/ fill
        rew = episode.get_rewards(
            indices=[-2, -1],
            fill=-10,
            neg_index_as_lookback=True,
        )
        check(
            rew,
            {
                "agent_1": [0.5, 1.1],
                "agent_2": [0.6, -10],
                "agent_3": [0.7, 1.2],
                "agent_4": [-10, 1.3],
            },
        )
        # Same, but w/o fill.
        episode.get_rewards(indices=[-2, -1], neg_index_as_lookback=True)

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
            neg_index_as_lookback=True,
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
        # Simple multi-agent episode, in which all agents always step.
        episode = self._create_simple_episode(
            [
                {"a0": 0, "a1": 0},
                {"a0": 1, "a1": 1},
                {"a0": 2, "a1": 2},
            ]
        )
        successor = episode.cut()
        check(len(successor), 0)
        check(successor.env_t_started, 2)
        check(successor.env_t, 2)
        check(successor.env_t_to_agent_t, {"a0": [0], "a1": [0]})
        a0 = successor.agent_episodes["a0"]
        a1 = successor.agent_episodes["a1"]
        check((len(a0), len(a1)), (0, 0))
        check((a0.t_started, a1.t_started), (2, 2))
        check((a0.t, a1.t), (2, 2))
        check((a0.observations, a1.observations), ([2], [2]))
        check((a0.actions, a1.actions), ([], []))
        check((a0.rewards, a1.rewards), ([], []))
        check(successor._hanging_actions_end, {})
        check(successor._hanging_rewards_end, {})
        check(successor._hanging_extra_model_outputs_end, {})

        # Multi-agent episode with lookback buffer, in which all agents always step.
        episode = self._create_simple_episode(
            [
                {"a0": 0, "a1": 0},
                {"a0": 1, "a1": 1},
                {"a0": 2, "a1": 2},
                {"a0": 3, "a1": 3},
            ],
            len_lookback_buffer=2,
        )
        # Cut with lookback=0 argument (default).
        successor = episode.cut()
        check(len(successor), 0)
        check(successor.env_t_started, 1)
        check(successor.env_t, 1)
        check(successor.env_t_to_agent_t, {"a0": [0], "a1": [0]})
        a0 = successor.agent_episodes["a0"]
        a1 = successor.agent_episodes["a1"]
        check((len(a0), len(a1)), (0, 0))
        check((a0.t_started, a1.t_started), (1, 1))
        check((a0.t, a1.t), (1, 1))
        check((a0.observations, a1.observations), ([3], [3]))
        check((a0.actions, a1.actions), ([], []))
        check((a0.rewards, a1.rewards), ([], []))
        check(successor._hanging_actions_end, {})
        check(successor._hanging_rewards_end, {})
        check(successor._hanging_extra_model_outputs_end, {})
        # Cut with lookback=2 argument.
        successor = episode.cut(len_lookback_buffer=2)
        check(len(successor), 0)
        check(successor.env_t_started, 1)
        check(successor.env_t, 1)
        check(successor.env_t_to_agent_t["a0"].data, [0, 1, 2])
        check(successor.env_t_to_agent_t["a1"].data, [0, 1, 2])
        check(successor.env_t_to_agent_t["a0"].lookback, 2)
        check(successor.env_t_to_agent_t["a1"].lookback, 2)
        a0 = successor.agent_episodes["a0"]
        a1 = successor.agent_episodes["a1"]
        check((len(a0), len(a1)), (0, 0))
        check((a0.t_started, a1.t_started), (1, 1))
        check((a0.t, a1.t), (1, 1))
        check((a0.observations, a1.observations), ([3], [3]))
        check((a0.actions, a1.actions), ([], []))
        check((a0.rewards, a1.rewards), ([], []))
        check(successor._hanging_actions_end, {})
        check(successor._hanging_rewards_end, {})
        check(successor._hanging_extra_model_outputs_end, {})

        # Multi-agent episode, in which one agent has a long sequence of not acting,
        # but does receive (intermittend/hanging) rewards during this time.
        observations = [
            {"a0": 0, "a1": 0},  # 0
            {"a0": 1},  # 1
            {"a0": 2},  # 2
            {"a0": 3},  # 3
        ]
        episode = MultiAgentEpisode(
            observations=observations,
            actions=observations[:-1],
            rewards=[
                {"a0": 0.0, "a1": 0.0},  # 0
                {"a0": 0.1, "a1": 0.1},  # 1
                {"a0": 0.2, "a1": 0.2},  # 2
            ],
            len_lookback_buffer=0,
        )
        successor = episode.cut()
        check(len(successor), 0)
        check(successor.env_t_started, 3)
        check(successor.env_t, 3)
        a0 = successor.agent_episodes["a0"]
        self.assertTrue("a1" not in successor.agent_episodes)
        check(len(a0), 0)
        check(a0.t_started, 3)
        check(a0.t, 3)
        check(a0.observations, [3])
        check(a0.actions, [])
        check(a0.rewards, [])
        check(successor._hanging_rewards_begin, {"a1": 0.3})
        check(successor._hanging_actions_end, {})
        check(successor._hanging_rewards_end, {"a1": 0.0})
        check(successor._hanging_extra_model_outputs_end, {})
        # Add a few timesteps to successor and test the resulting episode.
        successor.add_env_step(
            observations={"a0": 4},
            actions={"a0": 3},
            rewards={"a0": 0.3, "a1": 0.3},
        )
        check(len(successor), 1)
        check(successor.env_t_started, 3)
        check(successor.env_t, 4)
        # Just b/c we added an intermittend reward for a1 does not mean it should
        # already have a SAEps in `successor`. It still hasn't received its first obs
        # yet after the cut.
        self.assertTrue("a1" not in successor.agent_episodes)
        check(len(a0), 1)
        check(a0.t_started, 3)
        check(a0.t, 4)
        check(a0.observations, [3, 4])
        check(a0.actions, [3])
        check(a0.rewards, [0.3])
        check(successor._hanging_rewards_begin, {"a1": 0.6})
        check(successor._hanging_actions_end, {})
        check(successor._hanging_rewards_end, {"a1": 0.0})
        check(successor._hanging_extra_model_outputs_end, {})
        # Now a1 actually does receive its next obs.
        successor.add_env_step(
            observations={"a0": 5, "a1": 5},  # <- this is a1's 1st obs in this chunk
            actions={"a0": 4},
            rewards={"a0": 0.4, "a1": 0.4},
        )
        check(len(successor), 2)
        check(successor.env_t_started, 3)
        check(successor.env_t, 5)
        a1 = successor.agent_episodes["a1"]
        check((len(a0), len(a1)), (2, 0))
        check((a0.t_started, a1.t_started), (3, 0))
        check((a0.t, a1.t), (5, 0))
        check((a0.observations, a1.observations), ([3, 4, 5], [5]))
        check((a0.actions, a1.actions), ([3, 4], []))
        check((a0.rewards, a1.rewards), ([0.3, 0.4], []))
        # Begin caches keep accumulating a1's rewards.
        check(successor._hanging_rewards_begin, {"a1": 1.0})
        # But end caches are now empty (due to a1's observation/finished step).
        check(successor._hanging_actions_end, {})
        check(successor._hanging_rewards_end, {"a1": 0.0})
        check(successor._hanging_extra_model_outputs_end, {})

        # Generate a simple multi-agent episode and check all internals after
        # construction.
        episode_1 = self._create_simple_episode(
            [
                {"a0": 0, "a1": 0},
                {"a1": 1},
                {"a1": 2},
                {"a1": 3},
            ],
            len_lookback_buffer="auto",
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
        check(episode_2._hanging_actions_end, {})
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

        # Another complex case.
        episode = self._create_simple_episode(
            [
                {"a0": 0},
                {"a2": 0},
                {"a2": 1},
                {"a2": 2},
                {"a0": 1},
                {"a2": 3},
                {"a2": 4},
                # <- BUT: actual cut here, b/c of hanging action of a2
                {"a2": 5},
                # <- would expect cut here (b/c of lookback==1)
                {"a0": 2},
                {"a1": 0},
            ],
            len_lookback_buffer=0,
        )
        successor = episode.cut(len_lookback_buffer=1)
        check(len(successor), 0)
        check(successor.env_t, 9)
        check(successor.env_t_started, 9)
        self.assertTrue(all(len(e) == 0 for e in successor.agent_episodes.values()))
        self.assertTrue(all(len(e) == 1 for e in successor.env_t_to_agent_t.values()))
        self.assertTrue(
            all(e.lookback == 2 for e in successor.env_t_to_agent_t.values())
        )
        check(successor.env_t_to_agent_t["a0"].data, ["S", 0, "S"])
        check(successor.env_t_to_agent_t["a1"].data, ["S", "S", 0])
        check(successor.env_t_to_agent_t["a2"].data, [0, "S", "S"])

        check(successor.get_observations(0), {"a1": 0})
        with self.assertRaises(IndexError):
            successor.get_observations(1)
        check(successor.get_observations(-2), {"a0": 2})
        check(successor.get_observations(-3), {"a2": 5})
        with self.assertRaises(IndexError):
            successor.get_observations(-4)

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
                        neg_index_as_lookback=True,
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

    def test_slice(self):
        # Generate a simple multi-agent episode.
        episode = self._create_simple_episode(
            [
                {"a0": 0, "a1": 0},
                {"a1": 1},
                {"a1": 2},
                {"a0": 3, "a1": 3},
                {"a0": 4},
                {"a0": 5, "a1": 5},
                {"a0": 6, "a1": 6},
                {"a1": 7},
                {"a1": 8},
                {"a0": 9},
            ]
        )
        check(len(episode), 9)

        # Slice the episode in different ways and check results.
        # Empty slice.
        slice_ = episode[100:100]
        check(len(slice_), 0)
        check(slice_.env_t_started, 9)
        check(slice_.env_t, 9)
        # All-include slices.
        for s in [
            slice(None, None, None),
            slice(-100, None, None),
            slice(None, 1000, None),
            slice(-1000, 1000, None),
        ]:
            slice_ = episode[s]
            check(len(slice_), len(episode))
            check(slice_.env_t_started, 0)
            check(slice_.env_t, 9)
            a0 = slice_.agent_episodes["a0"]
            a1 = slice_.agent_episodes["a1"]
            check((len(a0), len(a1)), (5, 7))
            check((a0.t_started, a1.t_started), (0, 0))
            check((a0.t, a1.t), (5, 7))
            check(
                (a0.observations, a1.observations),
                ([0, 3, 4, 5, 6, 9], [0, 1, 2, 3, 5, 6, 7, 8]),
            )
            check((a0.actions, a1.actions), ([0, 3, 4, 5, 6], [0, 1, 2, 3, 5, 6, 7]))
            check(
                (a0.rewards, a1.rewards),
                ([0.0, 0.3, 0.4, 0.5, 0.6], [0.0, 0.1, 0.2, 0.3, 0.5, 0.6, 0.7]),
            )
            check((a0.is_done, a1.is_done), (False, False))
        # From pos start.
        slice_ = episode[2:]
        check(len(slice_), 7)
        check(slice_.env_t_started, 2)
        check(slice_.env_t, 9)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (4, 5))
        check((a0.t_started, a1.t_started), (1, 2))
        check((a0.t, a1.t), (5, 7))
        check(
            (a0.observations, a1.observations),
            ([3, 4, 5, 6, 9], [2, 3, 5, 6, 7, 8]),
        )
        check((a0.actions, a1.actions), ([3, 4, 5, 6], [2, 3, 5, 6, 7]))
        check(
            (a0.rewards, a1.rewards),
            ([0.3, 0.4, 0.5, 0.6], [0.2, 0.3, 0.5, 0.6, 0.7]),
        )
        check((a0.is_done, a1.is_done), (False, False))
        # If a slice ends in a "gap" for an agent, expect actions and rewards to be
        # cached in the agent's buffer.
        slice_ = episode[:1]
        check(len(slice_), 1)
        check(slice_.env_t_started, 0)
        check(slice_.env_t, 1)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (0, 1))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (0, 1))
        check((a0.observations, a1.observations), ([0], [0, 1]))
        check((a0.actions, a1.actions), ([], [0]))
        check((a0.rewards, a1.rewards), ([], [0.0]))
        check((a0.is_done, a1.is_done), (False, False))
        check(slice_._hanging_actions_end["a0"], 0)
        check(slice_._hanging_rewards_end["a0"], 0.0)
        # To pos stop.
        slice_ = episode[:3]
        check(len(slice_), 3)
        check(slice_.env_t_started, 0)
        check(slice_.env_t, 3)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (1, 3))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (1, 3))
        check((a0.observations, a1.observations), ([0, 3], [0, 1, 2, 3]))
        check((a0.actions, a1.actions), ([0], [0, 1, 2]))
        check((a0.rewards, a1.rewards), ([0.0], [0.0, 0.1, 0.2]))
        check((a0.is_done, a1.is_done), (False, False))
        # To neg stop.
        slice_ = episode[:-1]
        check(len(slice_), 8)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (4, 7))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (4, 7))
        check(
            (a0.observations, a1.observations),
            ([0, 3, 4, 5, 6], [0, 1, 2, 3, 5, 6, 7, 8]),
        )
        check((a0.actions, a1.actions), ([0, 3, 4, 5], [0, 1, 2, 3, 5, 6, 7]))
        check(
            (a0.rewards, a1.rewards),
            ([0.0, 0.3, 0.4, 0.5], [0.0, 0.1, 0.2, 0.3, 0.5, 0.6, 0.7]),
        )
        check((a0.is_done, a1.is_done), (False, False))
        # Expect the hanging action to be found in the buffer.
        check(slice_._hanging_actions_end["a0"], 6)

        slice_ = episode[:-4]
        check(len(slice_), 5)
        check(slice_.env_t_started, 0)
        check(slice_.env_t, 5)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (3, 4))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (3, 4))
        check((a0.observations, a1.observations), ([0, 3, 4, 5], [0, 1, 2, 3, 5]))
        check((a0.actions, a1.actions), ([0, 3, 4], [0, 1, 2, 3]))
        check(
            (a0.rewards, a1.rewards),
            ([0.0, 0.3, 0.4], [0.0, 0.1, 0.2, 0.3]),
        )
        check((a0.is_done, a1.is_done), (False, False))
        # From neg start.
        slice_ = episode[-2:]
        check(len(slice_), 2)
        check(slice_.env_t_started, 7)
        check(slice_.env_t, 9)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (0, 1))
        check((a0.t_started, a1.t_started), (5, 6))
        check((a0.t, a1.t), (5, 7))
        check((a0.observations, a1.observations), ([9], [7, 8]))
        check((a0.actions, a1.actions), ([], [7]))
        check((a0.rewards, a1.rewards), ([], [0.7]))
        check((a0.is_done, a1.is_done), (False, False))
        # From neg start.
        slice_ = episode[-3:]
        check(len(slice_), 3)
        check(slice_.env_t_started, 6)
        check(slice_.env_t, 9)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (1, 2))
        check((a0.t_started, a1.t_started), (4, 5))
        check((a0.t, a1.t), (5, 7))
        check((a0.observations, a1.observations), ([6, 9], [6, 7, 8]))
        check((a0.actions, a1.actions), ([6], [6, 7]))
        check((a0.rewards, a1.rewards), ([0.6], [0.6, 0.7]))
        check((a0.is_done, a1.is_done), (False, False))
        # From neg start.
        slice_ = episode[-5:]
        check(len(slice_), 5)
        check(slice_.env_t_started, 4)
        check(slice_.env_t, 9)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (3, 3))
        check((a0.t_started, a1.t_started), (2, 4))
        check((a0.t, a1.t), (5, 7))
        check((a0.observations, a1.observations), ([4, 5, 6, 9], [5, 6, 7, 8]))
        check((a0.actions, a1.actions), ([4, 5, 6], [5, 6, 7]))
        check((a0.rewards, a1.rewards), ([0.4, 0.5, 0.6], [0.5, 0.6, 0.7]))
        check((a0.is_done, a1.is_done), (False, False))
        # From neg start to neg stop.
        slice_ = episode[-4:-2]
        check(len(slice_), 2)
        check(slice_.env_t_started, 5)
        check(slice_.env_t, 7)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (1, 2))
        check((a0.t_started, a1.t_started), (3, 4))
        check((a0.t, a1.t), (4, 6))
        check((a0.observations, a1.observations), ([5, 6], [5, 6, 7]))
        check((a0.actions, a1.actions), ([5], [5, 6]))
        check((a0.rewards, a1.rewards), ([0.5], [0.5, 0.6]))
        check((a0.is_done, a1.is_done), (False, False))

        # Test what happens if one single-agent episode terminates earlier than the
        # other.
        observations = [
            {"a0": 0, "a1": 0},
            {"a0": 1, "a1": 1},
            {"a1": 2},
            {"a1": 3},
        ]
        actions = [
            {"a0": 0, "a1": 0},
            {"a1": 1},
            {"a1": 2},
        ]
        rewards = [{aid: a / 10 for aid, a in a.items()} for a in actions]
        # TODO (sven): Do NOT use self._create_simple_episode here b/c this util does
        #  not handle terminateds (should not create actions after final observations).
        episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            terminateds={"a0": True},
            len_lookback_buffer=0,
        )
        # ---
        slice_ = episode[:1]
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check(len(slice_), 1)
        check(slice_.env_t_started, 0)
        check(slice_.env_t, 1)
        check((len(a0), len(a1)), (1, 1))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (1, 1))
        check((a0.observations, a1.observations), ([0, 1], [0, 1]))
        check((a0.actions, a1.actions), ([0], [0]))
        check((a0.rewards, a1.rewards), ([0.0], [0.0]))
        check((a0.is_done, a1.is_done), (True, False))
        # ---
        slice_ = episode[:2]
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check(len(slice_), 2)
        check(slice_.env_t_started, 0)
        check(slice_.env_t, 2)
        check((len(a0), len(a1)), (1, 2))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (1, 2))
        check((a0.observations, a1.observations), ([0, 1], [0, 1, 2]))
        check((a0.actions, a1.actions), ([0], [0, 1]))
        check((a0.rewards, a1.rewards), ([0.0], [0.0, 0.1]))
        check((a0.is_done, a1.is_done), (True, False))
        # ---
        slice_ = episode[2:]
        self.assertTrue("a0" not in slice_.agent_episodes)
        a1 = slice_.agent_episodes["a1"]
        check(len(slice_), 1)
        check(slice_.env_t_started, 2)
        check(slice_.env_t, 3)
        check(len(a1), 1)
        check(a1.t_started, 2)
        check(a1.t, 3)
        check(a1.observations, [2, 3])
        check(a1.actions, [2])
        check(a1.rewards, [0.2])
        check(a1.is_done, False)

        # Test what happens if we have lookback buffers.
        observations = [
            {"a0": 0, "a1": 0},  # lookback -2
            {"a0": 1, "a1": 1},  # lookback -1
            {"a1": 2},  # 0
            {"a1": 3},  # 1
            {"a1": 4},  # 2
            {"a0": 5, "a1": 5},  # 3
            {"a0": 6},  # 4
            {"a0": 7, "a1": 7},  # 5
            {"a0": 8},  # 6
            {"a1": 9},  # 7
        ]
        episode = self._create_simple_episode(observations, len_lookback_buffer=2)
        # ---
        slice_ = episode[1:3]
        check(len(slice_), 2)
        check(slice_.env_t_started, 1)
        check(slice_.env_t, 3)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (0, 2))
        check((a0.t_started, a1.t_started), (2, 3))
        check((a0.t, a1.t), (2, 5))
        check((a0.observations, a1.observations), ([5], [3, 4, 5]))
        check((a0.actions, a1.actions), ([], [3, 4]))
        check((a0.rewards, a1.rewards), ([], [0.3, 0.4]))
        check((a0.is_done, a1.is_done), (False, False))
        # ---
        slice_ = episode[None:4]
        check(len(slice_), 4)
        check(slice_.env_t_started, 0)
        check(slice_.env_t, 4)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (1, 3))
        check((a0.t_started, a1.t_started), (2, 2))
        check((a0.t, a1.t), (3, 5))
        check((a0.observations, a1.observations), ([5, 6], [2, 3, 4, 5]))
        check((a0.actions, a1.actions), ([5], [2, 3, 4]))
        check((a0.rewards, a1.rewards), ([0.5], [0.2, 0.3, 0.4]))
        check((a0.is_done, a1.is_done), (False, False))
        # ---
        slice_ = episode[-3:-1]
        check(len(slice_), 2)
        check(slice_.env_t_started, 4)
        check(slice_.env_t, 6)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (2, 0))
        check((a0.t_started, a1.t_started), (3, 6))
        check((a0.t, a1.t), (5, 6))
        check((a0.observations, a1.observations), ([6, 7, 8], [7]))
        check((a0.actions, a1.actions), ([6, 7], []))
        check((a0.rewards, a1.rewards), ([0.6, 0.7], []))
        check((a0.is_done, a1.is_done), (False, False))
        # ---
        slice_ = episode[-1:None]
        check(len(slice_), 1)
        check(slice_.env_t_started, 6)
        check(slice_.env_t, 7)
        a0 = slice_.agent_episodes["a0"]
        a1 = slice_.agent_episodes["a1"]
        check((len(a0), len(a1)), (0, 0))
        check((a0.t_started, a1.t_started), (5, 7))
        check((a0.t, a1.t), (5, 7))
        check((a0.observations, a1.observations), ([8], [9]))
        check((a0.actions, a1.actions), ([], []))
        check((a0.rewards, a1.rewards), ([], []))
        check((a0.is_done, a1.is_done), (False, False))

    def test_concat_episode(self):
        # Generate a simple multi-agent episode.
        base_episode = self._create_simple_episode(
            [
                {"a0": 0, "a1": 0},
                {"a0": 1, "a1": 1},  # <- split here, then concat
                {"a0": 2, "a1": 2},
            ]
        )
        check(len(base_episode), 2)
        # Split it into two slices.
        episode_1, episode_2 = base_episode[:1], base_episode[1:]
        check(len(episode_1), 1)
        check(len(episode_2), 1)
        # Re-concat these slices.
        episode_1.concat_episode(episode_2)
        check(len(episode_1), 2)
        check(episode_1.env_t_started, 0)
        check(episode_1.env_t, 2)
        a0 = episode_1.agent_episodes["a0"]
        a1 = episode_1.agent_episodes["a1"]
        check((len(a0), len(a1)), (2, 2))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (2, 2))
        check((a0.observations, a1.observations), ([0, 1, 2], [0, 1, 2]))
        check((a0.actions, a1.actions), ([0, 1], [0, 1]))
        check((a0.rewards, a1.rewards), ([0.0, 0.1], [0.0, 0.1]))
        check((a0.is_done, a1.is_done), (False, False))

        # Generate a more complex multi-agent episode.
        base_episode = self._create_simple_episode(
            [
                {"a0": 0, "a1": 0},
                {"a0": 1, "a1": 1},
                {"a1": 2},
                {"a1": 3},
                {"a1": 4},  # <- split here, then concat
                {"a0": 5, "a1": 5},
                {"a0": 6},  # <- split here, then concat
                {"a0": 7, "a1": 7},  # <- split here, then concat
                {"a0": 8},  # <- split here, then concat
                {"a1": 9},
            ]
        )
        check(len(base_episode), 9)

        # Split it into two slices.
        for split_ in [(4, (4, 5)), (6, (6, 3)), (7, (7, 2)), (8, (8, 1))]:
            episode_1, episode_2 = base_episode[: split_[0]], base_episode[split_[0] :]
            check(len(episode_1), split_[1][0])
            check(len(episode_2), split_[1][1])
            # Re-concat these slices.
            episode_1.concat_episode(episode_2)
            check(len(episode_1), 9)
            check(episode_1.env_t_started, 0)
            check(episode_1.env_t, 9)
            a0 = episode_1.agent_episodes["a0"]
            a1 = episode_1.agent_episodes["a1"]
            check((len(a0), len(a1)), (5, 7))
            check((a0.t_started, a1.t_started), (0, 0))
            check((a0.t, a1.t), (5, 7))
            check(
                (a0.observations, a1.observations),
                ([0, 1, 5, 6, 7, 8], [0, 1, 2, 3, 4, 5, 7, 9]),
            )
            check((a0.actions, a1.actions), ([0, 1, 5, 6, 7], [0, 1, 2, 3, 4, 5, 7]))
            check(
                (a0.rewards, a1.rewards),
                ([0, 0.1, 0.5, 0.6, 0.7], [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7]),
            )
            check((a0.is_done, a1.is_done), (False, False))

        # Test hanging rewards.
        observations = [
            {"a0": 0, "a1": 0},  # 0
            {"a0": 1},  # 1
            {"a0": 2},  # 2  <- split here, then concat
            {"a0": 3},  # 3
            {"a0": 4},  # 4
        ]
        actions = observations[:-1]
        # a1 continues receiving rewards (along with a0's actions).
        rewards = [
            {"a0": 0.0, "a1": 0.0},  # 0
            {"a0": 0.1, "a1": 1.0},  # 1
            {"a0": 0.2, "a1": 2.0},  # 2
            {"a0": 0.3, "a1": 3.0},  # 3
        ]
        base_episode = MultiAgentEpisode(
            observations=observations,
            actions=actions,
            rewards=rewards,
            len_lookback_buffer=0,
        )
        check(len(base_episode), 4)
        check(base_episode._hanging_rewards_end, {"a1": 6.0})
        episode_1, episode_2 = base_episode[:2], base_episode[2:]
        check(len(episode_1), 2)
        check(len(episode_2), 2)
        # Re-concat these slices.
        episode_1.concat_episode(episode_2)
        check(len(episode_1), 4)
        check(episode_1.env_t_started, 0)
        check(episode_1.env_t, 4)
        a0 = episode_1.agent_episodes["a0"]
        a1 = episode_1.agent_episodes["a1"]
        check((len(a0), len(a1)), (4, 0))
        check((a0.t_started, a1.t_started), (0, 0))
        check((a0.t, a1.t), (4, 0))
        check(
            (a0.observations, a1.observations),
            ([0, 1, 2, 3, 4], [0]),
        )
        check((a0.actions, a1.actions), ([0, 1, 2, 3], []))
        check(
            (a0.rewards, a1.rewards),
            ([0, 0.1, 0.2, 0.3], []),
        )
        check(episode_1._hanging_rewards_end, {"a1": 6.0})
        check((a0.is_done, a1.is_done), (False, False))

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
        buffered_rewards = sum(episode._hanging_rewards_end.values())
        self.assertTrue(
            episode.get_return(include_hanging_rewards=True),
            agent_returns + buffered_rewards,
        )

    def test_len(self):
        # Generate an empty episode and ensure that `len()` raises an error.
        episode = MultiAgentEpisode()

        # Generate a new episode with some initialization data.
        obs = [
            {"a0": 0, "a1": 0},
            {"a1": 1},
            {"a0": 2},
            {"a0": 3, "a1": 3},
        ]
        episode = MultiAgentEpisode(
            observations=obs, actions=obs[:-1], rewards=obs[:-1], len_lookback_buffer=0
        )
        check(len(episode), 3)
        obs.append({"a1": 4})
        episode = MultiAgentEpisode(
            observations=obs, actions=obs[:-1], rewards=obs[:-1], len_lookback_buffer=0
        )
        check(len(episode), 4)
        obs.append({"a0": 5, "a1": 5})
        episode = MultiAgentEpisode(
            observations=obs, actions=obs[:-1], rewards=obs[:-1], len_lookback_buffer=0
        )
        check(len(episode), 5)
        obs.append({"a0": 6})
        episode = MultiAgentEpisode(
            observations=obs, actions=obs[:-1], rewards=obs[:-1], len_lookback_buffer=0
        )
        check(len(episode), 6)

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
        # episode.concat_episode(successor)
        # Assert that the length is now 100.
        # self.assertTrue(len(episode), 200)

    def test_get_state_and_from_state(self):
        # Generate an empty episode and ensure that the state is empty.
        # Generate a simple multi-agent episode.
        episode = self._create_simple_episode(
            [
                {"a0": 0, "a1": 0},
                {"a1": 1},
                {"a1": 2},
                {"a0": 3, "a1": 3},
                {"a0": 4},
                {"a0": 5, "a1": 5},
                {"a0": 6, "a1": 6},
                {"a1": 7},
                {"a1": 8},
                {"a0": 9},
            ]
        )

        # Get the state of the episode.
        state = episode.get_state()
        # Ensure that the state is not empty.
        self.assertTrue(state)
        episode_2 = MultiAgentEpisode.from_state(state)

        # Assert that the two episodes are identical.
        self.assertEqual(episode_2.id_, episode.id_)
        self.assertEqual(
            episode_2.agent_to_module_mapping_fn, episode.agent_to_module_mapping_fn
        )
        self.assertEqual(
            type(episode_2.observation_space), type(episode.observation_space)
        )
        self.assertEqual(type(episode_2.action_space), type(episode.action_space))
        check(episode_2.env_t_started, episode.env_t_started)
        check(episode_2.env_t, episode.env_t)
        check(episode_2.agent_t_started, episode.agent_t_started)
        self.assertEqual(episode_2.env_t_to_agent_t, episode.env_t_to_agent_t)
        for agent_id, env_t_to_agent_t in episode_2.env_t_to_agent_t.items():
            check(env_t_to_agent_t.data, episode.env_t_to_agent_t[agent_id].data)
            check(
                env_t_to_agent_t.lookback, episode.env_t_to_agent_t[agent_id].lookback
            )
        check(episode_2._hanging_actions_end, episode._hanging_actions_end)
        check(
            episode_2._hanging_extra_model_outputs_end,
            episode._hanging_extra_model_outputs_end,
        )
        check(episode_2._hanging_rewards_end, episode._hanging_rewards_end)
        check(episode_2._hanging_rewards_begin, episode._hanging_rewards_begin)
        check(episode_2.is_terminated, episode.is_terminated)
        check(episode_2.is_truncated, episode.is_truncated)
        self.assertSetEqual(
            set(episode_2.agent_episodes.keys()), set(episode.agent_episodes.keys())
        )
        for agent_id, agent_eps in episode_2.agent_episodes.items():
            self.assertEqual(agent_eps.id_, episode.agent_episodes[agent_id].id_)
        check(episode_2._start_time, episode._start_time)
        check(episode_2._last_step_time, episode._last_step_time)

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
        episode = MultiAgentEpisode(agent_ids=env.agents)
        # Convert now to sample batch.
        batch = episode.get_sample_batch()
        # Ensure that this batch is empty.
        check(len(batch), 0)

    def _create_simple_episode(self, obs, len_lookback_buffer=0):
        return MultiAgentEpisode(
            observations=obs,
            actions=obs[:-1],
            rewards=[{aid: o / 10 for aid, o in o_dict.items()} for o_dict in obs[:-1]],
            len_lookback_buffer=len_lookback_buffer,
        )

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
        episode = MultiAgentEpisode() if episode is None else episode

        # We initialize the episode, if requested.
        if init:
            obs, info = env.reset(seed=seed)
            episode.add_env_reset(observations=obs, infos=info)
        # In the other case we need at least the last observations for the next
        # actions.
        else:
            obs = dict(episode.get_observations(-1))

        # Sample `size` many records.
        done_agents = {aid for aid, t in episode.get_terminateds().items() if t}
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

    def test_setters(self):
        """Tests whether the MultiAgentEpisode's setter methods work as expected.

        Also tests numpy'ized episodes.

        This test covers all setter methods:
        - set_observations
        - set_actions
        - set_rewards
        - set_extra_model_outputs

        Each setter is tested with various indexing scenarios including:
        - Single index
        - List of indices
        - Slice objects
        - Negative indices (both regular and lookback buffer interpretation)

        Uses two agents: a0 and a1
        """
        import copy

        SOME_KEY = "some_key"

        # Create a simple multi-agent episode with two agents without lookback buffer first for basic tests
        episode = MultiAgentEpisode(
            observations=[
                {"a0": 100, "a1": 200},  # Initial observations
                {"a0": 101, "a1": 201},
                {"a0": 102, "a1": 202},
                {"a0": 103, "a1": 203},
                {"a0": 104, "a1": 204},
                {"a0": 105, "a1": 205},
                {"a0": 106, "a1": 206},
            ],
            actions=[
                {"a0": 1, "a1": 11},
                {"a0": 2, "a1": 12},
                {"a0": 3, "a1": 13},
                {"a0": 4, "a1": 14},
                {"a0": 5, "a1": 15},
                {"a0": 6, "a1": 16},
            ],
            rewards=[
                {"a0": 0.1, "a1": 1.1},
                {"a0": 0.2, "a1": 1.2},
                {"a0": 0.3, "a1": 1.3},
                {"a0": 0.4, "a1": 1.4},
                {"a0": 0.5, "a1": 1.5},
                {"a0": 0.6, "a1": 1.6},
            ],
            extra_model_outputs=[
                {"a0": {SOME_KEY: 0.01}, "a1": {SOME_KEY: 1.01}},
                {"a0": {SOME_KEY: 0.02}, "a1": {SOME_KEY: 1.02}},
                {"a0": {SOME_KEY: 0.03}, "a1": {SOME_KEY: 1.03}},
                {"a0": {SOME_KEY: 0.04}, "a1": {SOME_KEY: 1.04}},
                {"a0": {SOME_KEY: 0.05}, "a1": {SOME_KEY: 1.05}},
                {"a0": {SOME_KEY: 0.06}, "a1": {SOME_KEY: 1.06}},
            ],
            len_lookback_buffer=0,
        )

        test_patterns = [
            # (description, new_data, indices)
            ("zero index", {"a0": 7353.0, "a1": 8353.0}, 0),
            ("single index", {"a0": 7353.0, "a1": 8353.0}, 2),
            ("negative index", {"a0": 7353.0, "a1": 8353.0}, -1),
            ("short list of indices", {"a0": [7353.0], "a1": [8353.0]}, [1]),
            (
                "long list of indices",
                {"a0": [73.0, 53.0, 35.0, 53.0], "a1": [83.0, 63.0, 45.0, 63.0]},
                [1, 2, 3, 4],
            ),
            ("short slice", {"a0": [7353.0], "a1": [8353.0]}, slice(2, 3)),
            (
                "long slice",
                {"a0": [7.0, 3.0, 5.0, 3.0], "a1": [17.0, 13.0, 15.0, 13.0]},
                slice(2, 6),
            ),
        ]

        # Test setters with all patterns
        numpy_episode = copy.deepcopy(episode).to_numpy()

        for e in [episode, numpy_episode]:
            print(f"Testing MultiAgent numpy'ized={e.is_numpy}...")
            for desc, new_data, indices in test_patterns:
                print(f"Testing MultiAgent {desc}...")

                expected_data = new_data
                test_new_data = new_data

                # Convert lists to numpy arrays for numpy episodes
                if e.is_numpy and isinstance(list(new_data.values())[0], list):
                    test_new_data = {
                        agent_id: np.array(agent_data)
                        for agent_id, agent_data in new_data.items()
                    }

                # Test set_observations
                e.set_observations(new_data=test_new_data, at_indices=indices)
                result = e.get_observations(indices)
                for agent_id in ["a0", "a1"]:
                    check(result[agent_id], expected_data[agent_id])

                # Test set_actions
                e.set_actions(new_data=test_new_data, at_indices=indices)
                result = e.get_actions(indices)
                for agent_id in ["a0", "a1"]:
                    check(result[agent_id], expected_data[agent_id])

                # Test set_rewards
                e.set_rewards(new_data=test_new_data, at_indices=indices)
                result = e.get_rewards(indices)
                for agent_id in ["a0", "a1"]:
                    check(result[agent_id], expected_data[agent_id])

                # Test set_extra_model_outputs
                # Note: We test this by directly checking the underlying agent episodes
                # since get_extra_model_outputs can be complex with indices
                e.set_extra_model_outputs(
                    key=SOME_KEY, new_data=test_new_data, at_indices=indices
                )

                # Verify that the setter worked by checking the individual agent episodes
                if desc in ["single index", "zero index"]:
                    for agent_id in ["a0", "a1"]:
                        actual_idx = e.agent_episodes[agent_id].t_started + indices
                        if actual_idx < len(
                            e.agent_episodes[agent_id].get_extra_model_outputs(SOME_KEY)
                        ):
                            check(
                                e.agent_episodes[agent_id].get_extra_model_outputs(
                                    SOME_KEY
                                )[actual_idx],
                                expected_data[agent_id],
                            )
                elif desc == "negative index":
                    for agent_id in ["a0", "a1"]:
                        agent_ep = e.agent_episodes[agent_id]
                        actual_idx = (
                            len(agent_ep.get_extra_model_outputs(SOME_KEY)) + indices
                        )
                        if actual_idx >= 0:
                            check(
                                agent_ep.get_extra_model_outputs(SOME_KEY)[actual_idx],
                                expected_data[agent_id],
                            )
                elif desc in ["long list of indices", "short list of indices"]:
                    for agent_id in ["a0", "a1"]:
                        agent_ep = e.agent_episodes[agent_id]
                        for i, expected_val in enumerate(expected_data[agent_id]):
                            actual_idx = agent_ep.t_started + indices[i]
                            if actual_idx < len(
                                agent_ep.get_extra_model_outputs(SOME_KEY)
                            ):
                                check(
                                    agent_ep.get_extra_model_outputs(SOME_KEY)[
                                        actual_idx
                                    ],
                                    expected_val,
                                )
                elif desc in ["long slice", "short slice"]:
                    for agent_id in ["a0", "a1"]:
                        agent_ep = e.agent_episodes[agent_id]
                        slice_indices = list(range(indices.start, indices.stop))
                        for i, expected_val in enumerate(expected_data[agent_id]):
                            actual_idx = agent_ep.t_started + slice_indices[i]
                            if actual_idx < len(
                                agent_ep.get_extra_model_outputs(SOME_KEY)
                            ):
                                check(
                                    agent_ep.get_extra_model_outputs(SOME_KEY)[
                                        actual_idx
                                    ],
                                    expected_val,
                                )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
