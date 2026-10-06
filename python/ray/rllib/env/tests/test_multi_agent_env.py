import random
import unittest

import gymnasium as gym
import numpy as np
import tree  # pip install dm-tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env import (
    MultiAgentEnv,
    MultiAgentEnvWrapper,
)
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.tests.test_rollout_worker import MockPolicy
from ray.rllib.examples._old_api_stack.policy.random_policy import RandomPolicy
from ray.rllib.examples.envs.classes.mock_env import MockEnv, MockEnv2
from ray.rllib.policy.sample_batch import (
    convert_ma_batch_to_sample_batch,
)
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.numpy import one_hot
from ray.rllib.utils.test_utils import check
from ray.tune.registry import register_env


class BasicMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps."""

    metadata = {
        "render.modes": ["rgb_array"],
    }
    render_mode = "rgb_array"

    def __init__(self, num):
        super().__init__()
        self.envs = [MockEnv(25) for _ in range(num)]
        self.agents = list(range(num))
        self.terminateds = set()
        self.truncateds = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self.resetted = False

    def reset(self, *, seed=None, options=None):
        # Call super's `reset()` method to set the np_random with the value of `seed`.
        # Note: This call to super does NOT return anything.
        super().reset(seed=seed)

        self.resetted = True
        self.terminateds = set()
        self.truncateds = set()
        reset_results = [a.reset() for a in self.envs]
        return (
            {i: oi[0] for i, oi in enumerate(reset_results)},
            {i: oi[1] for i, oi in enumerate(reset_results)},
        )

    def step(self, action_dict):
        obs, rew, terminated, truncated, info = {}, {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.envs[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)
        terminated["__all__"] = len(self.terminateds) == len(self.envs)
        truncated["__all__"] = len(self.truncateds) == len(self.envs)
        return obs, rew, terminated, truncated, info

    def render(self):
        # Just generate a random image here for demonstration purposes.
        # Also see `gym/envs/classic_control/cartpole.py` for
        # an example on how to use a Viewer object.
        return np.random.randint(0, 256, size=(200, 300, 3), dtype=np.uint8)


class EarlyDoneMultiAgent(MultiAgentEnv):
    """Env for testing when the env terminates (after agent 0 does)."""

    def __init__(self):
        super().__init__()
        self.envs = [MockEnv(3), MockEnv(5)]
        self.agents = list(range(len(self.envs)))
        self.terminateds = set()
        self.truncateds = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.terminateds = set()
        self.truncateds = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.envs):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_terminated[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % len(self.envs)
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.terminateds) != len(self.envs)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_terminated[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.envs[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        terminated = {self.i: self.last_terminated[self.i]}
        truncated = {self.i: self.last_truncated[self.i]}
        info = {self.i: self.last_info[self.i]}
        if terminated[self.i]:
            rew[self.i] = 0
            self.terminateds.add(self.i)
        if truncated[self.i]:
            rew[self.i] = 0
            self.truncateds.add(self.i)
        self.i = (self.i + 1) % len(self.envs)
        terminated["__all__"] = len(self.terminateds) == len(self.envs) - 1
        truncated["__all__"] = len(self.truncateds) == len(self.envs) - 1
        return obs, rew, terminated, truncated, info


class FlexAgentsMultiAgent(MultiAgentEnv):
    """Env of independent agents, each of which exits after n steps."""

    def __init__(self):
        super().__init__()
        self.envs = {}
        self.agents = []
        self.possible_agents = list(range(10000))  # Absolute max. number of agents.
        self.agentID = 0
        self.terminateds = set()
        self.truncateds = set()
        # All agents have the exact same spaces.
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self.resetted = False

    def spawn(self):
        # Spawn a new agent into the current episode.
        agentID = self.agentID
        self.envs[agentID] = MockEnv(25)
        self.agents.append(agentID)
        self.agentID += 1
        return agentID

    def kill(self, agent_id):
        del self.envs[agent_id]
        self.agents.remove(agent_id)

    def reset(self, *, seed=None, options=None):
        self.envs = {}
        self.agents.clear()
        self.spawn()
        self.resetted = True
        self.terminateds = set()
        self.truncateds = set()
        obs = {}
        infos = {}
        for i, a in self.envs.items():
            obs[i], infos[i] = a.reset()

        return obs, infos

    def step(self, action_dict):
        obs, rew, terminated, truncated, info = {}, {}, {}, {}, {}
        # Apply the actions.
        for i, action in action_dict.items():
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.envs[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)

        # Sometimes, add a new agent to the episode.
        if random.random() > 0.75 and len(action_dict) > 0:
            aid = self.spawn()
            obs[aid], rew[aid], terminated[aid], truncated[aid], info[aid] = self.envs[
                aid
            ].step(action)
            if terminated[aid]:
                self.terminateds.add(aid)
            if truncated[aid]:
                self.truncateds.add(aid)

        # Sometimes, kill an existing agent.
        if len(self.envs) > 1 and random.random() > 0.25:
            keys = list(self.envs.keys())
            aid = random.choice(keys)
            self.kill(aid)
            terminated[aid] = True
            self.terminateds.add(aid)

        terminated["__all__"] = len(self.terminateds) == len(self.envs)
        truncated["__all__"] = len(self.truncateds) == len(self.envs)
        return obs, rew, terminated, truncated, info


class SometimesZeroAgentsMultiAgent(MultiAgentEnv):
    """Multi-agent env in which sometimes, no agent acts.

    At each timestep, we determine, which agents emit observations (and thereby request
    actions). This set of observing (and action-requesting) agents could be anything
    from the empty set to the full set of all agents.

    For simplicity, all agents terminate after n timesteps.
    """

    def __init__(self, num=3):
        super().__init__()
        self.agents = list(range(num))
        self.envs = [MockEnv(25) for _ in range(self.num_agents)]
        self._observations = {}
        self._infos = {}
        self.terminateds = set()
        self.truncateds = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.terminateds = set()
        self.truncateds = set()
        self._observations = {}
        self._infos = {}
        for aid in self._get_random_agents():
            self._observations[aid], self._infos[aid] = self.envs[aid].reset()
        return self._observations, self._infos

    def step(self, action_dict):
        rew, terminated, truncated = {}, {}, {}
        # Step those agents, for which we have actions from RLlib.
        for aid, action in action_dict.items():
            (
                self._observations[aid],
                rew[aid],
                terminated[aid],
                truncated[aid],
                self._infos[aid],
            ) = self.envs[aid].step(action)
            if terminated[aid]:
                self.terminateds.add(aid)
            if truncated[aid]:
                self.truncateds.add(aid)
        # Must add the __all__ flag.
        terminated["__all__"] = len(self.terminateds) == self.num_agents
        truncated["__all__"] = len(self.truncateds) == self.num_agents

        # Select some of our observations to be published next (randomly).
        obs = {}
        infos = {}
        for aid in self._get_random_agents():
            if aid not in self._observations:
                self._observations[aid] = self.observation_space.sample()
                self._infos[aid] = {"fourty-two": 42}
            obs[aid] = self._observations.pop(aid)
            infos[aid] = self._infos.pop(aid)

        # Override some of the rewards. Rewards and dones should be always publishable,
        # even if no observation/action for an agent was sent/received.
        # An agent might get a reward because of the action of another agent. In this
        # case, the rewards for that agent are accumulated over the in-between timesteps
        # (in which the other agents step, but not this agent).
        for aid in self._get_random_agents():
            rew[aid] = np.random.rand()

        return obs, rew, terminated, truncated, infos

    def _get_random_agents(self):
        num_observing_agents = np.random.randint(self.num_agents)
        aids = np.random.permutation(self.num_agents)[:num_observing_agents]
        return {
            aid
            for aid in aids
            if aid not in self.terminateds and aid not in self.truncateds
        }


class RoundRobinMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 5 steps.

    On each step() of the env, only one agent takes an action."""

    def __init__(self, num, increment_obs=False):
        super().__init__()
        if increment_obs:
            # Observations are 0, 1, 2, 3... etc. as time advances
            self.envs = [MockEnv2(5) for _ in range(num)]
        else:
            # Observations are all zeros
            self.envs = [MockEnv(5) for _ in range(num)]
        self._agent_ids = set(range(num))
        self.terminateds = set()
        self.truncateds = set()

        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        self.num = num
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.terminateds = set()
        self.truncateds = set()

        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.envs):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_terminated[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % self.num
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.terminateds) != len(self.envs)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_terminated[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.envs[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        terminated = {self.i: self.last_terminated[self.i]}
        truncated = {self.i: self.last_truncated[self.i]}
        info = {self.i: self.last_info[self.i]}
        if terminated[self.i]:
            rew[self.i] = 0
            self.terminateds.add(self.i)
        if truncated[self.i]:
            self.truncateds.add(self.i)
        self.i = (self.i + 1) % self.num
        terminated["__all__"] = len(self.terminateds) == len(self.envs)
        truncated["__all__"] = len(self.truncateds) == len(self.envs)
        return obs, rew, terminated, truncated, info


class NestedMultiAgentEnv(MultiAgentEnv):
    DICT_SPACE = gym.spaces.Dict(
        {
            "sensors": gym.spaces.Dict(
                {
                    "position": gym.spaces.Box(low=-100, high=100, shape=(3,)),
                    "velocity": gym.spaces.Box(low=-1, high=1, shape=(3,)),
                    "front_cam": gym.spaces.Tuple(
                        (
                            gym.spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                            gym.spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                        )
                    ),
                    "rear_cam": gym.spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                }
            ),
            "inner_state": gym.spaces.Dict(
                {
                    "charge": gym.spaces.Discrete(100),
                    "job_status": gym.spaces.Dict(
                        {
                            "task": gym.spaces.Discrete(5),
                            "progress": gym.spaces.Box(low=0, high=100, shape=()),
                        }
                    ),
                }
            ),
        }
    )
    TUPLE_SPACE = gym.spaces.Tuple(
        [
            gym.spaces.Box(low=-100, high=100, shape=(3,)),
            gym.spaces.Tuple(
                (
                    gym.spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                    gym.spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                )
            ),
            gym.spaces.Discrete(5),
        ]
    )

    def __init__(self):
        super().__init__()
        self.observation_space = gym.spaces.Dict(
            {"dict_agent": self.DICT_SPACE, "tuple_agent": self.TUPLE_SPACE}
        )
        self.action_space = gym.spaces.Dict(
            {
                "dict_agent": gym.spaces.Discrete(1),
                "tuple_agent": gym.spaces.Discrete(1),
            }
        )
        self._agent_ids = {"dict_agent", "tuple_agent"}
        self.steps = 0
        self.DICT_SAMPLES = [self.DICT_SPACE.sample() for _ in range(10)]
        self.TUPLE_SAMPLES = [self.TUPLE_SPACE.sample() for _ in range(10)]

    def reset(self, *, seed=None, options=None):
        self.steps = 0
        return {
            "dict_agent": self.DICT_SAMPLES[0],
            "tuple_agent": self.TUPLE_SAMPLES[0],
        }, {}

    def step(self, actions):
        self.steps += 1
        obs = {
            "dict_agent": self.DICT_SAMPLES[self.steps],
            "tuple_agent": self.TUPLE_SAMPLES[self.steps],
        }
        rew = {
            "dict_agent": 0,
            "tuple_agent": 0,
        }
        terminateds = {"__all__": self.steps >= 5}
        truncateds = {"__all__": self.steps >= 5}
        infos = {
            "dict_agent": {},
            "tuple_agent": {},
        }
        return obs, rew, terminateds, truncateds, infos


class TestMultiAgentEnv(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_basic_mock(self):
        env = BasicMultiAgent(4)
        obs, info = env.reset()
        check(obs, {0: 0, 1: 0, 2: 0, 3: 0})
        for _ in range(24):
            obs, rew, done, truncated, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
            check(obs, {0: 0, 1: 0, 2: 0, 3: 0})
            check(rew, {0: 1, 1: 1, 2: 1, 3: 1})
            check(done, {0: False, 1: False, 2: False, 3: False, "__all__": False})
        obs, rew, done, truncated, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
        check(done, {0: True, 1: True, 2: True, 3: True, "__all__": True})

    def test_round_robin_mock(self):
        env = RoundRobinMultiAgent(2)
        obs, info = env.reset()
        check(obs, {0: 0})
        for _ in range(5):
            obs, rew, done, truncated, info = env.step({0: 0})
            check(obs, {1: 0})
            check(done["__all__"], False)
            obs, rew, done, truncated, info = env.step({1: 0})
            check(obs, {0: 0})
            check(done["__all__"], False)
        obs, rew, done, truncated, info = env.step({0: 0})
        check(done["__all__"], True)

    def test_no_reset_until_poll(self):
        env = MultiAgentEnvWrapper(lambda v: BasicMultiAgent(2), [], 1)
        self.assertFalse(env.get_sub_environments()[0].resetted)
        env.poll()
        self.assertTrue(env.get_sub_environments()[0].resetted)

    def test_vectorize_basic(self):
        env = MultiAgentEnvWrapper(lambda v: BasicMultiAgent(2), [], 2)
        obs, rew, terminateds, truncateds, _, _ = env.poll()
        check(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        check(rew, {0: {}, 1: {}})
        check(terminateds, {0: {"__all__": False}, 1: {"__all__": False}})
        check(truncateds, terminateds)
        for _ in range(24):
            env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            obs, rew, terminateds, truncateds, _, _ = env.poll()
            check(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            check(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
            check(
                terminateds,
                {
                    0: {0: False, 1: False, "__all__": False},
                    1: {0: False, 1: False, "__all__": False},
                },
            )
            check(truncateds, terminateds)
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, terminateds, truncateds, _, _ = env.poll()
        check(
            terminateds,
            {
                0: {0: True, 1: True, "__all__": True},
                1: {0: True, 1: True, "__all__": True},
            },
        )
        check(truncateds, terminateds)

        # Reset processing
        self.assertRaises(
            ValueError, lambda: env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        )
        init_obs, init_infos = env.try_reset(0)
        check(init_obs, {0: {0: 0, 1: 0}})
        check(init_infos, {0: {0: {}, 1: {}}})
        init_obs, init_infos = env.try_reset(1)
        check(init_obs, {1: {0: 0, 1: 0}})
        check(init_infos, {1: {0: {}, 1: {}}})

        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, terminateds, truncateds, _, _ = env.poll()
        check(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        check(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
        check(
            terminateds,
            {
                0: {0: False, 1: False, "__all__": False},
                1: {0: False, 1: False, "__all__": False},
            },
        )
        check(truncateds, terminateds)

    def test_vectorize_round_robin(self):
        env = MultiAgentEnvWrapper(lambda v: RoundRobinMultiAgent(2), [], 2)
        obs, rew, terminateds, truncateds, _, _ = env.poll()
        check(obs, {0: {0: 0}, 1: {0: 0}})
        check(rew, {0: {}, 1: {}})
        check(truncateds, {0: {"__all__": False}, 1: {"__all__": False}})
        env.send_actions({0: {0: 0}, 1: {0: 0}})
        obs, rew, terminateds, truncateds, _, _ = env.poll()
        check(obs, {0: {1: 0}, 1: {1: 0}})
        check(
            truncateds,
            {0: {"__all__": False, 1: False}, 1: {"__all__": False, 1: False}},
        )
        env.send_actions({0: {1: 0}, 1: {1: 0}})
        obs, rew, terminateds, truncateds, _, _ = env.poll()
        check(obs, {0: {0: 0}, 1: {0: 0}})
        check(
            truncateds,
            {0: {"__all__": False, 0: False}, 1: {"__all__": False, 0: False}},
        )

    def test_multi_agent_sample(self):
        def policy_mapping_fn(agent_id, episode, worker, **kwargs):
            return "p{}".format(agent_id % 2)

        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .env_runners(rollout_fragment_length=50, num_env_runners=0)
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=policy_mapping_fn,
            ),
        )
        batch = ev.sample()
        check(batch.count, 50)
        check(batch.policy_batches["p0"].count, 150)
        check(batch.policy_batches["p1"].count, 100)
        check(batch.policy_batches["p0"]["t"].tolist(), list(range(25)) * 6)

    def test_multi_agent_sample_sync_remote(self):
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            default_policy_class=MockPolicy,
            # This signature will raise a soft-deprecation warning due
            # to the new signature we are using (agent_id, episode, **kwargs),
            # but should not break this test.
            config=AlgorithmConfig()
            .env_runners(
                rollout_fragment_length=50,
                num_env_runners=0,
                num_envs_per_env_runner=4,
                remote_worker_envs=True,
                remote_env_batch_wait_ms=99999999,
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: (
                    "p{}".format(agent_id % 2)
                ),
            ),
        )
        batch = ev.sample()
        check(batch.count, 200)

    def test_multi_agent_sample_async_remote(self):
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .env_runners(
                rollout_fragment_length=50,
                num_env_runners=0,
                num_envs_per_env_runner=4,
                remote_worker_envs=True,
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: (
                    "p{}".format(agent_id % 2)
                ),
            ),
        )
        batch = ev.sample()
        check(batch.count, 200)

    def test_sample_from_early_done_env(self):
        ev = RolloutWorker(
            env_creator=lambda _: EarlyDoneMultiAgent(),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .env_runners(
                rollout_fragment_length=1,
                num_env_runners=0,
                batch_mode="complete_episodes",
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: (
                    "p{}".format(agent_id % 2)
                ),
            ),
        )
        # This used to raise an Error due to the EarlyDoneMultiAgent
        # terminating at e.g. agent0 w/o publishing the observation for
        # agent1 anymore. This limitation is fixed and an env may
        # terminate at any time (as well as return rewards for any agent
        # at any time, even when that agent doesn't have an obs returned
        # in the same call to `step()`).
        ma_batch = ev.sample()
        # Make sure that agents took the correct (alternating timesteps)
        # path. Except for the last timestep, where both agents got
        # terminated.
        ag0_ts = ma_batch.policy_batches["p0"]["t"]
        ag1_ts = ma_batch.policy_batches["p1"]["t"]
        self.assertTrue(np.all(np.abs(ag0_ts[:-1] - ag1_ts[:-1]) == 1.0))
        self.assertTrue(ag0_ts[-1] == ag1_ts[-1])

    def test_multi_agent_with_flex_agents(self):
        register_env("flex_agents_multi_agent", lambda _: FlexAgentsMultiAgent())
        config = (
            PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .environment("flex_agents_multi_agent")
            .env_runners(num_env_runners=0)
            .training(train_batch_size=50, minibatch_size=50, num_epochs=1)
        )
        algo = config.build()
        for i in range(10):
            result = algo.train()
            print(
                "Iteration {}, reward {}, timesteps {}".format(
                    i,
                    result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN],
                    result[NUM_ENV_STEPS_SAMPLED_LIFETIME],
                )
            )
        algo.stop()

    def test_multi_agent_with_sometimes_zero_agents_observing(self):
        register_env(
            "sometimes_zero_agents", lambda _: SometimesZeroAgentsMultiAgent(num=4)
        )
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=False,
                enable_env_runner_and_connector_v2=False,
            )
            .environment("sometimes_zero_agents")
            .env_runners(num_env_runners=0)
        )
        algo = config.build()
        for i in range(4):
            result = algo.train()
            print(
                "Iteration {}, reward {}, timesteps {}".format(
                    i,
                    result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN],
                    result[NUM_ENV_STEPS_SAMPLED_LIFETIME],
                )
            )
        algo.stop()

    def test_multi_agent_sample_round_robin(self):
        ev = RolloutWorker(
            env_creator=lambda _: RoundRobinMultiAgent(5, increment_obs=True),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .env_runners(
                rollout_fragment_length=50,
                num_env_runners=0,
            )
            .multi_agent(
                policies={"p0"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "p0",
            ),
        )
        batch = ev.sample()
        check(batch.count, 50)
        # since we round robin introduce agents into the env, some of the env
        # steps don't count as proper transitions
        check(batch.policy_batches["p0"].count, 42)
        check(
            batch.policy_batches["p0"]["obs"][:10],
            one_hot(np.array([0, 1, 2, 3, 4] * 2), 10),
        )
        check(
            batch.policy_batches["p0"]["new_obs"][:10],
            one_hot(np.array([1, 2, 3, 4, 5] * 2), 10),
        )
        check(
            batch.policy_batches["p0"]["rewards"].tolist()[:10],
            [100, 100, 100, 100, 0] * 2,
        )
        check(
            batch.policy_batches["p0"]["terminateds"].tolist()[:10],
            [False, False, False, False, True] * 2,
        )
        check(
            batch.policy_batches["p0"]["truncateds"].tolist()[:10],
            [False, False, False, False, True] * 2,
        )
        check(
            batch.policy_batches["p0"]["t"].tolist()[:10],
            [4, 9, 14, 19, 24, 5, 10, 15, 20, 25],
        )

    def test_custom_rnn_state_values(self):
        h = {"some": {"here": np.array([1.0, 2.0, 3.0])}}

        class StatefulPolicy(RandomPolicy):
            def compute_actions(
                self,
                obs_batch,
                state_batches=None,
                prev_action_batch=None,
                prev_reward_batch=None,
                episodes=None,
                explore=True,
                timestep=None,
                **kwargs,
            ):
                obs_shape = (len(obs_batch),)
                actions = np.zeros(obs_shape, dtype=np.int32)
                states = tree.map_structure(
                    lambda x: np.ones(obs_shape + x.shape) * x, h
                )

                return actions, [states], {}

            def get_initial_state(self):
                return [{}]  # empty dict

            def is_recurrent(self):
                return True

        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v1"),
            default_policy_class=StatefulPolicy,
            config=(
                AlgorithmConfig().env_runners(
                    rollout_fragment_length=5,
                    num_env_runners=0,
                )
                # Force `state_in_0` to be repeated every ts in the collected batch
                # (even though we don't even have a model that would care about this).
                .training(model={"max_seq_len": 1})
            ),
        )
        batch = ev.sample()
        batch = convert_ma_batch_to_sample_batch(batch)
        check(batch.count, 5)
        check(batch["state_in_0"][0], {})
        check(batch["state_out_0"][0], h)
        for i in range(1, 5):
            check(batch["state_in_0"][i], h)
            check(batch["state_out_0"][i], h)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
