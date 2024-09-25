import gymnasium as gym
import numpy as np
import random
import tree  # pip install dm-tree
import unittest

import ray
from ray.tune.registry import register_env
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env import (
    make_multi_agent,
    MultiAgentEnv,
    MultiAgentEnvWrapper,
)
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.rollout_worker import get_global_worker, RolloutWorker
from ray.rllib.evaluation.tests.test_rollout_worker import MockPolicy
from ray.rllib.examples._old_api_stack.policy.random_policy import RandomPolicy
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.examples.envs.classes.mock_env import MockEnv, MockEnv2
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.policy.sample_batch import (
    convert_ma_batch_to_sample_batch,
)
from ray.rllib.tests.test_nested_observation_spaces import NestedMultiAgentEnv
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.numpy import one_hot
from ray.rllib.utils.test_utils import check


class BasicMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps."""

    metadata = {
        "render.modes": ["rgb_array"],
    }
    render_mode = "rgb_array"

    def __init__(self, num):
        super().__init__()
        self.agents = [MockEnv(25) for _ in range(num)]
        self._agent_ids = set(range(num))
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
        reset_results = [a.reset() for a in self.agents]
        return (
            {i: oi[0] for i, oi in enumerate(reset_results)},
            {i: oi[1] for i, oi in enumerate(reset_results)},
        )

    def step(self, action_dict):
        obs, rew, terminated, truncated, info = {}, {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.agents[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)
        terminated["__all__"] = len(self.terminateds) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
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
        self.agents = [MockEnv(3), MockEnv(5)]
        self._agent_ids = set(range(len(self.agents)))
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
        for i, a in enumerate(self.agents):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_terminated[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % len(self.agents)
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.terminateds) != len(self.agents)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_terminated[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.agents[i].step(action)
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
        self.i = (self.i + 1) % len(self.agents)
        terminated["__all__"] = len(self.terminateds) == len(self.agents) - 1
        truncated["__all__"] = len(self.truncateds) == len(self.agents) - 1
        return obs, rew, terminated, truncated, info


class FlexAgentsMultiAgent(MultiAgentEnv):
    """Env of independent agents, each of which exits after n steps."""

    def __init__(self):
        super().__init__()
        self.agents = {}
        self._agent_ids = set()
        self.agentID = 0
        self.terminateds = set()
        self.truncateds = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self.resetted = False

    def spawn(self):
        # Spawn a new agent into the current episode.
        agentID = self.agentID
        self.agents[agentID] = MockEnv(25)
        self._agent_ids.add(agentID)
        self.agentID += 1
        return agentID

    def reset(self, *, seed=None, options=None):
        self.agents = {}
        self._agent_ids = set()
        self.spawn()
        self.resetted = True
        self.terminateds = set()
        self.truncateds = set()
        obs = {}
        infos = {}
        for i, a in self.agents.items():
            obs[i], infos[i] = a.reset()

        return obs, infos

    def step(self, action_dict):
        obs, rew, terminated, truncated, info = {}, {}, {}, {}, {}
        # Apply the actions.
        for i, action in action_dict.items():
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.agents[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)

        # Sometimes, add a new agent to the episode.
        if random.random() > 0.75 and len(action_dict) > 0:
            i = self.spawn()
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.agents[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)

        # Sometimes, kill an existing agent.
        if len(self.agents) > 1 and random.random() > 0.25:
            keys = list(self.agents.keys())
            key = random.choice(keys)
            terminated[key] = True
            del self.agents[key]

        terminated["__all__"] = len(self.terminateds) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
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
        self.num_agents = num
        self.agents = [MockEnv(25) for _ in range(self.num_agents)]
        self._agent_ids = set(range(self.num_agents))
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
            self._observations[aid], self._infos[aid] = self.agents[aid].reset()
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
            ) = self.agents[aid].step(action)
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
            self.agents = [MockEnv2(5) for _ in range(num)]
        else:
            # Observations are all zeros
            self.agents = [MockEnv(5) for _ in range(num)]
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
        for i, a in enumerate(self.agents):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_terminated[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % self.num
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.terminateds) != len(self.agents)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_terminated[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.agents[i].step(action)
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
        terminated["__all__"] = len(self.terminateds) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, terminated, truncated, info


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
            .environment("flex_agents_multi_agent")
            .env_runners(num_env_runners=0)
            .framework("tf")
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
            .environment("sometimes_zero_agents")
            .env_runners(num_env_runners=0, enable_connectors=True)
            .framework("tf")
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
                # TODO: avnishn automatically infer this.
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

    def test_returning_model_based_rollouts_data(self):
        # TODO(avnishn): This test only works with the old api

        class ModelBasedPolicy(DQNTFPolicy):
            def compute_actions_from_input_dict(
                self, input_dict, explore=None, timestep=None, episodes=None, **kwargs
            ):
                obs_batch = input_dict["obs"]
                # In policy loss initialization phase, no episodes are passed
                # in.
                if episodes is not None:
                    # Pretend we did a model-based rollout and want to return
                    # the extra trajectory.
                    env_id = episodes[0].env_id
                    fake_eps = Episode(
                        episodes[0].policy_map,
                        episodes[0].policy_mapping_fn,
                        lambda: None,
                        lambda x: None,
                        env_id,
                    )
                    builder = get_global_worker().sampler.sample_collector
                    agent_id = "extra_0"
                    policy_id = "p1"  # use p1 so we can easily check it
                    builder.add_init_obs(
                        episode=fake_eps,
                        agent_id=agent_id,
                        policy_id=policy_id,
                        env_id=env_id,
                        init_obs=obs_batch[0],
                        init_infos={},
                    )
                    for t in range(4):
                        builder.add_action_reward_next_obs(
                            episode_id=fake_eps.episode_id,
                            agent_id=agent_id,
                            env_id=env_id,
                            policy_id=policy_id,
                            agent_done=t == 3,
                            values=dict(
                                t=t,
                                actions=0,
                                rewards=0,
                                terminateds=False,
                                truncateds=t == 3,
                                infos={},
                                new_obs=obs_batch[0],
                            ),
                        )
                    batch = builder.postprocess_episode(episode=fake_eps, build=True)
                    episodes[0].add_extra_batch(batch)

                # Just return zeros for actions
                return [0] * len(obs_batch), [], {}

        ev = RolloutWorker(
            env_creator=lambda _: MultiAgentCartPole({"num_agents": 2}),
            default_policy_class=ModelBasedPolicy,
            config=DQNConfig()
            .framework("tf")
            .env_runners(
                rollout_fragment_length=5,
                num_env_runners=0,
                enable_connectors=False,  # only works with old episode API
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "p0",
            ),
        )
        batch = ev.sample()
        # 5 environment steps (rollout_fragment_length).
        check(batch.count, 5)
        # 10 agent steps for p0: 2 agents, both using p0 as their policy.
        check(batch.policy_batches["p0"].count, 10)
        # 20 agent steps for p1: Each time both(!) agents takes 1 step,
        # p1 takes 4: 5 (rollout-fragment length) * 4 = 20
        check(batch.policy_batches["p1"].count, 20)

    def test_train_multi_agent_cartpole_single_policy(self):
        n = 10
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": n})
        )
        config = (
            PPOConfig()
            .environment("multi_agent_cartpole")
            .env_runners(num_env_runners=0)
            .framework("tf")
        )

        algo = config.build()
        for i in range(50):
            result = algo.train()
            print(
                "Iteration {}, reward {}, timesteps {}".format(
                    i,
                    result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN],
                    result[NUM_ENV_STEPS_SAMPLED_LIFETIME],
                )
            )
            if result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= 50 * n:
                algo.stop()
                return
        raise Exception("failed to improve reward")

    def test_train_multi_agent_cartpole_multi_policy(self):
        n = 10
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": n})
        )

        def gen_policy():
            config = PPOConfig.overrides(
                gamma=random.choice([0.5, 0.8, 0.9, 0.95, 0.99]),
                lr=random.choice([0.001, 0.002, 0.003]),
            )
            return PolicySpec(config=config)

        config = (
            PPOConfig()
            .environment("multi_agent_cartpole")
            .env_runners(num_env_runners=0)
            .multi_agent(
                policies={
                    "policy_1": gen_policy(),
                    "policy_2": gen_policy(),
                },
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: (
                    "policy_1"
                ),
            )
            .framework("tf")
            .training(train_batch_size=50, minibatch_size=50, num_epochs=1)
        )

        algo = config.build()
        # Just check that it runs without crashing
        for i in range(10):
            result = algo.train()
            print(
                "Iteration {}, reward {}, timesteps {}".format(
                    i,
                    result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN],
                    result[NUM_ENV_STEPS_SAMPLED_LIFETIME],
                )
            )
        self.assertTrue(
            algo.compute_single_action([0, 0, 0, 0], policy_id="policy_1") in [0, 1]
        )
        self.assertTrue(
            algo.compute_single_action([0, 0, 0, 0], policy_id="policy_2") in [0, 1]
        )
        self.assertRaisesRegex(
            KeyError,
            "not found in PolicyMap",
            lambda: algo.compute_single_action([0, 0, 0, 0], policy_id="policy_3"),
        )

    def test_space_in_preferred_format(self):
        env = NestedMultiAgentEnv()
        action_space_in_preferred_format = (
            env._check_if_action_space_maps_agent_id_to_sub_space()
        )
        obs_space_in_preferred_format = (
            env._check_if_obs_space_maps_agent_id_to_sub_space()
        )
        assert action_space_in_preferred_format, "Act space is not in preferred format."
        assert obs_space_in_preferred_format, "Obs space is not in preferred format."

        env2 = make_multi_agent("CartPole-v1")()
        action_spaces_in_preferred_format = (
            env2._check_if_action_space_maps_agent_id_to_sub_space()
        )
        obs_space_in_preferred_format = (
            env2._check_if_obs_space_maps_agent_id_to_sub_space()
        )
        assert (
            action_spaces_in_preferred_format
        ), "Action space should be in preferred format but isn't."
        assert (
            obs_space_in_preferred_format
        ), "Observation space should be in preferred format but isn't."

    def test_spaces_sample_contain_in_preferred_format(self):
        env = NestedMultiAgentEnv()
        # this environment has spaces that are in the preferred format
        # for multi-agent environments where the spaces are dict spaces
        # mapping agent-ids to sub-spaces
        obs = env.observation_space_sample()
        assert env.observation_space_contains(
            obs
        ), "Observation space does not contain obs"

        action = env.action_space_sample()
        assert env.action_space_contains(action), "Action space does not contain action"

    def test_spaces_sample_contain_not_in_preferred_format(self):
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        # this environment has spaces that are not in the preferred format
        # for multi-agent environments where the spaces not in the preferred
        # format, users must override the observation_space_contains,
        # action_space_contains observation_space_sample,
        # and action_space_sample methods in order to do proper checks
        obs = env.observation_space_sample()
        assert env.observation_space_contains(
            obs
        ), "Observation space does not contain obs"
        action = env.action_space_sample()
        assert env.action_space_contains(action), "Action space does not contain action"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
