import ray
import unittest
import numpy as np
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.env.mock_env import MockEnv3
from ray.rllib.policy import Policy
from ray.rllib.utils import override

NUM_STEPS = 25
NUM_AGENTS = 4


class LastInfoCallback(DefaultCallbacks):
    def __init__(self):
        super(LastInfoCallback, self).__init__()
        self.tc = unittest.TestCase()
        self.step = 0

    def on_episode_start(
        self, worker, base_env, policies, episode, env_index, **kwargs
    ):
        self.step = 0
        self._check_last_values(episode)

    def on_episode_step(self, worker, base_env, episode, env_index=None, **kwargs):
        self.step += 1
        self._check_last_values(episode)

    def on_episode_end(self, worker, base_env, policies, episode, **kwargs):
        self._check_last_values(episode)

    def _check_last_values(self, episode):
        last_obs = {
            k: np.where(v)[0].item() for k, v in episode._agent_to_last_obs.items()
        }
        last_info = episode._agent_to_last_info
        last_done = episode._agent_to_last_done
        last_action = episode._agent_to_last_action
        last_reward = {k: v[-1] for k, v in episode._agent_reward_history.items()}
        if self.step == 0:
            for last in [last_obs, last_info, last_done, last_action, last_reward]:
                self.tc.assertEqual(last, {})
        else:
            for agent in last_obs.keys():
                index = int(str(agent).replace("agent", ""))
                self.tc.assertEqual(last_obs[agent], self.step + index)
                self.tc.assertEqual(last_reward[agent], self.step + index)
                self.tc.assertEqual(last_done[agent], self.step == NUM_STEPS)
                if self.step == 1:
                    self.tc.assertEqual(last_action[agent], 0)
                else:
                    self.tc.assertEqual(last_action[agent], self.step + index - 1)
                self.tc.assertEqual(last_info[agent]["timestep"], self.step + index)


class EchoPolicy(Policy):
    @override(Policy)
    def compute_actions(
        self,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        episodes=None,
        explore=None,
        timestep=None,
        **kwargs
    ):
        return obs_batch.argmax(axis=1), [], {}


class EpisodeEnv(MultiAgentEnv):
    def __init__(self, episode_length, num):
        super().__init__()
        self._skip_env_checking = True
        self.agents = [MockEnv3(episode_length) for _ in range(num)]
        self.dones = set()
        self.observation_space = self.agents[0].observation_space
        self.action_space = self.agents[0].action_space

    def reset(self):
        self.dones = set()
        return {i: a.reset() for i, a in enumerate(self.agents)}

    def step(self, action_dict):
        obs, rew, done, info = {}, {}, {}, {}
        print("ACTIONDICT IN ENV\n", action_dict)
        for i, action in action_dict.items():
            obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
            obs[i] = obs[i] + i
            rew[i] = rew[i] + i
            info[i]["timestep"] = info[i]["timestep"] + i
            if done[i]:
                self.dones.add(i)
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


class TestEpisodeLastValues(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=1)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_single_agent_env(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv3(NUM_STEPS),
            default_policy_class=EchoPolicy,
            # Episode only works with env runner v1.
            config=AlgorithmConfig()
            .rollouts(enable_connectors=False)
            .rollouts(num_rollout_workers=0)
            .callbacks(LastInfoCallback),
        )
        ev.sample()

    def test_multi_agent_env(self):
        ev = RolloutWorker(
            env_creator=lambda _: EpisodeEnv(NUM_STEPS, NUM_AGENTS),
            default_policy_class=EchoPolicy,
            # Episode only works with env runner v1.
            config=AlgorithmConfig()
            .rollouts(enable_connectors=False)
            .rollouts(num_rollout_workers=0)
            .callbacks(LastInfoCallback)
            .multi_agent(
                policies={str(agent_id) for agent_id in range(NUM_AGENTS)},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: (
                    str(agent_id)
                ),
            ),
        )
        ev.sample()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
