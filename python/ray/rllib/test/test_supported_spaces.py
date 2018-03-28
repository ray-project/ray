import unittest
import traceback

import gym
from gym.spaces import Box, Discrete, Tuple
from gym.envs.registration import EnvSpec
import numpy as np

import ray
from ray.rllib.agent import get_agent_class
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.tune.registry import register_env

ACTION_SPACES_TO_TEST = {
    "discrete": Discrete(5),
    "vector": Box(0.0, 1.0, (5,), dtype=np.float32),
    "simple_tuple": Tuple([
        Box(0.0, 1.0, (5,), dtype=np.float32),
        Box(0.0, 1.0, (5,), dtype=np.float32)]),
    "implicit_tuple": [
        Box(0.0, 1.0, (5,), dtype=np.float32),
        Box(0.0, 1.0, (5,), dtype=np.float32)],
}

OBSERVATION_SPACES_TO_TEST = {
    "discrete": Discrete(5),
    "vector": Box(0.0, 1.0, (5,), dtype=np.float32),
    "image": Box(0.0, 1.0, (80, 80, 1), dtype=np.float32),
    "atari": Box(0.0, 1.0, (210, 160, 3), dtype=np.float32),
    "atari_ram": Box(0.0, 1.0, (128,), dtype=np.float32),
    "simple_tuple": Tuple([
        Box(0.0, 1.0, (5,), dtype=np.float32),
        Box(0.0, 1.0, (5,), dtype=np.float32)]),
    "mixed_tuple": Tuple([
        Discrete(10),
        Box(0.0, 1.0, (5,), dtype=np.float32)]),
}

# (alg, action_space, obs_space)
KNOWN_FAILURES = [
    # TODO(ekl) multiagent support for a3c
    ("A3C", "implicit_tuple", "atari"),
    ("A3C", "implicit_tuple", "atari_ram"),
    ("A3C", "implicit_tuple", "discrete"),
    ("A3C", "implicit_tuple", "image"),
    ("A3C", "implicit_tuple", "mixed_tuple"),
    ("A3C", "implicit_tuple", "simple_tuple"),
    ("A3C", "implicit_tuple", "vector"),
    ("A3C", "mixed_tuple", "atari"),
    ("A3C", "mixed_tuple", "atari_ram"),
    ("A3C", "mixed_tuple", "discrete"),
    ("A3C", "mixed_tuple", "image"),
    ("A3C", "mixed_tuple", "mixed_tuple"),
    ("A3C", "mixed_tuple", "simple_tuple"),
    ("A3C", "mixed_tuple", "vector"),
    ("A3C", "simple_tuple", "atari"),
    ("A3C", "simple_tuple", "atari_ram"),
    ("A3C", "simple_tuple", "discrete"),
    ("A3C", "simple_tuple", "image"),
    ("A3C", "simple_tuple", "mixed_tuple"),
    ("A3C", "simple_tuple", "simple_tuple"),
    ("A3C", "simple_tuple", "vector"),
]


def make_stub_env(action_space, obs_space):
    class StubEnv(gym.Env):
        def __init__(self):
            self.action_space = action_space
            self.observation_space = obs_space
            self.spec = EnvSpec("StubEnv-v0")

        def reset(self):
            sample = self.observation_space.sample()
            return sample

        def step(self, action):
            return self.observation_space.sample(), 1, True, {}

    return StubEnv


def check_support(alg, config, stats):
    for a_name, action_space in ACTION_SPACES_TO_TEST.items():
        for o_name, obs_space in OBSERVATION_SPACES_TO_TEST.items():
            print("=== Testing", alg, action_space, obs_space, "===")
            stub_env = make_stub_env(action_space, obs_space)
            register_env(
                "stub_env", lambda c: stub_env())
            stat = "ok"
            a = None
            try:
                a = get_agent_class(alg)(config=config, env="stub_env")
                a.train()
            except UnsupportedSpaceException as e:
                stat = "unsupported"
            except Exception as e:
                stat = "ERROR"
                print(e)
                print(traceback.format_exc())
            finally:
                if a:
                    try:
                        a.stop()
                    except Exception as e:
                        print("Ignoring error stopping agent", e)
                        pass
            print(stat)
            print()
            stats[alg, a_name, o_name] = stat


class ModelSupportedSpaces(unittest.TestCase):
    def testAll(self):
        ray.init()
        stats = {}
        check_support("DQN", {"timesteps_per_iteration": 1}, stats)
        check_support(
            "A3C", {"num_workers": 1, "optimizer": {"grads_per_step": 1}},
            stats)
        check_support(
            "PPO",
            {"num_workers": 1, "num_sgd_iter": 1, "timesteps_per_batch": 1,
             "devices": ["/cpu:0"], "min_steps_per_task": 1,
             "sgd_batchsize": 1},
            stats)
        check_support(
            "ES",
            {"num_workers": 1, "noise_size": 10000000,
             "episodes_per_batch": 1, "timesteps_per_batch": 1},
            stats)
        check_support(
            "PG",
            {"num_workers": 1, "optimizer": {}},
            stats)
        num_unexpected_errors = 0
        num_unexpected_success = 0
        for (alg, a_name, o_name), stat in sorted(stats.items()):
            if stat in ["ok", "unsupported"]:
                if (alg, a_name, o_name) in KNOWN_FAILURES:
                    num_unexpected_success += 1
            else:
                if (alg, a_name, o_name) not in KNOWN_FAILURES:
                    num_unexpected_errors += 1
            print(
                alg, "action_space", a_name, "obs_space", o_name,
                "result", stat)
        self.assertEqual(num_unexpected_errors, 0)
        self.assertEqual(num_unexpected_success, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
