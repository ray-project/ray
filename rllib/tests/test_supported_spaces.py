import gym
from gym.spaces import Box, Discrete, Tuple, Dict, MultiDiscrete
from gym.envs.registration import EnvSpec
import numpy as np
import unittest
import traceback

import ray
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.agents.registry import get_agent_class
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork as FCNetV2
from ray.rllib.models.tf.visionnet_v2 import VisionNetwork as VisionNetV2
from ray.rllib.models.torch.visionnet import VisionNetwork as TorchVisionNetV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFCNetV2
from ray.rllib.tests.test_multi_agent_env import MultiCartpole, \
    MultiMountainCar
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.tune.registry import register_env

tf = try_import_tf()

ACTION_SPACES_TO_TEST = {
    "discrete": Discrete(5),
    "vector": Box(-1.0, 1.0, (5, ), dtype=np.float32),
    "vector2": Box(-1.0, 1.0, (
        5,
        5,
    ), dtype=np.float32),
    "multidiscrete": MultiDiscrete([1, 2, 3, 4]),
    "tuple": Tuple(
        [Discrete(2),
         Discrete(3),
         Box(-1.0, 1.0, (5, ), dtype=np.float32)]),
}

OBSERVATION_SPACES_TO_TEST = {
    "discrete": Discrete(5),
    "vector": Box(-1.0, 1.0, (5, ), dtype=np.float32),
    "vector2": Box(-1.0, 1.0, (5, 5), dtype=np.float32),
    "image": Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32),
    "atari": Box(-1.0, 1.0, (210, 160, 3), dtype=np.float32),
    "tuple": Tuple([Discrete(10),
                    Box(-1.0, 1.0, (5, ), dtype=np.float32)]),
    "dict": Dict({
        "task": Discrete(10),
        "position": Box(-1.0, 1.0, (5, ), dtype=np.float32),
    }),
}


def make_stub_env(action_space, obs_space, check_action_bounds):
    class StubEnv(gym.Env):
        def __init__(self):
            self.action_space = action_space
            self.observation_space = obs_space
            self.spec = EnvSpec("StubEnv-v0")

        def reset(self):
            sample = self.observation_space.sample()
            return sample

        def step(self, action):
            if check_action_bounds and not self.action_space.contains(action):
                raise ValueError("Illegal action for {}: {}".format(
                    self.action_space, action))
            if (isinstance(self.action_space, Tuple)
                    and len(action) != len(self.action_space.spaces)):
                raise ValueError("Illegal action for {}: {}".format(
                    self.action_space, action))
            return self.observation_space.sample(), 1, True, {}

    return StubEnv


def check_support(alg, config, stats, check_bounds=False, name=None):
    covered_a = set()
    covered_o = set()
    config["log_level"] = "ERROR"
    first_error = None
    torch = config.get("use_pytorch", False)
    for a_name, action_space in ACTION_SPACES_TO_TEST.items():
        for o_name, obs_space in OBSERVATION_SPACES_TO_TEST.items():
            print("=== Testing {} (torch={}) A={} S={} ===".format(
                alg, torch, action_space, obs_space))
            stub_env = make_stub_env(action_space, obs_space, check_bounds)
            register_env("stub_env", lambda c: stub_env())
            stat = "ok"
            a = None
            try:
                if a_name in covered_a and o_name in covered_o:
                    stat = "skip"  # speed up tests by avoiding full grid
                # TODO(sven): Add necessary torch distributions.
                elif torch and a_name in ["tuple", "multidiscrete"]:
                    stat = "unsupported"
                else:
                    a = get_agent_class(alg)(config=config, env="stub_env")
                    if alg not in ["DDPG", "ES", "ARS", "SAC"]:
                        if o_name in ["atari", "image"]:
                            if torch:
                                assert isinstance(a.get_policy().model,
                                                  TorchVisionNetV2)
                            else:
                                assert isinstance(a.get_policy().model,
                                                  VisionNetV2)
                        elif o_name in ["vector", "vector2"]:
                            if torch:
                                assert isinstance(a.get_policy().model,
                                                  TorchFCNetV2)
                            else:
                                assert isinstance(a.get_policy().model,
                                                  FCNetV2)
                    a.train()
                    covered_a.add(a_name)
                    covered_o.add(o_name)
            except UnsupportedSpaceException:
                stat = "unsupported"
            except Exception as e:
                stat = "ERROR"
                print(e)
                print(traceback.format_exc())
                first_error = first_error if first_error is not None else e
            finally:
                if a:
                    try:
                        a.stop()
                    except Exception as e:
                        print("Ignoring error stopping agent", e)
                        pass
            print(stat)
            print()
            stats[name or alg, a_name, o_name] = stat

    # If anything happened, raise error.
    if first_error is not None:
        raise first_error


def check_support_multiagent(alg, config):
    register_env("multi_mountaincar", lambda _: MultiMountainCar(2))
    register_env("multi_cartpole", lambda _: MultiCartpole(2))
    config["log_level"] = "ERROR"
    if "DDPG" in alg:
        a = get_agent_class(alg)(config=config, env="multi_mountaincar")
    else:
        a = get_agent_class(alg)(config=config, env="multi_cartpole")
    try:
        a.train()
    finally:
        a.stop()


class ModelSupportedSpaces(unittest.TestCase):
    stats = {}

    def setUp(self):
        ray.init(num_cpus=4, ignore_reinit_error=True)

    def tearDown(self):
        ray.shutdown()

    def test_a3c(self):
        config = {"num_workers": 1, "optimizer": {"grads_per_step": 1}}
        check_support("A3C", config, self.stats, check_bounds=True)
        config["use_pytorch"] = True
        check_support("A3C", config, self.stats, check_bounds=True)

    def test_appo(self):
        check_support("APPO", {"num_gpus": 0, "vtrace": False}, self.stats)
        check_support(
            "APPO", {
                "num_gpus": 0,
                "vtrace": True
            },
            self.stats,
            name="APPO-vt")

    def test_ars(self):
        check_support(
            "ARS", {
                "num_workers": 1,
                "noise_size": 10000000,
                "num_rollouts": 1,
                "rollouts_used": 1
            }, self.stats)

    def test_ddpg(self):
        check_support(
            "DDPG", {
                "exploration_config": {
                    "ou_base_scale": 100.0
                },
                "timesteps_per_iteration": 1,
                "use_state_preprocessor": True,
            },
            self.stats,
            check_bounds=True)

    def test_dqn(self):
        check_support("DQN", {"timesteps_per_iteration": 1}, self.stats)

    def test_es(self):
        check_support(
            "ES", {
                "num_workers": 1,
                "noise_size": 10000000,
                "episodes_per_batch": 1,
                "train_batch_size": 1
            }, self.stats)

    def test_impala(self):
        check_support("IMPALA", {"num_gpus": 0}, self.stats)

    def test_ppo(self):
        config = {
            "num_workers": 1,
            "num_sgd_iter": 1,
            "train_batch_size": 10,
            "rollout_fragment_length": 10,
            "sgd_minibatch_size": 1,
        }
        check_support("PPO", config, self.stats, check_bounds=True)
        config["use_pytorch"] = True
        check_support("PPO", config, self.stats, check_bounds=True)

    def test_pg(self):
        config = {"num_workers": 1, "optimizer": {}}
        check_support("PG", config, self.stats, check_bounds=True)
        config["use_pytorch"] = True
        check_support("PG", config, self.stats, check_bounds=True)

    def test_sac(self):
        check_support("SAC", {}, self.stats, check_bounds=True)

    def test_a3c_multiagent(self):
        check_support_multiagent("A3C", {
            "num_workers": 1,
            "optimizer": {
                "grads_per_step": 1
            }
        })

    def test_apex_multiagent(self):
        check_support_multiagent(
            "APEX", {
                "num_workers": 2,
                "timesteps_per_iteration": 1000,
                "num_gpus": 0,
                "min_iter_time_s": 1,
                "learning_starts": 1000,
                "target_network_update_freq": 100,
            })

    def test_apex_ddpg_multiagent(self):
        check_support_multiagent(
            "APEX_DDPG", {
                "num_workers": 2,
                "timesteps_per_iteration": 1000,
                "num_gpus": 0,
                "min_iter_time_s": 1,
                "learning_starts": 1000,
                "target_network_update_freq": 100,
                "use_state_preprocessor": True,
            })

    def test_ddpg_multiagent(self):
        check_support_multiagent("DDPG", {
            "timesteps_per_iteration": 1,
            "use_state_preprocessor": True,
        })

    def test_dqn_multiagent(self):
        check_support_multiagent("DQN", {"timesteps_per_iteration": 1})

    def test_impala_multiagent(self):
        check_support_multiagent("IMPALA", {"num_gpus": 0})

    def test_pg_multiagent(self):
        check_support_multiagent("PG", {"num_workers": 1, "optimizer": {}})

    def test_ppo_multiagent(self):
        check_support_multiagent(
            "PPO", {
                "num_workers": 1,
                "num_sgd_iter": 1,
                "train_batch_size": 10,
                "rollout_fragment_length": 10,
                "sgd_minibatch_size": 1,
            })


if __name__ == "__main__":
    import pytest
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--smoke":
        ACTION_SPACES_TO_TEST = {
            "discrete": Discrete(5),
        }
        OBSERVATION_SPACES_TO_TEST = {
            "vector": Box(0.0, 1.0, (5, ), dtype=np.float32),
            "atari": Box(0.0, 1.0, (210, 160, 3), dtype=np.float32),
        }

    sys.exit(pytest.main(["-v", __file__]))
