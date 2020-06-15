from gym.spaces import Box, Dict, Discrete, Tuple, MultiDiscrete
import numpy as np
import unittest

import ray
from ray.rllib.agents.registry import get_agent_class
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork as FCNetV2
from ray.rllib.models.tf.visionnet import VisionNetwork as VisionNetV2
from ray.rllib.models.torch.visionnet import VisionNetwork as TorchVisionNetV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFCNetV2
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.test_utils import framework_iterator

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
    "dict": Dict({
        "action_choice": Discrete(3),
        "parameters": Box(-1.0, 1.0, (1, ), dtype=np.float32),
        "yet_another_nested_dict": Dict({
            "a": Tuple([Discrete(2), Discrete(3)])
        })
    }),
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


def check_support(alg, config, train=True, check_bounds=False, tfe=False):
    config["log_level"] = "ERROR"

    def _do_check(alg, config, a_name, o_name):
        fw = config["framework"]
        action_space = ACTION_SPACES_TO_TEST[a_name]
        obs_space = OBSERVATION_SPACES_TO_TEST[o_name]
        print("=== Testing {} (fw={}) A={} S={} ===".format(
            alg, fw, action_space, obs_space))
        config.update(
            dict(
                env_config=dict(
                    action_space=action_space,
                    observation_space=obs_space,
                    reward_space=Box(1.0, 1.0, shape=(), dtype=np.float32),
                    p_done=1.0,
                    check_action_bounds=check_bounds)))
        stat = "ok"
        a = None
        try:
            if alg == "SAC":
                config["use_state_preprocessor"] = o_name in ["atari", "image"]
            a = get_agent_class(alg)(config=config, env=RandomEnv)
            if alg not in ["DDPG", "ES", "ARS", "SAC"]:
                if o_name in ["atari", "image"]:
                    if fw == "torch":
                        assert isinstance(a.get_policy().model,
                                          TorchVisionNetV2)
                    else:
                        assert isinstance(a.get_policy().model, VisionNetV2)
                elif o_name in ["vector", "vector2"]:
                    if fw == "torch":
                        assert isinstance(a.get_policy().model, TorchFCNetV2)
                    else:
                        assert isinstance(a.get_policy().model, FCNetV2)
            if train:
                a.train()
        except UnsupportedSpaceException:
            stat = "unsupported"
        finally:
            if a:
                try:
                    a.stop()
                except Exception as e:
                    print("Ignoring error stopping agent", e)
                    pass
        print(stat)

    frameworks = ("tf", "torch")
    if tfe:
        frameworks += ("tfe", )
    for _ in framework_iterator(config, frameworks=frameworks):
        # Check all action spaces (using a discrete obs-space).
        for a_name, action_space in ACTION_SPACES_TO_TEST.items():
            _do_check(alg, config, a_name, "discrete")
        # Check all obs spaces (using a supported action-space).
        for o_name, obs_space in OBSERVATION_SPACES_TO_TEST.items():
            a_name = "discrete" if alg not in ["DDPG", "SAC"] else "vector"
            _do_check(alg, config, a_name, o_name)


class TestSupportedSpaces(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_a3c(self):
        config = {"num_workers": 1, "optimizer": {"grads_per_step": 1}}
        check_support("A3C", config, check_bounds=True)

    def test_appo(self):
        check_support("APPO", {"num_gpus": 0, "vtrace": False}, train=False)
        check_support("APPO", {"num_gpus": 0, "vtrace": True})

    def test_ars(self):
        check_support(
            "ARS", {
                "num_workers": 1,
                "noise_size": 1500000,
                "num_rollouts": 1,
                "rollouts_used": 1
            })

    def test_ddpg(self):
        check_support(
            "DDPG", {
                "exploration_config": {
                    "ou_base_scale": 100.0
                },
                "timesteps_per_iteration": 1,
                "buffer_size": 1000,
                "use_state_preprocessor": True,
            },
            check_bounds=True)

    def test_dqn(self):
        config = {"timesteps_per_iteration": 1, "buffer_size": 1000}
        check_support("DQN", config, tfe=True)

    def test_es(self):
        check_support(
            "ES", {
                "num_workers": 1,
                "noise_size": 1500000,
                "episodes_per_batch": 1,
                "train_batch_size": 1
            })

    def test_impala(self):
        check_support("IMPALA", {"num_gpus": 0})

    def test_ppo(self):
        config = {
            "num_workers": 1,
            "num_sgd_iter": 1,
            "train_batch_size": 10,
            "rollout_fragment_length": 10,
            "sgd_minibatch_size": 1,
        }
        check_support("PPO", config, check_bounds=True, tfe=True)

    def test_pg(self):
        config = {"num_workers": 1, "optimizer": {}}
        check_support("PG", config, train=False, check_bounds=True, tfe=True)

    def test_sac(self):
        check_support("SAC", {"buffer_size": 1000}, check_bounds=True)


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
