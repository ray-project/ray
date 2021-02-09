from gym.spaces import Box, Dict, Discrete, Tuple, MultiDiscrete
import numpy as np
import unittest

import ray
from ray.rllib.agents.registry import get_trainer_class
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
    "vector2": Box(-1.0, 1.0, (5, 5), dtype=np.float32),
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
    config["train_batch_size"] = 10
    config["rollout_fragment_length"] = 10

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

        try:
            a = get_trainer_class(alg)(config=config, env=RandomEnv)
        except UnsupportedSpaceException:
            stat = "unsupported"
        else:
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
            a.stop()
        print(stat)

    frameworks = ("tf", "torch")
    if tfe:
        frameworks += ("tf2", "tfe")
    for _ in framework_iterator(config, frameworks=frameworks):
        # Zip through action- and obs-spaces.
        for a_name, o_name in zip(ACTION_SPACES_TO_TEST.keys(),
                                  OBSERVATION_SPACES_TO_TEST.keys()):
            _do_check(alg, config, a_name, o_name)
        # Do the remaining obs spaces.
        assert len(OBSERVATION_SPACES_TO_TEST) >= len(ACTION_SPACES_TO_TEST)
        for i, o_name in enumerate(OBSERVATION_SPACES_TO_TEST.keys()):
            if i < len(ACTION_SPACES_TO_TEST):
                continue
            _do_check(alg, config, "discrete", o_name)


class TestSupportedSpacesPG(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_a3c(self):
        config = {"num_workers": 1, "optimizer": {"grads_per_step": 1}}
        check_support("A3C", config, check_bounds=True)

    def test_appo(self):
        check_support("APPO", {"num_gpus": 0, "vtrace": False}, train=False)
        check_support("APPO", {"num_gpus": 0, "vtrace": True})

    def test_impala(self):
        check_support("IMPALA", {"num_gpus": 0})

    def test_ppo(self):
        config = {
            "num_workers": 0,
            "train_batch_size": 100,
            "rollout_fragment_length": 10,
            "num_sgd_iter": 1,
            "sgd_minibatch_size": 10,
        }
        check_support("PPO", config, check_bounds=True, tfe=True)

    def test_pg(self):
        config = {"num_workers": 1, "optimizer": {}}
        check_support("PG", config, train=False, check_bounds=True, tfe=True)


class TestSupportedSpacesOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

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

    def test_sac(self):
        check_support("SAC", {"buffer_size": 1000}, check_bounds=True)


class TestSupportedSpacesEvolutionAlgos(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_ars(self):
        check_support(
            "ARS", {
                "num_workers": 1,
                "noise_size": 1500000,
                "num_rollouts": 1,
                "rollouts_used": 1
            })

    def test_es(self):
        check_support(
            "ES", {
                "num_workers": 1,
                "noise_size": 1500000,
                "episodes_per_batch": 1,
                "train_batch_size": 1
            })


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(
        pytest.main(
            ["-v", __file__ + ("" if class_ is None else "::" + class_)]))
