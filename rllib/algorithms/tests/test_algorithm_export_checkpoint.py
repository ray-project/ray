import numpy as np
import os
import shutil
import unittest

import ray
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def save_test(alg_name, framework="tf", multi_agent=False):
    cls, config = get_algorithm_class(alg_name, return_config=True)

    config["framework"] = framework

    # Switch on saving native DL-framework (tf, torch) model files.
    config["export_native_model_files"] = True

    if "DDPG" in alg_name or "SAC" in alg_name:
        algo = cls(config=config, env="Pendulum-v1")
        test_obs = np.array([[0.1, 0.2, 0.3]])
    else:
        if multi_agent:
            config["multiagent"] = {
                "policies": {"pol1", "pol2"},
                "policy_mapping_fn": (
                    lambda agent_id, episode, worker, **kwargs: "pol1"
                    if agent_id == "agent1"
                    else "pol2"
                ),
            }
            config["env"] = MultiAgentCartPole
            config["env_config"] = {
                "num_agents": 2,
            }
        else:
            config["env"] = "CartPole-v0"
        algo = cls(config=config)
        test_obs = np.array([[0.1, 0.2, 0.3, 0.4]])

    export_dir = os.path.join(
        ray._private.utils.get_user_temp_dir(), "export_dir_%s" % alg_name
    )

    print("Exporting algo checkpoint", alg_name, export_dir)
    export_dir = algo.save(export_dir)
    model_dir = os.path.join(
        export_dir,
        "policies",
        "pol1" if multi_agent else DEFAULT_POLICY_ID,
        "model",
    )

    # Test loading exported model and perform forward pass.
    if framework == "torch":
        filename = os.path.join(model_dir, "model.pt")
        model = torch.load(filename)
        assert model
        results = model(
            input_dict={"obs": torch.from_numpy(test_obs)},
            # TODO (sven): Make non-RNN models NOT expect these args at all.
            state=[torch.tensor(0)],  # dummy value
            seq_lens=torch.tensor(0),  # dummy value
        )
        assert len(results) == 2
        assert results[0].shape == (1, 2)
        assert results[1] == [torch.tensor(0)]  # dummy
    else:
        model = tf.saved_model.load(model_dir)
        assert model
        results = model(tf.convert_to_tensor(test_obs, dtype=tf.float32))
        assert len(results) == 2
        assert results[0].shape == (1, 2)
        # TODO (sven): Make non-RNN models NOT return states (empty list).
        assert results[1].shape == (1, 1)  # dummy state-out

    shutil.rmtree(export_dir)


class TestAlgorithmSave(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_save_appo_multi_agent(self):
        for fw in framework_iterator():
            save_test("APPO", fw, multi_agent=True)

    def test_save_ppo(self):
        for fw in framework_iterator():
            save_test("PPO", fw)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
