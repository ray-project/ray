import numpy as np
import os
import shutil
import unittest

import ray
from ray.air.checkpoint import Checkpoint
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.files import dict_contents_to_dir
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def export_test(alg_name, framework="tf", multi_agent=False):
    cls, config = get_algorithm_class(alg_name, return_config=True)
    config["framework"] = framework
    if "DDPG" in alg_name or "SAC" in alg_name:
        algo = cls(config=config, env="Pendulum-v1")
        test_obs = np.array([[0.1, 0.2, 0.3]])
    else:
        if multi_agent:
            config["multiagent"] = {
                "policies": {"pol1", "pol2"},
                "policy_mapping_fn": (
                    lambda agent_id, episode, worker, **kwargs:
                    "pol1" if agent_id == "agent1" else "pol2"
                )
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

    print("Exporting policy checkpoint", alg_name, export_dir)
    if multi_agent:
        algo.export_policy_checkpoint(export_dir, policy_id="pol1")
        algo.export_policy_checkpoint(export_dir + "_2", policy_id="pol2")

    else:
        algo.export_policy_checkpoint(export_dir, policy_id=DEFAULT_POLICY_ID)
    checkpoint = Checkpoint.from_directory(export_dir)

    checkpoint_dict = checkpoint.to_dict()
    # Only if keras model gets properly saved by the Policy's get_state() method.
    # NOTE: This is not the case (yet) for TF Policies like SAC or DQN, which use
    # ModelV2s that have more than one keras "base_model" properties in them. For
    # example, SACTfModel contains `q_net` and `action_model`, both of which have
    # their own `base_model`.
    if "model" in checkpoint_dict:
        dict_contents_to_dir(checkpoint_dict["model"], export_dir)

        # Test loading exported model and perform forward pass.
        if framework == "torch":
            model = torch.load(os.path.join(export_dir, "model.pickle"))
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
            model = tf.saved_model.load(export_dir)
            assert model
            results = model(tf.convert_to_tensor(test_obs, dtype=tf.float32))
            assert len(results) == 2
            assert results[0].shape == (1, 2)
            # TODO (sven): Make non-RNN models NOT return states (empty list).
            assert results[1].shape == (1, 1)  # dummy state-out

    shutil.rmtree(export_dir)
    if multi_agent:
        shutil.rmtree(export_dir + "_2")


class TestAlgorithmSave(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_export_appo_multi_agent(self):
        for fw in framework_iterator():
            export_test("APPO", fw)

    def test_export_ppo(self):
        for fw in framework_iterator():
            export_test("PPO", fw, multi_agent=True)



if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
