#!/usr/bin/env python

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

CONFIGS = {
    "A3C": {
        "explore": False,
        "num_workers": 1,
    },
    "APEX_DDPG": {
        "explore": False,
        "observation_filter": "MeanStdFilter",
        "num_workers": 2,
        "min_time_s_per_iteration": 1,
        "optimizer": {
            "num_replay_buffer_shards": 1,
        },
    },
    "ARS": {
        "explore": False,
        "num_rollouts": 10,
        "num_workers": 2,
        "noise_size": 2500000,
        "observation_filter": "MeanStdFilter",
    },
    "DDPG": {
        "explore": False,
        "min_sample_timesteps_per_iteration": 100,
    },
    "DQN": {
        "explore": False,
    },
    "ES": {
        "explore": False,
        "episodes_per_batch": 10,
        "train_batch_size": 100,
        "num_workers": 2,
        "noise_size": 2500000,
        "observation_filter": "MeanStdFilter",
    },
    "PPO": {
        "explore": False,
        "num_sgd_iter": 5,
        "train_batch_size": 1000,
        "num_workers": 2,
    },
    "SAC": {
        "explore": False,
    },
}


def export_test(
    alg_name,
    framework="tf",
    multi_agent=False,
    tf_expected_to_work=True,
):
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

    print("Exporting policy checkpoint", alg_name, export_dir)
    if multi_agent:
        algo.export_policy_checkpoint(export_dir, policy_id="pol1")

    else:
        algo.export_policy_checkpoint(export_dir, policy_id=DEFAULT_POLICY_ID)

    # Only if keras model gets properly saved by the Policy's get_state() method.
    # NOTE: This is not the case (yet) for TF Policies like SAC or DQN, which use
    # ModelV2s that have more than one keras "base_model" properties in them. For
    # example, SACTfModel contains `q_net` and `action_model`, both of which have
    # their own `base_model`.

    # Test loading exported model and perform forward pass.
    if framework == "torch":
        model = torch.load(os.path.join(export_dir, "model", "model.pt"))
        assert model
        results = model(
            input_dict={"obs": torch.from_numpy(test_obs)},
            # TODO (sven): Make non-RNN models NOT expect these args at all.
            state=[torch.tensor(0)],  # dummy value
            seq_lens=torch.tensor(0),  # dummy value
        )
        assert len(results) == 2
        assert results[0].shape in [(1, 2), (1, 3), (1, 256)], results[0].shape
        assert results[1] == [torch.tensor(0)]  # dummy

    # Only if keras model gets properly saved by the Policy's export_model() method.
    # NOTE: This is not the case (yet) for TF Policies like SAC, which use ModelV2s
    # that have more than one keras "base_model" properties in them. For example,
    # SACTfModel contains `q_net` and `action_model`, both of which have their own
    # `base_model`.
    elif tf_expected_to_work:
        model = tf.saved_model.load(os.path.join(export_dir, "model"))
        assert model
        results = model(tf.convert_to_tensor(test_obs, dtype=tf.float32))
        assert len(results) == 2
        assert results[0].shape in [(1, 2), (1, 3), (1, 256)], results[0].shape
        # TODO (sven): Make non-RNN models NOT return states (empty list).
        assert results[1].shape == (1, 1), results[1].shape  # dummy state-out

    shutil.rmtree(export_dir)

    print("Exporting policy (`default_policy`) model ", alg_name, export_dir)
    # Expect an error due to not being able to identify, which exact keras
    # base_model to export (e.g. SACTfModel has two keras.Models in it:
    # self.q_net.base_model and self.action_model.base_model).
    if multi_agent:
        algo.export_policy_model(export_dir, policy_id="pol1")
        algo.export_policy_model(export_dir + "_2", policy_id="pol2")
    else:
        algo.export_policy_model(export_dir, policy_id=DEFAULT_POLICY_ID)

    # Test loading exported model and perform forward pass.
    if framework == "torch":
        filename = os.path.join(export_dir, "model.pt")
        model = torch.load(filename)
        assert model
        results = model(
            input_dict={"obs": torch.from_numpy(test_obs)},
            # TODO (sven): Make non-RNN models NOT expect these args at all.
            state=[torch.tensor(0)],  # dummy value
            seq_lens=torch.tensor(0),  # dummy value
        )
        assert len(results) == 2
        assert results[0].shape in [(1, 2), (1, 3), (1, 256)], results[0].shape
        assert results[1] == [torch.tensor(0)]  # dummy

    # Only if keras model gets properly saved by the Policy's export_model() method.
    # NOTE: This is not the case (yet) for TF Policies like SAC, which use ModelV2s
    # that have more than one keras "base_model" properties in them. For example,
    # SACTfModel contains `q_net` and `action_model`, both of which have their own
    # `base_model`.
    elif tf_expected_to_work:
        model = tf.saved_model.load(export_dir)
        assert model
        results = model(tf.convert_to_tensor(test_obs, dtype=tf.float32))
        assert len(results) == 2
        assert results[0].shape in [(1, 2), (1, 3), (1, 256)], results[0].shape
        # TODO (sven): Make non-RNN models NOT return states (empty list).
        assert results[1].shape == (1, 1), results[1].shape  # dummy state-out

    if os.path.exists(export_dir):
        shutil.rmtree(export_dir)
        if multi_agent:
            shutil.rmtree(export_dir + "_2")

    algo.stop()


class TestExportCheckpointAndModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_export_a3c(self):
        for fw in framework_iterator():
            export_test("A3C", fw)

    def test_export_appo(self):
        for fw in framework_iterator():
            export_test("APPO", fw)

    def test_export_ppo(self):
        for fw in framework_iterator():
            export_test("PPO", fw)

    def test_export_ppo_multi_agent(self):
        for fw in framework_iterator():
            export_test("PPO", fw, multi_agent=True)

    def test_export_sac(self):
        for fw in framework_iterator():
            export_test("SAC", fw, tf_expected_to_work=False)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
