#!/usr/bin/env python

import numpy as np
import os
import shutil
import unittest

import ray
from ray.air.checkpoint import Checkpoint
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.utils.files import dict_contents_to_dir
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune.experiment.trial import ExportFormat

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


def export_test(alg_name, failures, framework="tf"):
    def valid_tf_model(model_dir):
        return os.path.exists(os.path.join(model_dir, "saved_model.pb")) and os.listdir(
            os.path.join(model_dir, "variables")
        )

    def valid_tf_checkpoint(checkpoint_dir):
        return (
            os.path.exists(os.path.join(checkpoint_dir, "model.meta"))
            and os.path.exists(os.path.join(checkpoint_dir, "model.index"))
            and os.path.exists(os.path.join(checkpoint_dir, "checkpoint"))
        )

    cls = get_algorithm_class(alg_name)
    config = CONFIGS[alg_name].copy()
    config["framework"] = framework
    if "DDPG" in alg_name or "SAC" in alg_name:
        algo = cls(config=config, env="Pendulum-v1")
        test_obs = np.array([[0.1, 0.2, 0.3]])
    else:
        algo = cls(config=config, env="CartPole-v0")
        test_obs = np.array([[0.1, 0.2, 0.3, 0.4]])

    for _ in range(1):
        res = algo.train()
        print("current status: " + str(res))

    export_dir = os.path.join(
        ray._private.utils.get_user_temp_dir(), "export_dir_%s" % alg_name
    )

    print("Exporting policy (`default_policy`) checkpoint", alg_name, export_dir)
    algo.export_policy_checkpoint(export_dir)
    if framework == "tf" and not valid_tf_checkpoint(export_dir):
        failures.append(alg_name)
    checkpoint = Checkpoint.from_directory(export_dir)
    if framework != "tf":
        checkpoint_dict = checkpoint.to_dict()
        dict_contents_to_dir(checkpoint_dict["model"], export_dir)
    if framework == "torch":
        model = torch.load(os.path.join(export_dir, "model.pickle"))
        assert model
        # Test forward pass.
        results = model(
            input_dict={"obs": torch.from_numpy(test_obs)},
            # TODO (sven): Make non-RNN models NOT expect these args at all.
            state=[torch.tensor(0)],  # dummy value
            seq_lens=torch.tensor(0),  # dummy value
        )
        assert len(results) == 2
        assert results[0].shape == (1, 2)
        assert results[1] == [torch.tensor(0)]  # dummy
    elif framework == "tf2":
        model = tf.saved_model.load(export_dir)
        assert model
        results = model(tf.convert_to_tensor(test_obs, dtype=tf.float32))
        assert len(results) == 2
        assert results[0].shape == (1, 2)
        # TODO (sven): Make non-RNN models NOT return states (empty list).
        assert results[1].shape == (1, 1)  # dummy state-out

    shutil.rmtree(export_dir)

    print("Exporting policy (`default_policy`) model ", alg_name, export_dir)
    algo.export_policy_model(export_dir)
    if framework in ["tf", "tf2", "tfe"] and not valid_tf_model(export_dir):
        failures.append(alg_name)

    # Test loading the exported model.
    if framework == "torch":
        model = torch.load(os.path.join(export_dir, ExportFormat.MODEL, "model.pt"))
        assert model
        # Test forward pass.
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

    algo.stop()


class TestExportModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_export_a3c(self):
        failures = []
        export_test("A3C", failures, "tf")
        assert not failures, failures

    def test_export_ddpg(self):
        failures = []
        export_test("DDPG", failures, "tf")
        assert not failures, failures

    def test_export_dqn(self):
        failures = []
        export_test("DQN", failures, "tf")
        assert not failures, failures

    def test_export_ppo(self):
        failures = []
        for fw in framework_iterator():
            export_test("PPO", failures, fw)
        assert not failures, failures

    def test_export_sac(self):
        failures = []
        export_test("SAC", failures, "tf")
        assert not failures, failures


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
