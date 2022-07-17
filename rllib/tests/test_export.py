#!/usr/bin/env python

import os
import shutil
import unittest

import ray
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.utils.framework import try_import_tf
from ray.tune.experiment.trial import ExportFormat

tf1, tf, tfv = try_import_tf()

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
    else:
        algo = cls(config=config, env="CartPole-v0")

    for _ in range(1):
        res = algo.train()
        print("current status: " + str(res))

    export_dir = os.path.join(
        ray._private.utils.get_user_temp_dir(), "export_dir_%s" % alg_name
    )
    print("Exporting model ", alg_name, export_dir)
    algo.export_policy_model(export_dir)
    if framework == "tf" and not valid_tf_model(export_dir):
        failures.append(alg_name)
    shutil.rmtree(export_dir)

    if framework == "tf":
        print("Exporting checkpoint", alg_name, export_dir)
        algo.export_policy_checkpoint(export_dir)
        if framework == "tf" and not valid_tf_checkpoint(export_dir):
            failures.append(alg_name)
        shutil.rmtree(export_dir)

        print("Exporting default policy", alg_name, export_dir)
        algo.export_model([ExportFormat.CHECKPOINT, ExportFormat.MODEL], export_dir)
        if not valid_tf_model(
            os.path.join(export_dir, ExportFormat.MODEL)
        ) or not valid_tf_checkpoint(os.path.join(export_dir, ExportFormat.CHECKPOINT)):
            failures.append(alg_name)

        # Test loading the exported model.
        model = tf.saved_model.load(os.path.join(export_dir, ExportFormat.MODEL))
        assert model

        shutil.rmtree(export_dir)
    algo.stop()


class TestExport(unittest.TestCase):
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
        export_test("PPO", failures, "torch")
        export_test("PPO", failures, "tf")
        assert not failures, failures

    def test_export_sac(self):
        failures = []
        export_test("SAC", failures, "tf")
        assert not failures, failures
        print("All export tests passed!")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
