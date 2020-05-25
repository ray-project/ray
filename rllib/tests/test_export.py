#!/usr/bin/env python

import os
import shutil
import unittest

import ray
from ray.rllib.agents.registry import get_agent_class
from ray.tune.trial import ExportFormat

CONFIGS = {
    "A3C": {
        "explore": False,
        "num_workers": 1,
        "framework": "tf",
    },
    "APEX_DDPG": {
        "explore": False,
        "observation_filter": "MeanStdFilter",
        "num_workers": 2,
        "min_iter_time_s": 1,
        "optimizer": {
            "num_replay_buffer_shards": 1,
        },
        "framework": "tf",
    },
    "ARS": {
        "explore": False,
        "num_rollouts": 10,
        "num_workers": 2,
        "noise_size": 2500000,
        "observation_filter": "MeanStdFilter",
        "framework": "tf",
    },
    "DDPG": {
        "explore": False,
        "timesteps_per_iteration": 100,
        "framework": "tf",
    },
    "DQN": {
        "explore": False,
        "framework": "tf",
    },
    "ES": {
        "explore": False,
        "episodes_per_batch": 10,
        "train_batch_size": 100,
        "num_workers": 2,
        "noise_size": 2500000,
        "observation_filter": "MeanStdFilter",
        "framework": "tf",
    },
    "PPO": {
        "explore": False,
        "num_sgd_iter": 5,
        "train_batch_size": 1000,
        "num_workers": 2,
        "framework": "tf",
    },
    "SAC": {
        "explore": False,
        "framework": "tf",
    },
}


def export_test(alg_name, failures):
    def valid_tf_model(model_dir):
        return os.path.exists(os.path.join(model_dir, "saved_model.pb")) \
            and os.listdir(os.path.join(model_dir, "variables"))

    def valid_tf_checkpoint(checkpoint_dir):
        return os.path.exists(os.path.join(checkpoint_dir, "model.meta")) \
            and os.path.exists(os.path.join(checkpoint_dir, "model.index")) \
            and os.path.exists(os.path.join(checkpoint_dir, "checkpoint"))

    cls = get_agent_class(alg_name)
    if "DDPG" in alg_name or "SAC" in alg_name:
        algo = cls(config=CONFIGS[alg_name], env="Pendulum-v0")
    else:
        algo = cls(config=CONFIGS[alg_name], env="CartPole-v0")

    for _ in range(1):
        res = algo.train()
        print("current status: " + str(res))

    export_dir = os.path.join(ray.utils.get_user_temp_dir(),
                              "export_dir_%s" % alg_name)
    print("Exporting model ", alg_name, export_dir)
    algo.export_policy_model(export_dir)
    if not valid_tf_model(export_dir):
        failures.append(alg_name)
    shutil.rmtree(export_dir)

    print("Exporting checkpoint", alg_name, export_dir)
    algo.export_policy_checkpoint(export_dir)
    if not valid_tf_checkpoint(export_dir):
        failures.append(alg_name)
    shutil.rmtree(export_dir)

    print("Exporting default policy", alg_name, export_dir)
    algo.export_model([ExportFormat.CHECKPOINT, ExportFormat.MODEL],
                      export_dir)
    if not valid_tf_model(os.path.join(export_dir, ExportFormat.MODEL)) \
            or not valid_tf_checkpoint(os.path.join(export_dir,
                                                    ExportFormat.CHECKPOINT)):
        failures.append(alg_name)
    shutil.rmtree(export_dir)


class TestExport(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(
            num_cpus=10, object_store_memory=1e9, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_export(self):
        failures = []
        for name in ["A3C", "DQN", "DDPG", "PPO", "SAC"]:
            export_test(name, failures)
        assert not failures, failures
        print("All export tests passed!")


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
