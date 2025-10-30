#!/usr/bin/env python

import os
import shutil
import unittest

import numpy as np

import ray
import ray._common
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_torch
from ray.tune.registry import get_trainable_cls

torch, _ = try_import_torch()


def export_test(
    alg_name,
    framework="tf",
    multi_agent=False,
):
    cls = get_trainable_cls(alg_name)
    config = cls.get_default_config()
    config.api_stack(
        enable_rl_module_and_learner=False,
        enable_env_runner_and_connector_v2=False,
    )
    config.framework(framework)
    # Switch on saving native DL-framework (tf, torch) model files.
    config.checkpointing(export_native_model_files=True)
    if "SAC" in alg_name:
        algo = config.build(env="Pendulum-v1")
        test_obs = np.array([[0.1, 0.2, 0.3]])
    else:
        if multi_agent:
            config.multi_agent(
                policies={"pol1", "pol2"},
                policy_mapping_fn=(
                    lambda agent_id, episode, worker, **kwargs: "pol1"
                    if agent_id == "agent1"
                    else "pol2"
                ),
            ).environment(MultiAgentCartPole, env_config={"num_agents": 2})
        else:
            config.environment("CartPole-v1")
        algo = config.build()
        test_obs = np.array([[0.1, 0.2, 0.3, 0.4]])

    export_dir = os.path.join(
        ray._common.utils.get_user_temp_dir(), "export_dir_%s" % alg_name
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

    if os.path.exists(export_dir):
        shutil.rmtree(export_dir)
        if multi_agent:
            shutil.rmtree(export_dir + "_2")

    algo.stop()


class TestExportCheckpointAndModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_export_appo(self):
        export_test("APPO", "torch")

    def test_export_ppo(self):
        export_test("PPO", "torch")

    def test_export_ppo_multi_agent(self):
        export_test("PPO", "torch", multi_agent=True)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
