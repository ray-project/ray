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

# Keep a set of all RLlib algos that support the RLModule API.
# For these algos we need to disable the RLModule API in the config for the purpose of
# this test. This test is made for the ModelV2 API which is not the same as RLModule.
RLMODULE_SUPPORTED_ALGOS = {"PPO"}


def save_test(alg_name, framework="tf", multi_agent=False):
    config = (
        get_trainable_cls(alg_name)
        .get_default_config()
        .api_stack(
            enable_env_runner_and_connector_v2=False, enable_rl_module_and_learner=False
        )
        .framework(framework)
        # Switch on saving native DL-framework (tf, torch) model files.
        .checkpointing(export_native_model_files=True)
    )

    if alg_name in RLMODULE_SUPPORTED_ALGOS:
        config.api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )

    if multi_agent:
        config.multi_agent(
            policies={"pol1", "pol2"},
            policy_mapping_fn=(
                lambda agent_id, episode, worker, **kwargs: "pol1"
                if agent_id == "agent1"
                else "pol2"
            ),
        )
        config.environment(MultiAgentCartPole, env_config={"num_agents": 2})
    else:
        config.environment("CartPole-v1")
    algo = config.build()
    test_obs = np.array([[0.1, 0.2, 0.3, 0.4]])

    export_dir = os.path.join(
        ray._common.utils.get_user_temp_dir(), "export_dir_%s" % alg_name
    )

    algo.train()
    print("Exporting algo checkpoint", alg_name, export_dir)
    export_dir = algo.save(export_dir).checkpoint.path
    model_dir = os.path.join(
        export_dir,
        "policies",
        "pol1" if multi_agent else DEFAULT_POLICY_ID,
        "model",
    )

    # Test loading exported model and perform forward pass.
    filename = os.path.join(model_dir, "model.pt")
    model = torch.load(filename, weights_only=False)
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

    shutil.rmtree(export_dir)


class TestAlgorithmSave(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_save_appo_multi_agent(self):
        save_test("APPO", "torch", multi_agent=True)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
