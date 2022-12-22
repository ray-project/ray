import numpy as np
import os
import shutil
import unittest

import ray
import ray.rllib.algorithms.pg as pg
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune.registry import get_trainable_cls

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def save_test(alg_name, framework="tf", multi_agent=False):
    cls = get_trainable_cls(alg_name)
    config = (
        cls.get_default_config().framework(framework)
        # Switch on saving native DL-framework (tf, torch) model files.
        .checkpointing(export_native_model_files=True)
    )

    if "DDPG" in alg_name or "SAC" in alg_name:
        config.environment("Pendulum-v1")
        algo = config.build()
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
            )
            config.environment(MultiAgentCartPole, env_config={"num_agents": 2})
        else:
            config.environment("CartPole-v1")
        algo = config.build()
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


class TestAlgorithmCheckpointing(unittest.TestCase):
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

    def test_counters_and_global_timestep_after_checkpoint(self):
        # We expect algorithm to not start counters from zero after loading a
        # checkpoint on a fresh Algorithm instance.
        config = pg.PGConfig().environment(env="CartPole-v1")
        algo = config.build()

        self.assertTrue(all(c == 0 for c in algo._counters.values()))
        algo.train()
        self.assertTrue((all(c != 0 for c in algo._counters.values())))
        self.assertTrue(algo.workers.local_worker().global_vars["timestep"] > 0)
        self.assertTrue(algo.get_policy().global_timestep > 0)
        counter_values = list(algo._counters.values())
        state = algo.__getstate__()
        algo.stop()

        algo2 = config.build()
        self.assertTrue(all(c == 0 for c in algo2._counters.values()))
        self.assertTrue(algo2.workers.local_worker().global_vars["timestep"] == 0)
        algo2.__setstate__(state)
        counter_values2 = list(algo2._counters.values())
        self.assertEqual(counter_values, counter_values2)
        # Also check global_vars `timesteps`.
        self.assertEqual(
            algo.workers.local_worker().global_vars["timestep"],
            algo2.workers.local_worker().global_vars["timestep"],
        )
        self.assertTrue(algo2.get_policy().global_timestep > 0)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
