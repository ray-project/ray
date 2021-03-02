import numpy as np
import pickle
import unittest

import ray
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.examples.env.debug_counter_env import DebugCounterEnv
from ray.rllib.examples.models.rnn_spy_model import RNNSpyModel
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.tune.registry import register_env


class TestLSTMUtils(unittest.TestCase):
    def test_basic(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 1, 1, 1, 1, 1, 1]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4)
        self.assertEqual([f.tolist() for f in f_pad], [
            [101, 102, 103, 0, 201, 202, 203, 204, 205, 0, 0, 0],
            [[101], [102], [103], [0], [201], [202], [203], [204], [205], [0],
             [0], [0]],
        ])
        self.assertEqual([s.tolist() for s in s_init], [[209, 109, 105]])
        self.assertEqual(seq_lens.tolist(), [3, 4, 1])

    def test_multi_dim(self):
        eps_ids = [1, 1, 1]
        agent_ids = [1, 1, 1]
        obs = np.ones((84, 84, 4))
        f = [[obs, obs * 2, obs * 3]]
        s = [[209, 208, 207]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4)
        self.assertEqual([f.tolist() for f in f_pad], [
            np.array([obs, obs * 2, obs * 3]).tolist(),
        ])
        self.assertEqual([s.tolist() for s in s_init], [[209]])
        self.assertEqual(seq_lens.tolist(), [3])

    def test_batch_id(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        batch_ids = [1, 1, 2, 2, 3, 3, 4, 4]
        agent_ids = [1, 1, 1, 1, 1, 1, 1, 1]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        _, _, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=batch_ids,
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4)
        self.assertEqual(seq_lens.tolist(), [2, 1, 1, 2, 2])

    def test_multi_agent(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 2, 1, 1, 2, 2, 3]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4,
            dynamic_max=False)
        self.assertEqual(seq_lens.tolist(), [2, 1, 2, 2, 1])
        self.assertEqual(len(f_pad[0]), 20)
        self.assertEqual(len(s_init[0]), 5)

    def test_dynamic_max_len(self):
        eps_ids = [5, 2, 2]
        agent_ids = [2, 2, 2]
        f = [[1, 1, 1]]
        s = [[1, 1, 1]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4)
        self.assertEqual([f.tolist() for f in f_pad], [[1, 0, 1, 1]])
        self.assertEqual([s.tolist() for s in s_init], [[1, 1]])
        self.assertEqual(seq_lens.tolist(), [1, 2])


class TestRNNSequencing(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(num_cpus=4)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_simple_optimizer_sequencing(self):
        ModelCatalog.register_custom_model("rnn", RNNSpyModel)
        register_env("counter", lambda _: DebugCounterEnv())
        ppo = PPOTrainer(
            env="counter",
            config={
                "num_workers": 0,
                "rollout_fragment_length": 10,
                "train_batch_size": 10,
                "sgd_minibatch_size": 10,
                "num_sgd_iter": 1,
                "model": {
                    "custom_model": "rnn",
                    "max_seq_len": 4,
                    "vf_share_layers": True,
                },
                "framework": "tf",
            })
        ppo.train()
        ppo.train()

        batch0 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_0"))
        self.assertEqual(
            batch0["sequences"].tolist(),
            [[[0], [1], [2], [3]], [[4], [5], [6], [7]], [[8], [9], [0], [0]]])
        self.assertEqual(batch0["seq_lens"].tolist(), [4, 4, 2])
        self.assertEqual(batch0["state_in"][0][0].tolist(), [0, 0, 0])
        self.assertEqual(batch0["state_in"][1][0].tolist(), [0, 0, 0])
        self.assertGreater(abs(np.sum(batch0["state_in"][0][1])), 0)
        self.assertGreater(abs(np.sum(batch0["state_in"][1][1])), 0)
        self.assertTrue(
            np.allclose(batch0["state_in"][0].tolist()[1:],
                        batch0["state_out"][0].tolist()[:-1]))
        self.assertTrue(
            np.allclose(batch0["state_in"][1].tolist()[1:],
                        batch0["state_out"][1].tolist()[:-1]))

        batch1 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_1"))
        self.assertEqual(batch1["sequences"].tolist(), [
            [[10], [11], [12], [13]],
            [[14], [0], [0], [0]],
            [[0], [1], [2], [3]],
            [[4], [0], [0], [0]],
        ])
        self.assertEqual(batch1["seq_lens"].tolist(), [4, 1, 4, 1])
        self.assertEqual(batch1["state_in"][0][2].tolist(), [0, 0, 0])
        self.assertEqual(batch1["state_in"][1][2].tolist(), [0, 0, 0])
        self.assertGreater(abs(np.sum(batch1["state_in"][0][0])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][1][0])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][0][1])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][1][1])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][0][3])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][1][3])), 0)

    def test_minibatch_sequencing(self):
        ModelCatalog.register_custom_model("rnn", RNNSpyModel)
        register_env("counter", lambda _: DebugCounterEnv())
        ppo = PPOTrainer(
            env="counter",
            config={
                "shuffle_sequences": False,  # for deterministic testing
                "num_workers": 0,
                "rollout_fragment_length": 20,
                "train_batch_size": 20,
                "sgd_minibatch_size": 10,
                "num_sgd_iter": 1,
                "model": {
                    "custom_model": "rnn",
                    "max_seq_len": 4,
                    "vf_share_layers": True,
                },
                "framework": "tf",
            })
        ppo.train()
        ppo.train()

        # first epoch: 20 observations get split into 2 minibatches of 8
        # four observations are discarded
        batch0 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_0"))
        batch1 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_1"))
        if batch0["sequences"][0][0][0] > batch1["sequences"][0][0][0]:
            batch0, batch1 = batch1, batch0  # sort minibatches
        self.assertEqual(batch0["seq_lens"].tolist(), [4, 4, 2])
        self.assertEqual(batch1["seq_lens"].tolist(), [4, 3, 3])
        self.assertEqual(batch0["sequences"].tolist(), [
            [[0], [1], [2], [3]],
            [[4], [5], [6], [7]],
            [[8], [9], [0], [0]],
        ])
        self.assertEqual(batch1["sequences"].tolist(), [
            [[8], [9], [10], [11]],
            [[12], [13], [14], [0]],
            [[0], [1], [2], [0]],
        ])

        # second epoch: 20 observations get split into 2 minibatches of 8
        # four observations are discarded
        batch2 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_2"))
        batch3 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_3"))
        if batch2["sequences"][0][0][0] > batch3["sequences"][0][0][0]:
            batch2, batch3 = batch3, batch2
        self.assertEqual(batch2["seq_lens"].tolist(), [4, 4, 2])
        self.assertEqual(batch3["seq_lens"].tolist(), [4, 4, 2])
        self.assertEqual(batch2["sequences"].tolist(), [
            [[0], [1], [2], [3]],
            [[4], [5], [6], [7]],
            [[8], [9], [0], [0]],
        ])
        self.assertEqual(batch3["sequences"].tolist(), [
            [[5], [6], [7], [8]],
            [[9], [10], [11], [12]],
            [[13], [14], [0], [0]],
        ])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
