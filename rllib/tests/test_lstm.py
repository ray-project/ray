import gym
import numpy as np
import pickle
import unittest

import ray
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.recurrent_tf_modelv2 import RecurrentTFModelV2
#from ray.rllib.models.model import Model
from ray.tune.registry import register_env
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override

tf = try_import_tf()


class TestLSTMUtils(unittest.TestCase):
    def test_basic(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 1, 1, 1, 1, 1, 1]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(eps_ids,
                                                      np.ones_like(eps_ids),
                                                      agent_ids, f, s, 4)
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
        f_pad, s_init, seq_lens = chop_into_sequences(eps_ids,
                                                      np.ones_like(eps_ids),
                                                      agent_ids, f, s, 4)
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
        _, _, seq_lens = chop_into_sequences(eps_ids, batch_ids, agent_ids, f,
                                             s, 4)
        self.assertEqual(seq_lens.tolist(), [2, 1, 1, 2, 2])

    def test_multi_agent(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 2, 1, 1, 2, 2, 3]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            eps_ids,
            np.ones_like(eps_ids),
            agent_ids,
            f,
            s,
            4,
            dynamic_max=False)
        self.assertEqual(seq_lens.tolist(), [2, 1, 2, 2, 1])
        self.assertEqual(len(f_pad[0]), 20)
        self.assertEqual(len(s_init[0]), 5)

    def test_dynamic_max_len(self):
        eps_ids = [5, 2, 2]
        agent_ids = [2, 2, 2]
        f = [[1, 1, 1]]
        s = [[1, 1, 1]]
        f_pad, s_init, seq_lens = chop_into_sequences(eps_ids,
                                                      np.ones_like(eps_ids),
                                                      agent_ids, f, s, 4)
        self.assertEqual([f.tolist() for f in f_pad], [[1, 0, 1, 1]])
        self.assertEqual([s.tolist() for s in s_init], [[1, 1]])
        self.assertEqual(seq_lens.tolist(), [1, 2])


class RNNSpyModel(RecurrentTFModelV2):
    capture_index = 0
    cell_size = 3

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        self.lstm = tf.keras.layers.LSTM(
            self.cell_size, return_state=True, return_sequences=True)

    def forward_rnn(self, inputs, state, seq_lens):
        lstm_out, h, c = self.lstm(
            inputs=inputs,
            mask=tf.sequence_mask(seq_lens),
            initial_state=state)

        def spy(inputs, seq_lens):
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "t_spy_in_{}".format(RNNSpyModel.capture_index),
                pickle.dumps((inputs, seq_lens)),
                overwrite=True)
            RNNSpyModel.capture_index += 1
            return 0

        spy_fn = tf.py_func(
            spy, [
                inputs,
                seq_lens,
            ], tf.int64, stateful=True)

        with tf.control_dependencies([spy_fn]):
            output = tf.layers.dense(lstm_out, self.num_outputs)
            value_out = tf.layers.dense(lstm_out, 1)
            self._value_out = value_out
            return output, [h, c]

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    @override(ModelV2)
    def get_initial_state(self):
        return [
            np.zeros(self.cell_size, np.float32),
            np.zeros(self.cell_size, np.float32)
        ]


class RNNSpyModel_old(RecurrentTFModelV2):
    capture_index = 0
    cell_size = 3

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        self.cell_size = RNNSpyModel.cell_size

        def spy(
                inputs, seq_lens
        ):  #, state_in_h, state_in_c, state_out_h, state_out_c, seq_lens):
            if len(inputs) == 1:
                return 0  # don't capture inference inputs
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "rnn_spy_in_{}".format(RNNSpyModel.capture_index),
                pickle.dumps({
                    "sequences": inputs,
                    "seq_lens": seq_lens,
                }),
                overwrite=True)
            RNNSpyModel.capture_index += 1
            return 0

        # Create a keras LSTM model.
        inputs = tf.keras.layers.Input(
            shape=(None, ) + obs_space.shape, name="input")
        state_in_h = tf.keras.layers.Input(shape=(self.cell_size, ), name="h")
        state_in_c = tf.keras.layers.Input(shape=(self.cell_size, ), name="c")
        seq_in = tf.keras.layers.Input(shape=(), name="seq_in", dtype=tf.int32)

        lstm_out, state_h, state_c = tf.keras.layers.LSTM(
            self.cell_size,
            return_sequences=True,
            return_state=True,
            name="lstm")(
                inputs=inputs,
                mask=tf.sequence_mask(seq_in),
                initial_state=[state_in_h, state_in_c])

        def lambda_(
                inputs  #, state_in_c
        ):  #, lstm_out, state_in_h, state_in_c, state_out_h, state_out_c, seq_lens):
            spy_fn = tf.py_func(
                spy,
                [
                    inputs[0],
                    #state_in_c,
                    #state_in_h,
                    #state_out_c,
                    #state_out_h,
                    inputs[1],
                ],
                tf.int64,
                stateful=True)

            # Compute outputs
            with tf.control_dependencies([spy_fn]):
                #last_layer = tf.reshape(x, [-1, self.cell_size])
                logits = tf.keras.layers.Dense(
                    units=self.num_outputs,
                    kernel_initializer=normc_initializer(0.01))(lstm_out)

                return logits

        logits = tf.keras.layers.Lambda(lambda_)(
            [lstm_out, seq_in])  #, arguments=dict(state_in_c=state_in_c))(
        #lstm_out)  #, arguments=dict(
        #lstm_out=lstm_out, state_in_h=state_in_h, state_in_c=state_in_c, state_out_h=state_h, state_out_c=state_c, seq_lens=seq_in
        #))
        #(inputs)

        #logits = tf.keras.layers.Dense(
        #    units=self.num_outputs,
        #    kernel_initializer=normc_initializer(0.01))(lstm_out)

        # Value branch.
        value_out = tf.keras.layers.Dense(
            units=1, kernel_initializer=normc_initializer(1.0))(lstm_out)

        self.base_model = tf.keras.Model(
            [inputs, seq_in, state_in_h, state_in_c],
            [logits, value_out, state_h, state_c])
        self.base_model.summary()
        self.register_variables(self.base_model.variables)

    @override(RecurrentTFModelV2)
    def forward_rnn(self, inputs, state, seq_lens):
        # Previously, a new class object was created during
        # deserialization and this `capture_index`
        # variable would be refreshed between class instantiations.
        # This behavior is no longer the case, so we manually refresh
        # the variable.
        RNNSpyModel.capture_index = 0
        model_out, value_out, h, c = self.base_model(
            [inputs, seq_lens, state[0], state[1]])
        self._value_out = value_out
        return model_out, [h, c]
        #features = inputs  #["obs"]
        #last_layer = add_time_dimension(features, seq_lens)

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    @override(ModelV2)
    def get_initial_state(self):
        return [
            np.zeros(self.cell_size, np.float32),
            np.zeros(self.cell_size, np.float32)
        ]


class DebugCounterEnv(gym.Env):
    def __init__(self):
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(0, 100, (1, ))
        self.i = 0

    def reset(self):
        self.i = 0
        return [self.i]

    def step(self, action):
        self.i += 1
        return [self.i], self.i % 3, self.i >= 15, {}


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
                "vf_share_layers": True,
                "simple_optimizer": True,
                "num_sgd_iter": 1,
                "model": {
                    "custom_model": "rnn",
                    "max_seq_len": 4,
                    "state_shape": [3, 3],
                },
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
                #"eager": True,
                "shuffle_sequences": False,  # for deterministic testing
                "num_workers": 0,
                "rollout_fragment_length": 20,
                "train_batch_size": 20,
                "sgd_minibatch_size": 10,
                "vf_share_layers": True,
                "simple_optimizer": False,
                "num_sgd_iter": 1,
                "model": {
                    "custom_model": "rnn",
                    "max_seq_len": 4,
                    "state_shape": [3, 3],
                },
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
        self.assertEqual(batch0["seq_lens"].tolist(), [4, 4])
        self.assertEqual(batch1["seq_lens"].tolist(), [4, 3])
        self.assertEqual(batch0["sequences"].tolist(), [
            [[0], [1], [2], [3]],
            [[4], [5], [6], [7]],
        ])
        self.assertEqual(batch1["sequences"].tolist(), [
            [[8], [9], [10], [11]],
            [[12], [13], [14], [0]],
        ])

        # second epoch: 20 observations get split into 2 minibatches of 8
        # four observations are discarded
        batch2 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_2"))
        batch3 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_3"))
        if batch2["sequences"][0][0][0] > batch3["sequences"][0][0][0]:
            batch2, batch3 = batch3, batch2
        self.assertEqual(batch2["seq_lens"].tolist(), [4, 4])
        self.assertEqual(batch3["seq_lens"].tolist(), [2, 4])
        self.assertEqual(batch2["sequences"].tolist(), [
            [[5], [6], [7], [8]],
            [[9], [10], [11], [12]],
        ])
        self.assertEqual(batch3["sequences"].tolist(), [
            [[13], [14], [0], [0]],
            [[0], [1], [2], [3]],
        ])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
