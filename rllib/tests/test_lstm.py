import gym
import numpy as np
import pickle
import unittest

import ray
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.recurrent_tf_modelv2 import RecurrentTFModelV2
#from ray.rllib.models.model import Model
from ray.tune.registry import register_env
from ray.rllib.utils import try_import_tf

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

        def spy(sequences, state_in_c, state_in_h, state_out_c, state_out_h):  #, seq_lens):
            if len(sequences) == 1:
                return 0  # don't capture inference inputs
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "rnn_spy_in_{}".format(RNNSpyModel.capture_index),
                pickle.dumps({
                    "sequences": sequences,
                    "state_in": [state_in_c, state_in_h],
                    "state_out": [state_out_c, state_out_h],
                    #"seq_lens": seq_lens
                }),
                overwrite=True)
            RNNSpyModel.capture_index += 1
            return 0

        # Create a keras LSTM model.
        self.lstm_cell = tf.nn.rnn_cell.BasicLSTMCell(
            RNNSpyModel.cell_size, state_is_tuple=True)
        inputs = tf.keras.layers.Input(shape=(None, ) + obs_space.shape,
                                       name="input")
        # Setup the LSTM cell
        #state_init = self.get_initial_state()
        state_in = [
            tf.keras.layers.Input(
                shape=(self.lstm_cell.state_size.c, ), name="state_in_c"),
            tf.keras.layers.Input(
                shape=(self.lstm_cell.state_size.h, ), name="state_in_h"),
        ]
        #seq_lens = tf.keras.layers.Input(shape=(), dtype=tf.int32,
        #                                 name="seq_lens")
        #last_layer = inputs

        # Setup LSTM inputs
        #if state:  #self.state_in:
        #    c_in, h_in = self.state_in
        #else:
        #    c_in = tf.placeholder(
        #        tf.float32, [None, self.lstm.state_size.c], name="c")
        #    h_in = tf.placeholder(
        #        tf.float32, [None, self.lstm.state_size.h], name="h")
        #self.state_in = [c_in, h_in]
        #c_in, h_in = state

        # Setup LSTM outputs
        state_in_tuple = tf.nn.rnn_cell.LSTMStateTuple(state_in[0], state_in[1])
        lstm_out, state_out_c, state_out_h = tf.keras.layers.RNN(
            self.lstm_cell,
            time_major=False,
            return_sequences=True,
            return_state=True
            )(inputs, state_in_tuple)
            #inputs,
            #initial_state=state_in,
            #sequence_length=seq_lens,
            #dtype=tf.float32)

        def lambda_(x, state_in_c, state_in_h, state_out_c, state_out_h):
            spy_fn = tf.py_function(
                spy, [
                    x,
                    state_in_c,
                    state_in_h,
                    state_out_c,
                    state_out_h,
                    #seq_lens,
                ],
                tf.int64)
                #stateful=True)
    
            # Compute outputs
            with tf.control_dependencies([spy_fn]):
                last_layer = tf.reshape(lstm_out, [-1, RNNSpyModel.cell_size])
                logits = tf.keras.layers.Dense(
                    units=self.num_outputs,
                    kernel_initializer=normc_initializer(0.01))(last_layer)
    
                return last_layer, logits

        last_layer, logits = tf.keras.layers.Lambda(lambda_, arguments=dict(
            state_in_c=state_in_tuple.c, state_in_h=state_in_tuple.h, state_out_c=state_out_c, state_out_h=state_out_h
        ))(inputs)

        value_out = tf.keras.layers.Dense(
            units=1, kernel_initializer=normc_initializer(1.0))(last_layer)

        self.base_model = tf.keras.Model(
            [inputs, state_in], [logits, state_out_c, state_out_h, value_out])

        self.register_variables(self.base_model.variables)

    def forward_rnn(self, inputs, state, seq_lens):
        # Previously, a new class object was created during
        # deserialization and this `capture_index`
        # variable would be refreshed between class instantiations.
        # This behavior is no longer the case, so we manually refresh
        # the variable.
        RNNSpyModel.capture_index = 0
        out, state_out_c, state_out_h, value_out = self.base_model([inputs, state])
        self.value_out = value_out
        return out, [state_out_c, state_out_h]
        #features = inputs  #["obs"]
        #last_layer = add_time_dimension(features, seq_lens)

    def value_function(self):
        return self.value_out

    def get_initial_state(self):
        return [
            np.zeros(self.lstm_cell.state_size.c, np.float32),
            np.zeros(self.lstm_cell.state_size.h, np.float32)
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
