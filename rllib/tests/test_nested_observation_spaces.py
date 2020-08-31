from gym import spaces
from gym.envs.registration import EnvSpec
import gym
import numpy as np
import pickle
import unittest

import ray
from ray.rllib.agents.a3c import A2CTrainer
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.env import MultiAgentEnv
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.rollout import rollout
from ray.rllib.tests.test_external_env import SimpleServing
from ray.tune.registry import register_env
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.spaces.repeated import Repeated

tf1, tf, tfv = try_import_tf()
_, nn = try_import_torch()

DICT_SPACE = spaces.Dict({
    "sensors": spaces.Dict({
        "position": spaces.Box(low=-100, high=100, shape=(3, )),
        "velocity": spaces.Box(low=-1, high=1, shape=(3, )),
        "front_cam": spaces.Tuple(
            (spaces.Box(low=0, high=1, shape=(10, 10, 3)),
             spaces.Box(low=0, high=1, shape=(10, 10, 3)))),
        "rear_cam": spaces.Box(low=0, high=1, shape=(10, 10, 3)),
    }),
    "inner_state": spaces.Dict({
        "charge": spaces.Discrete(100),
        "job_status": spaces.Dict({
            "task": spaces.Discrete(5),
            "progress": spaces.Box(low=0, high=100, shape=()),
        })
    })
})

DICT_SAMPLES = [DICT_SPACE.sample() for _ in range(10)]

TUPLE_SPACE = spaces.Tuple([
    spaces.Box(low=-100, high=100, shape=(3, )),
    spaces.Tuple((spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                  spaces.Box(low=0, high=1, shape=(10, 10, 3)))),
    spaces.Discrete(5),
])
TUPLE_SAMPLES = [TUPLE_SPACE.sample() for _ in range(10)]

# Constraints on the Repeated space.
MAX_PLAYERS = 4
MAX_ITEMS = 7
MAX_EFFECTS = 2
ITEM_SPACE = spaces.Box(-5, 5, shape=(1, ))
EFFECT_SPACE = spaces.Box(9000, 9999, shape=(4, ))
PLAYER_SPACE = spaces.Dict({
    "location": spaces.Box(-100, 100, shape=(2, )),
    "items": Repeated(ITEM_SPACE, max_len=MAX_ITEMS),
    "effects": Repeated(EFFECT_SPACE, max_len=MAX_EFFECTS),
    "status": spaces.Box(-1, 1, shape=(10, )),
})
REPEATED_SPACE = Repeated(PLAYER_SPACE, max_len=MAX_PLAYERS)
REPEATED_SAMPLES = [REPEATED_SPACE.sample() for _ in range(10)]


def one_hot(i, n):
    out = [0.0] * n
    out[i] = 1.0
    return out


class NestedDictEnv(gym.Env):
    def __init__(self):
        self.action_space = spaces.Discrete(2)
        self.observation_space = DICT_SPACE
        self._spec = EnvSpec("NestedDictEnv-v0")
        self.steps = 0

    def reset(self):
        self.steps = 0
        return DICT_SAMPLES[0]

    def step(self, action):
        self.steps += 1
        return DICT_SAMPLES[self.steps], 1, self.steps >= 5, {}


class NestedTupleEnv(gym.Env):
    def __init__(self):
        self.action_space = spaces.Discrete(2)
        self.observation_space = TUPLE_SPACE
        self._spec = EnvSpec("NestedTupleEnv-v0")
        self.steps = 0

    def reset(self):
        self.steps = 0
        return TUPLE_SAMPLES[0]

    def step(self, action):
        self.steps += 1
        return TUPLE_SAMPLES[self.steps], 1, self.steps >= 5, {}


class RepeatedSpaceEnv(gym.Env):
    def __init__(self):
        self.action_space = spaces.Discrete(2)
        self.observation_space = REPEATED_SPACE
        self._spec = EnvSpec("RepeatedSpaceEnv-v0")
        self.steps = 0

    def reset(self):
        self.steps = 0
        return REPEATED_SAMPLES[0]

    def step(self, action):
        self.steps += 1
        return REPEATED_SAMPLES[self.steps], 1, self.steps >= 5, {}


class NestedMultiAgentEnv(MultiAgentEnv):
    def __init__(self):
        self.steps = 0

    def reset(self):
        return {
            "dict_agent": DICT_SAMPLES[0],
            "tuple_agent": TUPLE_SAMPLES[0],
        }

    def step(self, actions):
        self.steps += 1
        obs = {
            "dict_agent": DICT_SAMPLES[self.steps],
            "tuple_agent": TUPLE_SAMPLES[self.steps],
        }
        rew = {
            "dict_agent": 0,
            "tuple_agent": 0,
        }
        dones = {"__all__": self.steps >= 5}
        infos = {
            "dict_agent": {},
            "tuple_agent": {},
        }
        return obs, rew, dones, infos


class InvalidModel(TorchModelV2):
    def forward(self, input_dict, state, seq_lens):
        return "not", "valid"


class InvalidModel2(TFModelV2):
    def forward(self, input_dict, state, seq_lens):
        return tf.constant(0), tf.constant(0)


class TorchSpyModel(TorchModelV2, nn.Module):
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)
        self.fc = FullyConnectedNetwork(
            obs_space.original_space.spaces["sensors"].spaces["position"],
            action_space, num_outputs, model_config, name)

    def forward(self, input_dict, state, seq_lens):
        pos = input_dict["obs"]["sensors"]["position"].numpy()
        front_cam = input_dict["obs"]["sensors"]["front_cam"][0].numpy()
        task = input_dict["obs"]["inner_state"]["job_status"]["task"].numpy()
        ray.experimental.internal_kv._internal_kv_put(
            "torch_spy_in_{}".format(TorchSpyModel.capture_index),
            pickle.dumps((pos, front_cam, task)),
            overwrite=True)
        TorchSpyModel.capture_index += 1
        return self.fc({
            "obs": input_dict["obs"]["sensors"]["position"]
        }, state, seq_lens)

    def value_function(self):
        return self.fc.value_function()


class TorchRepeatedSpyModel(TorchModelV2, nn.Module):
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)
        self.fc = FullyConnectedNetwork(
            obs_space.original_space.child_space["location"], action_space,
            num_outputs, model_config, name)

    def forward(self, input_dict, state, seq_lens):
        ray.experimental.internal_kv._internal_kv_put(
            "torch_rspy_in_{}".format(TorchRepeatedSpyModel.capture_index),
            pickle.dumps(input_dict["obs"].unbatch_all()),
            overwrite=True)
        TorchRepeatedSpyModel.capture_index += 1
        return self.fc({
            "obs": input_dict["obs"].values["location"][:, 0]
        }, state, seq_lens)

    def value_function(self):
        return self.fc.value_function()


def to_list(value):
    if isinstance(value, list):
        return [to_list(x) for x in value]
    elif isinstance(value, dict):
        return {k: to_list(v) for k, v in value.items()}
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif isinstance(value, int):
        return value
    else:
        return value.numpy().tolist()


class DictSpyModel(TFModelV2):
    capture_index = 0

    def forward(self, input_dict, state, seq_lens):
        def spy(pos, front_cam, task):
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "d_spy_in_{}".format(DictSpyModel.capture_index),
                pickle.dumps((pos, front_cam, task)),
                overwrite=True)
            DictSpyModel.capture_index += 1
            return np.array(0, dtype=np.int64)

        spy_fn = tf1.py_func(
            spy, [
                input_dict["obs"]["sensors"]["position"],
                input_dict["obs"]["sensors"]["front_cam"][0],
                input_dict["obs"]["inner_state"]["job_status"]["task"]
            ],
            tf.int64,
            stateful=True)

        with tf1.control_dependencies([spy_fn]):
            output = tf1.layers.dense(input_dict["obs"]["sensors"]["position"],
                                      self.num_outputs)
        return output, []


class TupleSpyModel(TFModelV2):
    capture_index = 0

    def forward(self, input_dict, state, seq_lens):
        def spy(pos, cam, task):
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "t_spy_in_{}".format(TupleSpyModel.capture_index),
                pickle.dumps((pos, cam, task)),
                overwrite=True)
            TupleSpyModel.capture_index += 1
            return np.array(0, dtype=np.int64)

        spy_fn = tf1.py_func(
            spy, [
                input_dict["obs"][0],
                input_dict["obs"][1][0],
                input_dict["obs"][2],
            ],
            tf.int64,
            stateful=True)

        with tf1.control_dependencies([spy_fn]):
            output = tf1.layers.dense(input_dict["obs"][0], self.num_outputs)
        return output, []


class NestedSpacesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_invalid_model(self):
        ModelCatalog.register_custom_model("invalid", InvalidModel)
        self.assertRaisesRegexp(
            ValueError,
            "Subclasses of TorchModelV2 must also inherit from nn.Module",
            lambda: PGTrainer(
                env="CartPole-v0",
                config={
                    "model": {
                        "custom_model": "invalid",
                    },
                    "framework": "torch",
                }))

    def test_invalid_model2(self):
        ModelCatalog.register_custom_model("invalid2", InvalidModel2)
        self.assertRaisesRegexp(
            ValueError, "Expected output shape of",
            lambda: PGTrainer(
                env="CartPole-v0", config={
                    "model": {
                        "custom_model": "invalid2",
                    },
                    "framework": "tf",
                }))

    def do_test_nested_dict(self, make_env, test_lstm=False):
        ModelCatalog.register_custom_model("composite", DictSpyModel)
        register_env("nested", make_env)
        pg = PGTrainer(
            env="nested",
            config={
                "num_workers": 0,
                "rollout_fragment_length": 5,
                "train_batch_size": 5,
                "model": {
                    "custom_model": "composite",
                    "use_lstm": test_lstm,
                },
                "framework": "tf",
            })
        pg.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "d_spy_in_{}".format(i)))
            pos_i = DICT_SAMPLES[i]["sensors"]["position"].tolist()
            cam_i = DICT_SAMPLES[i]["sensors"]["front_cam"][0].tolist()
            task_i = one_hot(
                DICT_SAMPLES[i]["inner_state"]["job_status"]["task"], 5)
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            self.assertEqual(seen[2][0].tolist(), task_i)

    def do_test_nested_tuple(self, make_env):
        ModelCatalog.register_custom_model("composite2", TupleSpyModel)
        register_env("nested2", make_env)
        pg = PGTrainer(
            env="nested2",
            config={
                "num_workers": 0,
                "rollout_fragment_length": 5,
                "train_batch_size": 5,
                "model": {
                    "custom_model": "composite2",
                },
                "framework": "tf",
            })
        pg.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "t_spy_in_{}".format(i)))
            pos_i = TUPLE_SAMPLES[i][0].tolist()
            cam_i = TUPLE_SAMPLES[i][1][0].tolist()
            task_i = one_hot(TUPLE_SAMPLES[i][2], 5)
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            self.assertEqual(seen[2][0].tolist(), task_i)

    def test_nested_dict_gym(self):
        self.do_test_nested_dict(lambda _: NestedDictEnv())

    def test_nested_dict_gym_lstm(self):
        self.do_test_nested_dict(lambda _: NestedDictEnv(), test_lstm=True)

    def test_nested_dict_vector(self):
        self.do_test_nested_dict(
            lambda _: VectorEnv.wrap(lambda i: NestedDictEnv()))

    def test_nested_dict_serving(self):
        self.do_test_nested_dict(lambda _: SimpleServing(NestedDictEnv()))

    def test_nested_dict_async(self):
        self.do_test_nested_dict(
            lambda _: BaseEnv.to_base_env(NestedDictEnv()))

    def test_nested_tuple_gym(self):
        self.do_test_nested_tuple(lambda _: NestedTupleEnv())

    def test_nested_tuple_vector(self):
        self.do_test_nested_tuple(
            lambda _: VectorEnv.wrap(lambda i: NestedTupleEnv()))

    def test_nested_tuple_serving(self):
        self.do_test_nested_tuple(lambda _: SimpleServing(NestedTupleEnv()))

    def test_nested_tuple_async(self):
        self.do_test_nested_tuple(
            lambda _: BaseEnv.to_base_env(NestedTupleEnv()))

    def test_multi_agent_complex_spaces(self):
        ModelCatalog.register_custom_model("dict_spy", DictSpyModel)
        ModelCatalog.register_custom_model("tuple_spy", TupleSpyModel)
        register_env("nested_ma", lambda _: NestedMultiAgentEnv())
        act_space = spaces.Discrete(2)
        pg = PGTrainer(
            env="nested_ma",
            config={
                "num_workers": 0,
                "rollout_fragment_length": 5,
                "train_batch_size": 5,
                "multiagent": {
                    "policies": {
                        "tuple_policy": (
                            PGTFPolicy, TUPLE_SPACE, act_space,
                            {"model": {"custom_model": "tuple_spy"}}),
                        "dict_policy": (
                            PGTFPolicy, DICT_SPACE, act_space,
                            {"model": {"custom_model": "dict_spy"}}),
                    },
                    "policy_mapping_fn": lambda a: {
                        "tuple_agent": "tuple_policy",
                        "dict_agent": "dict_policy"}[a],
                },
                "framework": "tf",
            })
        pg.train()

        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "d_spy_in_{}".format(i)))
            pos_i = DICT_SAMPLES[i]["sensors"]["position"].tolist()
            cam_i = DICT_SAMPLES[i]["sensors"]["front_cam"][0].tolist()
            task_i = one_hot(
                DICT_SAMPLES[i]["inner_state"]["job_status"]["task"], 5)
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            self.assertEqual(seen[2][0].tolist(), task_i)

        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "t_spy_in_{}".format(i)))
            pos_i = TUPLE_SAMPLES[i][0].tolist()
            cam_i = TUPLE_SAMPLES[i][1][0].tolist()
            task_i = one_hot(TUPLE_SAMPLES[i][2], 5)
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            self.assertEqual(seen[2][0].tolist(), task_i)

    def test_rollout_dict_space(self):
        register_env("nested", lambda _: NestedDictEnv())
        agent = PGTrainer(env="nested", config={"framework": "tf"})
        agent.train()
        path = agent.save()
        agent.stop()

        # Test train works on restore
        agent2 = PGTrainer(env="nested", config={"framework": "tf"})
        agent2.restore(path)
        agent2.train()

        # Test rollout works on restore
        rollout(agent2, "nested", 100)

    def test_py_torch_model(self):
        ModelCatalog.register_custom_model("composite", TorchSpyModel)
        register_env("nested", lambda _: NestedDictEnv())
        a2c = A2CTrainer(
            env="nested",
            config={
                "num_workers": 0,
                "rollout_fragment_length": 5,
                "train_batch_size": 5,
                "model": {
                    "custom_model": "composite",
                },
                "framework": "torch",
            })

        a2c.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "torch_spy_in_{}".format(i)))
            pos_i = DICT_SAMPLES[i]["sensors"]["position"].tolist()
            cam_i = DICT_SAMPLES[i]["sensors"]["front_cam"][0].tolist()
            task_i = one_hot(
                DICT_SAMPLES[i]["inner_state"]["job_status"]["task"], 5)
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            self.assertEqual(seen[2][0].tolist(), task_i)

    # TODO(ekl) should probably also add a test for TF/eager
    def test_torch_repeated(self):
        ModelCatalog.register_custom_model("r1", TorchRepeatedSpyModel)
        register_env("repeat", lambda _: RepeatedSpaceEnv())
        a2c = A2CTrainer(
            env="repeat",
            config={
                "num_workers": 0,
                "rollout_fragment_length": 5,
                "train_batch_size": 5,
                "model": {
                    "custom_model": "r1",
                },
                "framework": "torch",
            })

        a2c.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "torch_rspy_in_{}".format(i)))
            self.assertEqual(to_list(seen), [to_list(REPEATED_SAMPLES[i])])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
