from gymnasium import spaces
import gymnasium as gym
import numpy as np
import pickle
import unittest

import ray
from ray.rllib.algorithms.a2c import A2CConfig
from ray.rllib.algorithms.pg import PGConfig
from ray.rllib.env import MultiAgentEnv
from ray.rllib.env.base_env import convert_to_base_env
from ray.rllib.env.tests.test_external_env import SimpleServing
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.evaluate import rollout
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import one_hot
from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.test_utils import check

tf1, tf, tfv = try_import_tf()
_, nn = try_import_torch()

DICT_SPACE = spaces.Dict(
    {
        "sensors": spaces.Dict(
            {
                "position": spaces.Box(low=-100, high=100, shape=(3,)),
                "velocity": spaces.Box(low=-1, high=1, shape=(3,)),
                "front_cam": spaces.Tuple(
                    (
                        spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                        spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                    )
                ),
                "rear_cam": spaces.Box(low=0, high=1, shape=(10, 10, 3)),
            }
        ),
        "inner_state": spaces.Dict(
            {
                "charge": spaces.Discrete(100),
                "job_status": spaces.Dict(
                    {
                        "task": spaces.Discrete(5),
                        "progress": spaces.Box(low=0, high=100, shape=()),
                    }
                ),
            }
        ),
    }
)

DICT_SAMPLES = [DICT_SPACE.sample() for _ in range(10)]

TUPLE_SPACE = spaces.Tuple(
    [
        spaces.Box(low=-100, high=100, shape=(3,)),
        spaces.Tuple(
            (
                spaces.Box(low=0, high=1, shape=(10, 10, 3)),
                spaces.Box(low=0, high=1, shape=(10, 10, 3)),
            )
        ),
        spaces.Discrete(5),
    ]
)
TUPLE_SAMPLES = [TUPLE_SPACE.sample() for _ in range(10)]

# Constraints on the Repeated space.
MAX_PLAYERS = 4
MAX_ITEMS = 7
MAX_EFFECTS = 2
ITEM_SPACE = spaces.Box(-5, 5, shape=(1,))
EFFECT_SPACE = spaces.Box(9000, 9999, shape=(4,))
PLAYER_SPACE = spaces.Dict(
    {
        "location": spaces.Box(-100, 100, shape=(2,)),
        "items": Repeated(ITEM_SPACE, max_len=MAX_ITEMS),
        "effects": Repeated(EFFECT_SPACE, max_len=MAX_EFFECTS),
        "status": spaces.Box(-1, 1, shape=(10,)),
    }
)
REPEATED_SPACE = Repeated(PLAYER_SPACE, max_len=MAX_PLAYERS)
REPEATED_SAMPLES = [REPEATED_SPACE.sample() for _ in range(10)]


class NestedDictEnv(gym.Env):
    def __init__(self):
        self.action_space = spaces.Discrete(2)
        self.observation_space = DICT_SPACE
        self.steps = 0

    def reset(self, *, seed=None, options=None):
        self.steps = 0
        return DICT_SAMPLES[0], {}

    def step(self, action):
        self.steps += 1
        terminated = False
        truncated = self.steps >= 5
        return DICT_SAMPLES[self.steps], 1, terminated, truncated, {}


class NestedTupleEnv(gym.Env):
    def __init__(self):
        self.action_space = spaces.Discrete(2)
        self.observation_space = TUPLE_SPACE
        self.steps = 0

    def reset(self, *, seed=None, options=None):
        self.steps = 0
        return TUPLE_SAMPLES[0], {}

    def step(self, action):
        self.steps += 1
        terminated = False
        truncated = self.steps >= 5
        return TUPLE_SAMPLES[self.steps], 1, terminated, truncated, {}


class RepeatedSpaceEnv(gym.Env):
    def __init__(self):
        self.action_space = spaces.Discrete(2)
        self.observation_space = REPEATED_SPACE
        self.steps = 0

    def reset(self, *, seed=None, options=None):
        self.steps = 0
        return REPEATED_SAMPLES[0], {}

    def step(self, action):
        self.steps += 1
        terminated = False
        truncated = self.steps >= 5
        return REPEATED_SAMPLES[self.steps], 1, terminated, truncated, {}


class NestedMultiAgentEnv(MultiAgentEnv):
    def __init__(self):
        super().__init__()
        self.observation_space = spaces.Dict(
            {"dict_agent": DICT_SPACE, "tuple_agent": TUPLE_SPACE}
        )
        self.action_space = spaces.Dict(
            {"dict_agent": spaces.Discrete(1), "tuple_agent": spaces.Discrete(1)}
        )
        self._agent_ids = {"dict_agent", "tuple_agent"}
        self.steps = 0

    def reset(self, *, seed=None, options=None):
        return {
            "dict_agent": DICT_SAMPLES[0],
            "tuple_agent": TUPLE_SAMPLES[0],
        }, {}

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
        terminateds = {"__all__": self.steps >= 5}
        truncateds = {"__all__": self.steps >= 5}
        infos = {
            "dict_agent": {},
            "tuple_agent": {},
        }
        return obs, rew, terminateds, truncateds, infos


class InvalidModel(TorchModelV2):
    def forward(self, input_dict, state, seq_lens):
        return "not", "valid"


class InvalidModel2(TFModelV2):
    def forward(self, input_dict, state, seq_lens):
        return tf.constant(0), tf.constant(0)


class TorchSpyModel(TorchModelV2, nn.Module):
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)
        self.fc = FullyConnectedNetwork(
            obs_space.original_space["sensors"].spaces["position"],
            action_space,
            num_outputs,
            model_config,
            name,
        )

    def forward(self, input_dict, state, seq_lens):
        pos = input_dict["obs"]["sensors"]["position"].detach().cpu().numpy()
        front_cam = input_dict["obs"]["sensors"]["front_cam"][0].detach().cpu().numpy()
        task = (
            input_dict["obs"]["inner_state"]["job_status"]["task"]
            .detach()
            .cpu()
            .numpy()
        )
        ray.experimental.internal_kv._internal_kv_put(
            "torch_spy_in_{}".format(TorchSpyModel.capture_index),
            pickle.dumps((pos, front_cam, task)),
            overwrite=True,
        )
        TorchSpyModel.capture_index += 1
        return self.fc(
            {"obs": input_dict["obs"]["sensors"]["position"]}, state, seq_lens
        )

    def value_function(self):
        return self.fc.value_function()


class TorchRepeatedSpyModel(TorchModelV2, nn.Module):
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)
        self.fc = FullyConnectedNetwork(
            obs_space.original_space.child_space["location"],
            action_space,
            num_outputs,
            model_config,
            name,
        )

    def forward(self, input_dict, state, seq_lens):
        ray.experimental.internal_kv._internal_kv_put(
            "torch_rspy_in_{}".format(TorchRepeatedSpyModel.capture_index),
            pickle.dumps(input_dict["obs"].unbatch_all()),
            overwrite=True,
        )
        TorchRepeatedSpyModel.capture_index += 1
        return self.fc(
            {"obs": input_dict["obs"].values["location"][:, 0]}, state, seq_lens
        )

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
        return value.detach().cpu().numpy().tolist()


class DictSpyModel(TFModelV2):
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super().__init__(obs_space, action_space, None, model_config, name)
        # Will only feed in sensors->pos.
        input_ = tf.keras.layers.Input(
            shape=self.obs_space["sensors"]["position"].shape
        )

        self.num_outputs = num_outputs or 64
        out = tf.keras.layers.Dense(self.num_outputs)(input_)
        self._main_layer = tf.keras.models.Model([input_], [out])

    def forward(self, input_dict, state, seq_lens):
        def spy(pos, front_cam, task):
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "d_spy_in_{}".format(DictSpyModel.capture_index),
                pickle.dumps((pos, front_cam, task)),
                overwrite=True,
            )
            DictSpyModel.capture_index += 1
            return np.array(0, dtype=np.int64)

        spy_fn = tf1.py_func(
            spy,
            [
                input_dict["obs"]["sensors"]["position"],
                input_dict["obs"]["sensors"]["front_cam"][0],
                input_dict["obs"]["inner_state"]["job_status"]["task"],
            ],
            tf.int64,
            stateful=True,
        )

        with tf1.control_dependencies([spy_fn]):
            output = self._main_layer([input_dict["obs"]["sensors"]["position"]])

        return output, []


class TupleSpyModel(TFModelV2):
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super().__init__(obs_space, action_space, None, model_config, name)
        # Will only feed in 0th index of observation Tuple space.
        input_ = tf.keras.layers.Input(shape=self.obs_space[0].shape)

        self.num_outputs = num_outputs or 64
        out = tf.keras.layers.Dense(self.num_outputs)(input_)
        self._main_layer = tf.keras.models.Model([input_], [out])

    def forward(self, input_dict, state, seq_lens):
        def spy(pos, cam, task):
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "t_spy_in_{}".format(TupleSpyModel.capture_index),
                pickle.dumps((pos, cam, task)),
                overwrite=True,
            )
            TupleSpyModel.capture_index += 1
            return np.array(0, dtype=np.int64)

        spy_fn = tf1.py_func(
            spy,
            [
                input_dict["obs"][0],
                input_dict["obs"][1][0],
                input_dict["obs"][2],
            ],
            tf.int64,
            stateful=True,
        )

        with tf1.control_dependencies([spy_fn]):
            output = tf1.layers.dense(input_dict["obs"][0], self.num_outputs)
        return output, []


class TestNestedObservationSpaces(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_invalid_model(self):
        ModelCatalog.register_custom_model("invalid", InvalidModel)
        config = (
            PGConfig()
            .environment("CartPole-v1")
            .framework("torch")
            .training(model={"custom_model": "invalid"})
        )
        self.assertRaisesRegex(
            ValueError,
            "Subclasses of TorchModelV2 must also inherit from nn.Module",
            lambda: config.build(),
        )

    def test_invalid_model2(self):
        ModelCatalog.register_custom_model("invalid2", InvalidModel2)
        config = (
            PGConfig()
            .environment("CartPole-v1")
            .framework("tf")
            .training(model={"custom_model": "invalid2"})
        )
        self.assertRaisesRegex(
            ValueError,
            "State output is not a list",
            lambda: config.build(),
        )

    def do_test_nested_dict(self, make_env, test_lstm=False, disable_connectors=False):
        ModelCatalog.register_custom_model("composite", DictSpyModel)
        gym.register("nested", make_env)
        config = (
            PGConfig()
            .environment("nested", disable_env_checking=True)
            .rollouts(num_rollout_workers=0, rollout_fragment_length=5)
            .framework("tf")
            .training(
                model={"custom_model": "composite", "use_lstm": test_lstm},
                train_batch_size=5,
            )
        )
        if disable_connectors:
            # manually disable the connectors
            # TODO(avnishn): remove this after deprecating external_env
            config = config.rollouts(enable_connectors=False)
        pg = config.build()
        # Skip first passes as they came from the TorchPolicy loss
        # initialization.
        DictSpyModel.capture_index = 0
        pg.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get("d_spy_in_{}".format(i))
            )
            pos_i = DICT_SAMPLES[i]["sensors"]["position"].tolist()
            cam_i = DICT_SAMPLES[i]["sensors"]["front_cam"][0].tolist()
            task_i = DICT_SAMPLES[i]["inner_state"]["job_status"]["task"]
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            check(seen[2][0], task_i)

    def do_test_nested_tuple(self, make_env, disable_connectors=False):
        ModelCatalog.register_custom_model("composite2", TupleSpyModel)
        gym.register("nested2", make_env)
        config = (
            PGConfig()
            .environment("nested2", disable_env_checking=True)
            .rollouts(num_rollout_workers=0, rollout_fragment_length=5)
            .framework("tf")
            .training(model={"custom_model": "composite2"}, train_batch_size=5)
        )
        if disable_connectors:
            # manually disable the connectors
            # TODO(avnishn): remove this after deprecating external_env
            config = config.rollouts(enable_connectors=False)

        pg = config.build()
        # Skip first passes as they came from the TorchPolicy loss
        # initialization.
        TupleSpyModel.capture_index = 0
        pg.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get("t_spy_in_{}".format(i))
            )
            pos_i = TUPLE_SAMPLES[i][0].tolist()
            cam_i = TUPLE_SAMPLES[i][1][0].tolist()
            task_i = TUPLE_SAMPLES[i][2]
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            check(seen[2][0], task_i)

    def test_nested_dict_gym(self):
        self.do_test_nested_dict(lambda: NestedDictEnv())

    def test_nested_dict_gym_lstm(self):
        self.do_test_nested_dict(lambda: NestedDictEnv(), test_lstm=True)

    def test_nested_dict_vector(self):
        self.do_test_nested_dict(
            lambda: VectorEnv.vectorize_gym_envs(lambda i: NestedDictEnv())
        )

    def test_nested_dict_serving(self):
        # TODO: (Artur) Enable this test again for connectors if discrepancies
        #  between EnvRunnerV2 and ExternalEnv are resolved
        if not PGConfig().enable_connectors:
            self.do_test_nested_dict(lambda: SimpleServing(NestedDictEnv()))

    def test_nested_dict_async(self):
        self.do_test_nested_dict(lambda: convert_to_base_env(NestedDictEnv()))

    def test_nested_tuple_gym(self):
        self.do_test_nested_tuple(lambda: NestedTupleEnv())

    def test_nested_tuple_vector(self):
        self.do_test_nested_tuple(
            lambda: VectorEnv.vectorize_gym_envs(lambda i: NestedTupleEnv())
        )

    def test_nested_tuple_serving(self):
        # TODO: (Artur) Enable this test again for connectors if discrepancies
        #  between EnvRunnerV2 and ExternalEnv are resolved
        if not PGConfig().enable_connectors:
            self.do_test_nested_tuple(lambda: SimpleServing(NestedTupleEnv()))

    def test_nested_tuple_async(self):
        self.do_test_nested_tuple(lambda: convert_to_base_env(NestedTupleEnv()))

    def test_multi_agent_complex_spaces(self):
        ModelCatalog.register_custom_model("dict_spy", DictSpyModel)
        ModelCatalog.register_custom_model("tuple_spy", TupleSpyModel)
        gym.register("nested_ma", lambda: NestedMultiAgentEnv())
        act_space = spaces.Discrete(2)
        config = (
            PGConfig()
            .environment("nested_ma", disable_env_checking=True)
            .framework("tf")
            .rollouts(num_rollout_workers=0, rollout_fragment_length=5)
            .training(train_batch_size=5)
            .multi_agent(
                policies={
                    "tuple_policy": (
                        None,
                        TUPLE_SPACE,
                        act_space,
                        PGConfig.overrides(model={"custom_model": "tuple_spy"}),
                    ),
                    "dict_policy": (
                        None,
                        DICT_SPACE,
                        act_space,
                        PGConfig.overrides(model={"custom_model": "dict_spy"}),
                    ),
                },
                policy_mapping_fn=(
                    lambda agent_id, episode, worker, **kwargs: {
                        "tuple_agent": "tuple_policy",
                        "dict_agent": "dict_policy",
                    }[agent_id]
                ),
            )
        )

        pg = config.build()

        # Skip first passes as they came from the TorchPolicy loss
        # initialization.
        TupleSpyModel.capture_index = DictSpyModel.capture_index = 0
        pg.train()

        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get("d_spy_in_{}".format(i))
            )
            pos_i = DICT_SAMPLES[i]["sensors"]["position"].tolist()
            cam_i = DICT_SAMPLES[i]["sensors"]["front_cam"][0].tolist()
            task_i = DICT_SAMPLES[i]["inner_state"]["job_status"]["task"]
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            check(seen[2][0], task_i)

        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get("t_spy_in_{}".format(i))
            )
            pos_i = TUPLE_SAMPLES[i][0].tolist()
            cam_i = TUPLE_SAMPLES[i][1][0].tolist()
            task_i = TUPLE_SAMPLES[i][2]
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            check(seen[2][0], task_i)

    def test_rollout_dict_space(self):
        gym.register("nested", lambda: NestedDictEnv())

        config = PGConfig().environment("nested").framework("tf")
        algo = config.build()
        algo.train()
        path = algo.save()
        algo.stop()

        # Test train works on restore
        algo2 = config.build()
        algo2.restore(path)
        algo2.train()

        # Test rollout works on restore
        rollout(algo2, "nested", 100)

    def test_py_torch_model(self):
        ModelCatalog.register_custom_model("composite", TorchSpyModel)
        gym.register("nested", lambda: NestedDictEnv())

        config = (
            A2CConfig()
            .environment("nested")
            .framework("torch")
            .rollouts(num_rollout_workers=0, rollout_fragment_length=5)
            .training(train_batch_size=5, model={"custom_model": "composite"})
        )
        a2c = config.build()

        # Skip first passes as they came from the TorchPolicy loss
        # initialization.
        TorchSpyModel.capture_index = 0
        a2c.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "torch_spy_in_{}".format(i)
                )
            )

            pos_i = DICT_SAMPLES[i]["sensors"]["position"].tolist()
            cam_i = DICT_SAMPLES[i]["sensors"]["front_cam"][0].tolist()
            task_i = one_hot(DICT_SAMPLES[i]["inner_state"]["job_status"]["task"], 5)
            # Only look at the last entry (-1) in `seen` as we reset (re-use)
            # the ray-kv indices before training.
            self.assertEqual(seen[0][-1].tolist(), pos_i)
            self.assertEqual(seen[1][-1].tolist(), cam_i)
            check(seen[2][-1], task_i)

    # TODO(ekl) should probably also add a test for TF/eager
    def test_torch_repeated(self):
        ModelCatalog.register_custom_model("r1", TorchRepeatedSpyModel)
        gym.register("repeat", lambda: RepeatedSpaceEnv())

        config = (
            A2CConfig()
            .environment("repeat")
            .framework("torch")
            .rollouts(num_rollout_workers=0, rollout_fragment_length=5)
            .training(train_batch_size=5, model={"custom_model": "r1"})
        )
        a2c = config.build()

        # Skip first passes as they came from the TorchPolicy loss
        # initialization.
        TorchRepeatedSpyModel.capture_index = 0
        a2c.train()

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "torch_rspy_in_{}".format(i)
                )
            )

            # Only look at the last entry (-1) in `seen` as we reset (re-use)
            # the ray-kv indices before training.
            self.assertEqual(to_list(seen[:][-1]), to_list(REPEATED_SAMPLES[i]))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
