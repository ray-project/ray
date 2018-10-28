from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle

from gym import spaces
from gym.envs.registration import EnvSpec
import gym
import tensorflow.contrib.slim as slim
import tensorflow as tf
import unittest

import ray
from ray.rllib.agents.pg import PGAgent
from ray.rllib.env.async_vector_env import AsyncVectorEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.models import ModelCatalog
from ray.rllib.models.model import Model
from ray.rllib.test.test_serving_env import SimpleServing
from ray.tune.registry import register_env

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


class InvalidModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        return "not", "valid"


class DictSpyModel(Model):
    capture_index = 0

    def _build_layers_v2(self, input_dict, num_outputs, options):
        def spy(pos, front_cam, task):
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "d_spy_in_{}".format(DictSpyModel.capture_index),
                pickle.dumps((pos, front_cam, task)))
            DictSpyModel.capture_index += 1
            return 0

        spy_fn = tf.py_func(
            spy, [
                input_dict["obs"]["sensors"]["position"],
                input_dict["obs"]["sensors"]["front_cam"][0],
                input_dict["obs"]["inner_state"]["job_status"]["task"]
            ],
            tf.int64,
            stateful=True)

        with tf.control_dependencies([spy_fn]):
            output = slim.fully_connected(
                input_dict["obs"]["sensors"]["position"], num_outputs)
        return output, output


class TupleSpyModel(Model):
    capture_index = 0

    def _build_layers_v2(self, input_dict, num_outputs, options):
        def spy(pos, cam, task):
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "t_spy_in_{}".format(TupleSpyModel.capture_index),
                pickle.dumps((pos, cam, task)))
            TupleSpyModel.capture_index += 1
            return 0

        spy_fn = tf.py_func(
            spy, [
                input_dict["obs"][0],
                input_dict["obs"][1][0],
                input_dict["obs"][2],
            ],
            tf.int64,
            stateful=True)

        with tf.control_dependencies([spy_fn]):
            output = slim.fully_connected(input_dict["obs"][0], num_outputs)
        return output, output


class NestedSpacesTest(unittest.TestCase):
    def testInvalidModel(self):
        ModelCatalog.register_custom_model("invalid", InvalidModel)
        self.assertRaises(ValueError, lambda: PGAgent(
            env="CartPole-v0", config={
                "model": {
                    "custom_model": "invalid",
                },
            }))

    def doTestNestedDict(self, make_env):
        ModelCatalog.register_custom_model("composite", DictSpyModel)
        register_env("nested", make_env)
        pg = PGAgent(
            env="nested",
            config={
                "num_workers": 0,
                "sample_batch_size": 5,
                "model": {
                    "custom_model": "composite",
                },
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

    def doTestNestedTuple(self, make_env):
        ModelCatalog.register_custom_model("composite2", TupleSpyModel)
        register_env("nested2", make_env)
        pg = PGAgent(
            env="nested2",
            config={
                "num_workers": 0,
                "sample_batch_size": 5,
                "model": {
                    "custom_model": "composite2",
                },
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

    def testNestedDictGym(self):
        self.doTestNestedDict(lambda _: NestedDictEnv())

    def testNestedDictVector(self):
        self.doTestNestedDict(
            lambda _: VectorEnv.wrap(lambda i: NestedDictEnv()))

    def testNestedDictServing(self):
        self.doTestNestedDict(lambda _: SimpleServing(NestedDictEnv()))

    def testNestedDictAsync(self):
        self.assertRaisesRegexp(
            ValueError, "Found raw Dict space.*",
            lambda: self.doTestNestedDict(
                lambda _: AsyncVectorEnv.wrap_async(NestedDictEnv())))

    def testNestedTupleGym(self):
        self.doTestNestedTuple(lambda _: NestedTupleEnv())

    def testNestedTupleVector(self):
        self.doTestNestedTuple(
            lambda _: VectorEnv.wrap(lambda i: NestedTupleEnv()))

    def testNestedTupleServing(self):
        self.doTestNestedTuple(lambda _: SimpleServing(NestedTupleEnv()))

    def testNestedTupleAsync(self):
        self.assertRaisesRegexp(
            ValueError, "Found raw Tuple space.*",
            lambda: self.doTestNestedTuple(
                lambda _: AsyncVectorEnv.wrap_async(NestedTupleEnv())))


if __name__ == "__main__":
    ray.init(num_cpus=5)
    unittest.main(verbosity=2)
