#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import tensorflow as tf

from ray.rllib.agents.agent import get_agent_class

ray.init(num_cpus=10)


def train_and_export(algo_name, num_steps, export_dir):
    cls = get_agent_class(algo_name)
    alg = cls(config={}, env="CartPole-v0")
    for _ in range(3):
        alg.train()

    alg.export_policy_model(export_dir)


def restore(export_dir):
    signature_key = \
        tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY
    g = tf.Graph()
    with g.as_default():
        with tf.Session(graph=g) as sess:
            meta_graph_def = \
                tf.saved_model.load(sess,
                                    [tf.saved_model.tag_constants.SERVING],
                                    export_dir)
            print("Model restored!")
            print("Signature Def Information:")
            print(meta_graph_def.signature_def[signature_key])
            print("You can inspect the model using TensorFlow SavedModel CLI.")
            print("https://www.tensorflow.org/guide/saved_model")


if __name__ == "__main__":
    algo = "DQN"
    export_dir = "/tmp/export_dir"
    num_steps = 3
    train_and_export(algo, num_steps, export_dir)
    restore(export_dir)
