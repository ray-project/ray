#!/usr/bin/env python

import os
import ray

from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()

ray.init(num_cpus=10)


def train_and_export(algo_name, num_steps, model_dir, ckpt_dir, prefix):
    cls = get_algorithm_class(algo_name)
    alg = cls(config={}, env="CartPole-v0")
    for _ in range(num_steps):
        alg.train()

    # Export tensorflow checkpoint for fine-tuning
    alg.export_policy_checkpoint(ckpt_dir, filename_prefix=prefix)
    # Export tensorflow SavedModel for online serving
    alg.export_policy_model(model_dir)


def restore_saved_model(export_dir):
    signature_key = (
        tf1.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY
    )
    g = tf1.Graph()
    with g.as_default():
        with tf1.Session(graph=g) as sess:
            meta_graph_def = tf1.saved_model.load(
                sess, [tf1.saved_model.tag_constants.SERVING], export_dir
            )
            print("Model restored!")
            print("Signature Def Information:")
            print(meta_graph_def.signature_def[signature_key])
            print("You can inspect the model using TensorFlow SavedModel CLI.")
            print("https://www.tensorflow.org/guide/saved_model")


def restore_checkpoint(export_dir, prefix):
    sess = tf1.Session()
    meta_file = "%s.meta" % prefix
    saver = tf1.train.import_meta_graph(os.path.join(export_dir, meta_file))
    saver.restore(sess, os.path.join(export_dir, prefix))
    print("Checkpoint restored!")
    print("Variables Information:")
    for v in tf1.trainable_variables():
        value = sess.run(v)
        print(v.name, value)


if __name__ == "__main__":
    algo = "DQN"
    model_dir = os.path.join(ray._private.utils.get_user_temp_dir(), "model_export_dir")
    ckpt_dir = os.path.join(ray._private.utils.get_user_temp_dir(), "ckpt_export_dir")
    prefix = "model.ckpt"
    num_steps = 3
    train_and_export(algo, num_steps, model_dir, ckpt_dir, prefix)
    restore_saved_model(model_dir)
    restore_checkpoint(ckpt_dir, prefix)
