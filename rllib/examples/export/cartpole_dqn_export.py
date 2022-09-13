#!/usr/bin/env python

import numpy as np
import os
import ray

from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()

ray.init(num_cpus=10)


def train_and_export(algo_name, num_steps, model_dir, ckpt_dir):
    cls, config = get_algorithm_class(algo_name, return_config=True)
    # Set exporting native (DL-framework) model files to True.
    config["checkpoints_contain_native_model_files"] = True
    alg = cls(config=config, env="CartPole-v0")
    for _ in range(num_steps):
        alg.train()

    # Export Policy checkpoint.
    alg.export_policy_checkpoint(ckpt_dir)
    # Export tensorflow keras Model for online serving
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


def restore_checkpoint(export_dir):
    # Load the model from the checkpoint.
    model = tf.saved_model.load(export_dir)
    # Perform a dummy (CartPole) forward pass.
    test_obs = np.array([[0.1, 0.2, 0.3, 0.4]])
    results = model(tf.convert_to_tensor(test_obs, dtype=tf.float32))
    # Check results for correctness.
    assert len(results) == 2
    assert results[0].shape == (1, 2)
    # TODO (sven): Make non-RNN models NOT return states (empty list).
    assert results[1].shape == (1, 1)  # dummy state-out


if __name__ == "__main__":
    algo = "PPO"
    model_dir = os.path.join(ray._private.utils.get_user_temp_dir(), "model_export_dir")
    ckpt_dir = os.path.join(ray._private.utils.get_user_temp_dir(), "ckpt_export_dir")
    num_steps = 1
    train_and_export(algo, num_steps, model_dir, ckpt_dir)
    restore_saved_model(model_dir)
    restore_checkpoint(ckpt_dir)
