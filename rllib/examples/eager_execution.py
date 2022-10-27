import argparse
import os
import random

import ray
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.examples.models.eager_model import EagerModel
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved
from ray import air, tune

# Always import tensorflow using this utility function:
tf1, tf, tfv = try_import_tf()
# tf1: The installed tf1.x package OR the tf.compat.v1 module within
#   a 2.x tf installation.
# tf: The installed tf package (whatever tf version was installed).
# tfv: The tf version int (either 1 or 2).

# To enable eager mode, do:
# >> tf1.enable_eager_execution()
# >> x = tf.Variable(0.0)
# >> x.numpy()
# 0.0

# RLlib will automatically enable eager mode, if you set
# AlgorithmConfig.framework("tf2", eager_tracing=False).
# If you would like to remain in tf static-graph mode, but still use tf2.x's
# new APIs (some of which are not supported by tf1.x), specify your "framework"
# as "tf" and check for the version (tfv) to be 2:

# Example:
# >> def dense(x, W, b):
# ..   return tf.nn.sigmoid(tf.matmul(x, W) + b)
#
# >> @tf.function
# >> def multilayer_perceptron(x, w0, b0):
# ..   return dense(x, w0, b0)

# Also be careful to distinguish between tf1 and tf in your code. For example,
# to create a placeholder:
# >> tf1.placeholder(tf.float32, (2, ))  # <- must use `tf1` here

parser = argparse.ArgumentParser()
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)


def policy_gradient_loss(policy, model, dist_class, train_batch):
    """Example of using embedded eager execution in a custom loss.

    Here `compute_penalty` prints the actions and rewards for debugging, and
    also computes a (dummy) penalty term to add to the loss.
    """

    def compute_penalty(actions, rewards):
        assert tf.executing_eagerly()
        penalty = tf.reduce_mean(tf.cast(actions, tf.float32))
        if random.random() > 0.9:
            print("The eagerly computed penalty is", penalty, actions, rewards)
        return penalty

    logits, _ = model(train_batch)
    action_dist = dist_class(logits, model)

    actions = train_batch[SampleBatch.ACTIONS]
    rewards = train_batch[SampleBatch.REWARDS]
    penalty = tf.py_function(compute_penalty, [actions, rewards], Tout=tf.float32)

    return penalty - tf.reduce_mean(action_dist.logp(actions) * rewards)


# <class 'ray.rllib.policy.tf_policy_template.MyTFPolicy'>
MyTFPolicy = build_tf_policy(
    name="MyTFPolicy",
    loss_fn=policy_gradient_loss,
)


# Create a new Algorithm using the Policy defined above.
class MyAlgo(Algorithm):
    def get_default_policy_class(self, config):
        return MyTFPolicy


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    ModelCatalog.register_custom_model("eager_model", EagerModel)

    config = {
        "env": "CartPole-v1",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 0,
        "model": {"custom_model": "eager_model"},
        "framework": "tf2",
    }
    stop = {
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.Tuner(
        MyAlgo, run_config=air.RunConfig(stop=stop, verbose=1), param_space=config
    ).fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
