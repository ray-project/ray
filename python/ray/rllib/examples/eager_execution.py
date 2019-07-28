from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import random

import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.models import Model, ModelCatalog
from ray.rllib.models.tf.fcnet_v1 import FullyConnectedNetwork
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--iters", type=int, default=200)


class EagerModel(Model):
    """Example of using embedded eager execution in a custom model.

    This shows how to use tf.py_function() to execute a snippet of TF code
    in eager mode. Here the `self.forward_eager` method just prints out
    the intermediate tensor for debug purposes, but you can in general
    perform any TF eager operation in tf.py_function().
    """

    def _build_layers_v2(self, input_dict, num_outputs, options):
        self.fcnet = FullyConnectedNetwork(input_dict, self.obs_space,
                                           self.action_space, num_outputs,
                                           options)
        feature_out = tf.py_function(self.forward_eager,
                                     [self.fcnet.last_layer], tf.float32)

        with tf.control_dependencies([feature_out]):
            return tf.identity(self.fcnet.outputs), feature_out

    def forward_eager(self, feature_layer):
        assert tf.executing_eagerly()
        if random.random() > 0.99:
            print("Eagerly printing the feature layer mean value",
                  tf.reduce_mean(feature_layer))
        return feature_layer


def policy_gradient_loss(policy, batch_tensors):
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

    actions = batch_tensors[SampleBatch.ACTIONS]
    rewards = batch_tensors[SampleBatch.REWARDS]
    penalty = tf.py_function(
        compute_penalty, [actions, rewards], Tout=tf.float32)

    return penalty - tf.reduce_mean(policy.action_dist.logp(actions) * rewards)


# <class 'ray.rllib.policy.tf_policy_template.MyTFPolicy'>
MyTFPolicy = build_tf_policy(
    name="MyTFPolicy",
    loss_fn=policy_gradient_loss,
)

# <class 'ray.rllib.agents.trainer_template.MyCustomTrainer'>
MyTrainer = build_trainer(
    name="MyCustomTrainer",
    default_policy=MyTFPolicy,
)

if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    ModelCatalog.register_custom_model("eager_model", EagerModel)
    tune.run(
        MyTrainer,
        stop={"training_iteration": args.iters},
        config={
            "env": "CartPole-v0",
            "num_workers": 0,
            "model": {
                "custom_model": "eager_model"
            },
        })
