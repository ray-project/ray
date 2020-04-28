import argparse
import random

import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--iters", type=int, default=200)


class EagerModel(TFModelV2):
    """Example of using embedded eager execution in a custom model.

    This shows how to use tf.py_function() to execute a snippet of TF code
    in eager mode. Here the `self.forward_eager` method just prints out
    the intermediate tensor for debug purposes, but you can in general
    perform any TF eager operation in tf.py_function().
    """

    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name):
        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        inputs = tf.keras.layers.Input(shape=observation_space.shape)
        self.fcnet = FullyConnectedNetwork(
            obs_space=self.obs_space,
            action_space=self.action_space,
            num_outputs=self.num_outputs,
            model_config=self.model_config,
            name="fc1")
        out, value_out = self.fcnet.base_model(inputs)

        def lambda_(x):
            eager_out = tf.py_function(self.forward_eager, [x], tf.float32)
            with tf.control_dependencies([eager_out]):
                eager_out.set_shape(x.shape)
                return eager_out

        out = tf.keras.layers.Lambda(lambda_)(out)
        self.base_model = tf.keras.models.Model(inputs, [out, value_out])
        self.register_variables(self.base_model.variables)

    @override(TFModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"], state,
                                               seq_lens)
        return out, []

    @override(TFModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    def forward_eager(self, feature_layer):
        assert tf.executing_eagerly()
        if random.random() > 0.99:
            print("Eagerly printing the feature layer mean value",
                  tf.reduce_mean(feature_layer))
        return feature_layer


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

    logits, _ = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)

    actions = train_batch[SampleBatch.ACTIONS]
    rewards = train_batch[SampleBatch.REWARDS]
    penalty = tf.py_function(
        compute_penalty, [actions, rewards], Tout=tf.float32)

    return penalty - tf.reduce_mean(action_dist.logp(actions) * rewards)


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

    config = {
        "env": "CartPole-v0",
        "num_workers": 0,
        "model": {
            "custom_model": "eager_model"
        },
    }

    tune.run(MyTrainer, stop={"training_iteration": args.iters}, config=config)
