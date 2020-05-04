"""Simple example of setting up a multi-agent policy mapping.

Control the number of agents and policies via --num-agents and --num-policies.

This works with hundreds of agents and policies, but note that initializing
many TF policies will take some time.

Also, TF evals might slow down with large numbers of policies. To debug TF
execution, set the TF_TIMELINE_DIR environment variable.
"""

import argparse
import gym
import random

import ray
from ray import tune
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override

tf = try_import_tf()

parser = argparse.ArgumentParser()

parser.add_argument("--num-agents", type=int, default=4)
parser.add_argument("--num-policies", type=int, default=2)
parser.add_argument("--num-iters", type=int, default=20)
parser.add_argument("--simple", action="store_true")
parser.add_argument("--num-cpus", type=int, default=0)


class CustomModel1(TFModelV2):
    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name):
        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        inputs = tf.keras.layers.Input(observation_space.shape)
        # Example of (optional) weight sharing between two different policies.
        # Here, we share the variables defined in the 'shared' variable scope
        # by entering it explicitly with tf.AUTO_REUSE. This creates the
        # variables for the 'fc1' layer in a global scope called 'shared'
        # outside of the policy's normal variable scope.
        with tf.variable_scope(
                tf.VariableScope(tf.AUTO_REUSE, "shared"),
                reuse=tf.AUTO_REUSE,
                auxiliary_name_scope=False):
            last_layer = tf.keras.layers.Dense(
                units=64, activation=tf.nn.relu, name="fc1")(inputs)
        output = tf.keras.layers.Dense(
            units=num_outputs, activation=None, name="fc_out")(last_layer)
        vf = tf.keras.layers.Dense(
            units=1, activation=None, name="value_out")(last_layer)
        self.base_model = tf.keras.models.Model(inputs, [output, vf])
        self.register_variables(self.base_model.variables)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class CustomModel2(TFModelV2):
    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name):
        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        inputs = tf.keras.layers.Input(observation_space.shape)

        # Weights shared with CustomModel1.
        with tf.variable_scope(
                tf.VariableScope(tf.AUTO_REUSE, "shared"),
                reuse=tf.AUTO_REUSE,
                auxiliary_name_scope=False):
            last_layer = tf.keras.layers.Dense(
                units=64, activation=tf.nn.relu, name="fc1")(inputs)
        output = tf.keras.layers.Dense(
            units=num_outputs, activation=None, name="fc_out")(last_layer)
        vf = tf.keras.layers.Dense(
            units=1, activation=None, name="value_out")(last_layer)
        self.base_model = tf.keras.models.Model(inputs, [output, vf])
        self.register_variables(self.base_model.variables)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)

    ModelCatalog.register_custom_model("model1", CustomModel1)
    ModelCatalog.register_custom_model("model2", CustomModel2)
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    # Each policy can have a different configuration (including custom model)
    def gen_policy(i):
        config = {
            "model": {
                "custom_model": ["model1", "model2"][i % 2],
            },
            "gamma": random.choice([0.95, 0.99]),
        }
        return (None, obs_space, act_space, config)

    # Setup PPO with an ensemble of `num_policies` different policies
    policies = {
        "policy_{}".format(i): gen_policy(i)
        for i in range(args.num_policies)
    }
    policy_ids = list(policies.keys())

    tune.run(
        "PPO",
        stop={"training_iteration": args.num_iters},
        config={
            "env": MultiAgentCartPole,
            "env_config": {
                "num_agents": args.num_agents,
            },
            "log_level": "DEBUG",
            "simple_optimizer": args.simple,
            "num_sgd_iter": 10,
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": (
                    lambda agent_id: random.choice(policy_ids)),
            },
        },
    )
