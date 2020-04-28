"""Example of using a custom model with batch norm."""

import argparse

import ray
from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--num-iters", type=int, default=200)
parser.add_argument("--run", type=str, default="PPO")


class BatchNormModel(TFModelV2):
    @override(TFModelV2)
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        self.inputs = tf.keras.layers.Input(
            shape=obs_space.shape, name="inputs")
        self.is_training = tf.keras.layers.Input(
            shape=(), dtype=tf.bool, name="is_training")
        last_layer = self.inputs
        hiddens = [256, 256]
        for i, size in enumerate(hiddens):
            label = "fc{}".format(i)
            last_layer = tf.keras.layers.Dense(
                units=size,
                kernel_initializer=normc_initializer(1.0),
                activation=tf.nn.tanh,
                name=label)(last_layer)
            # Add a batch norm layer
            last_layer = tf.keras.layers.BatchNormalization()(
                last_layer, training=self.is_training[0])
        output = tf.keras.layers.Dense(
            units=self.num_outputs,
            kernel_initializer=normc_initializer(0.01),
            activation=None,
            name="fc_out")(last_layer)
        value_out = tf.keras.layers.Dense(
            units=1,
            kernel_initializer=normc_initializer(0.01),
            activation=None,
            name="value_out")(last_layer)
        self.base_model = tf.keras.models.Model(
            inputs=[self.inputs, self.is_training],
            outputs=[output, value_out])
        self.register_variables(self.base_model.variables)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        if input_dict["is_training"].shape.as_list() == []:
            input_dict["is_training"] = tf.expand_dims(
                input_dict["is_training"], 0)
        out, self._value_out = self.base_model(
            [input_dict["obs"], input_dict["is_training"]])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    ModelCatalog.register_custom_model("bn_model", BatchNormModel)

    config = {
        "env": "Pendulum-v0" if args.run == "DDPG" else "CartPole-v0",
        "model": {
            "custom_model": "bn_model",
        },
        "num_workers": 0,
    }

    from ray.rllib.agents.ppo import PPOTrainer
    trainer = PPOTrainer(config=config)
    trainer.train()

    tune.run(
        args.run,
        stop={"training_iteration": args.num_iters},
        config=config,
    )
