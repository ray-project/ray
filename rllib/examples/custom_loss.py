from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of using custom_loss() with an imitation learning loss.

The default input file is too small to learn a good policy, but you can
generate new experiences for IL training as follows:

To generate experiences:
$ ./train.py --run=PG --config='{"output": "/tmp/cartpole"}' --env=CartPole-v0

To train on experiences with joint PG + IL loss:
$ python custom_loss.py --input-files=/tmp/cartpole
"""

import argparse
import os

import ray
from ray import tune
from ray.rllib.models import Model, ModelCatalog
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.tf.fcnet_v1 import FullyConnectedNetwork
from ray.rllib.models.model import restore_original_dimensions
from ray.rllib.offline import JsonReader
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--iters", type=int, default=200)
parser.add_argument(
    "--input-files",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../tests/data/cartpole_small"))


class CustomLossModel(Model):
    """Custom model that adds an imitation loss on top of the policy loss."""

    def _build_layers_v2(self, input_dict, num_outputs, options):
        self.obs_in = input_dict["obs"]
        with tf.variable_scope("shared", reuse=tf.AUTO_REUSE):
            self.fcnet = FullyConnectedNetwork(input_dict, self.obs_space,
                                               self.action_space, num_outputs,
                                               options)
        return self.fcnet.outputs, self.fcnet.last_layer

    def custom_loss(self, policy_loss, loss_inputs):
        # create a new input reader per worker
        reader = JsonReader(self.options["custom_options"]["input_files"])
        input_ops = reader.tf_input_ops()

        # define a secondary loss by building a graph copy with weight sharing
        obs = tf.cast(input_ops["obs"], tf.float32)
        logits, _ = self._build_layers_v2({
            "obs": restore_original_dimensions(obs, self.obs_space)
        }, self.num_outputs, self.options)

        # You can also add self-supervised losses easily by referencing tensors
        # created during _build_layers_v2(). For example, an autoencoder-style
        # loss can be added as follows:
        # ae_loss = squared_diff(
        #     loss_inputs["obs"], Decoder(self.fcnet.last_layer))
        print("FYI: You can also use these tensors: {}, ".format(loss_inputs))

        # compute the IL loss
        action_dist = Categorical(logits, self.options)
        self.policy_loss = policy_loss
        self.imitation_loss = tf.reduce_mean(
            -action_dist.logp(input_ops["actions"]))
        return policy_loss + 10 * self.imitation_loss

    def custom_stats(self):
        return {
            "policy_loss": self.policy_loss,
            "imitation_loss": self.imitation_loss,
        }


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()

    ModelCatalog.register_custom_model("custom_loss", CustomLossModel)
    tune.run(
        "PG",
        stop={
            "training_iteration": args.iters,
        },
        config={
            "env": "CartPole-v0",
            "num_workers": 0,
            "model": {
                "custom_model": "custom_loss",
                "custom_options": {
                    "input_files": args.input_files,
                },
            },
        },
    )
