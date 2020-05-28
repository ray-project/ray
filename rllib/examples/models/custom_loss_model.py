from ray.rllib.models.model import Model, restore_original_dimensions
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFC
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.offline import JsonReader

tf = try_import_tf()
torch, nn = try_import_torch()


class CustomLossModel(TFModelV2):
    """Custom model that adds an imitation loss on top of the policy loss."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)

        self.fcnet = FullyConnectedNetwork(
            self.obs_space,
            self.action_space,
            num_outputs,
            model_config,
            name="fcnet")
        self.register_variables(self.fcnet.variables())

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        # Delegate to our FCNet.
        return self.fcnet(input_dict, state, seq_lens)

    @override(ModelV2)
    def custom_loss(self, policy_loss, loss_inputs):
        # Create a new input reader per worker.
        reader = JsonReader(
            self.model_config["custom_model_config"]["input_files"])
        input_ops = reader.tf_input_ops()

        # Define a secondary loss by building a graph copy with weight sharing.
        obs = restore_original_dimensions(
            tf.cast(input_ops["obs"], tf.float32), self.obs_space)
        logits, _ = self.forward({"obs": obs}, [], None)

        # You can also add self-supervised losses easily by referencing tensors
        # created during _build_layers_v2(). For example, an autoencoder-style
        # loss can be added as follows:
        # ae_loss = squared_diff(
        #     loss_inputs["obs"], Decoder(self.fcnet.last_layer))
        print("FYI: You can also use these tensors: {}, ".format(loss_inputs))

        # Compute the IL loss.
        action_dist = Categorical(logits, self.model_config)
        self.policy_loss = policy_loss
        self.imitation_loss = tf.reduce_mean(
            -action_dist.logp(input_ops["actions"]))
        return policy_loss + 10 * self.imitation_loss

    def custom_stats(self):
        return {
            "policy_loss": self.policy_loss,
            "imitation_loss": self.imitation_loss,
        }


class DeprecatedCustomLossModelV1(Model):
    """Model(V1) version of above custom-loss model."""

    def _build_layers_v2(self, input_dict, num_outputs, options):
        self.obs_in = input_dict["obs"]
        with tf.variable_scope("shared", reuse=tf.AUTO_REUSE):
            self.fcnet = FullyConnectedNetwork(input_dict, self.obs_space,
                                               self.action_space, num_outputs,
                                               options)
        return self.fcnet.outputs, self.fcnet.last_layer

    def custom_loss(self, policy_loss, loss_inputs):
        # create a new input reader per worker
        reader = JsonReader(self.options["custom_model_config"]["input_files"])
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


class TorchCustomLossModel(TorchModelV2, nn.Module):
    """PyTorch version of the CustomLossModel above."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name, input_files):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        nn.Module.__init__(self)

        self.input_files = input_files
        self.fcnet = TorchFC(
            self.obs_space,
            self.action_space,
            num_outputs,
            model_config,
            name="fcnet")

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        # Delegate to our FCNet.
        return self.fcnet(input_dict, state, seq_lens)

    @override(ModelV2)
    def custom_loss(self, policy_loss, loss_inputs):
        # Create a new input reader per worker.
        reader = JsonReader(self.input_files)
        input_ops = reader.tf_input_ops()

        # Define a secondary loss by building a graph copy with weight sharing.
        obs = restore_original_dimensions(
            tf.cast(input_ops["obs"], tf.float32), self.obs_space)
        logits, _ = self.forward({"obs": obs}, [], None)

        # You can also add self-supervised losses easily by referencing tensors
        # created during _build_layers_v2(). For example, an autoencoder-style
        # loss can be added as follows:
        # ae_loss = squared_diff(
        #     loss_inputs["obs"], Decoder(self.fcnet.last_layer))
        print("FYI: You can also use these tensors: {}, ".format(loss_inputs))

        # Compute the IL loss.
        action_dist = TorchCategorical(logits, self.model_config)
        self.policy_loss = policy_loss
        self.imitation_loss = torch.mean(
            -action_dist.logp(input_ops["actions"]))
        return policy_loss + 10 * self.imitation_loss

    def custom_stats(self):
        return {
            "policy_loss": self.policy_loss,
            "imitation_loss": self.imitation_loss,
        }
