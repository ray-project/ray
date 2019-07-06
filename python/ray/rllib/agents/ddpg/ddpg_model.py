from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class DDPGModel(TFModelV2):
    """Extension of standard TFModel for DDPG.

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 actor_hidden_activation="relu",
                 actor_hiddens=(400, 300),
                 critic_hidden_activation="relu",
                 critic_hiddens=(400, 300),
                 parameter_noise=False,
                 twin_q=False):
        """Initialize variables of this model.

        Extra model kwargs:
            actor_hidden_activation (str): activation for actor network
            actor_hiddens (list): hidden layers sizes for actor network
            critic_hidden_activation (str): activation for critic network
            critic_hiddens (list): hidden layers sizes for critic network
            parameter_noise (bool): use param noise exploration
            twin_q (bool): build twin Q networks

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the output heads. Those layers for
        forward() should be defined in subclasses of DDPGModel.
        """

        super(DDPGModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        self.action_dim = np.product(action_space.shape)
        self.model_out = tf.keras.layers.Input(
            shape=(num_outputs, ), name="model_out")
        self.actions = tf.keras.layers.Input(
            shape=(self.action_dim, ), name="actions")

        def build_action_net(action_out):
            activation = getattr(tf.nn, actor_hidden_activation)
            for hidden in actor_hiddens:
                if parameter_noise:
                    import tensorflow.contrib.layers as layers
                    action_out = layers.fully_connected(
                        action_out,
                        num_outputs=hidden,
                        activation_fn=activation,
                        normalizer_fn=layers.layer_norm)
                else:
                    action_out = tf.layers.dense(
                        action_out, units=hidden, activation=activation)
            action_out = tf.layers.dense(
                action_out, units=self.action_dim, activation=None)
            return tf.reshape(self.action_space.shape, action_out)

        pi_out = tf.keras.layers.Lambda(build_action_net)(self.model_out)
        self.action_net = tf.keras.Model(self.model_out, pi_out)
        self.register_variables(self.action_net.variables)

        def build_q_net(model_out, actions):
            q_out = tf.concat([model_out, actions], axis=1)
            activation = getattr(tf.nn, critic_hidden_activation)
            for hidden in critic_hiddens:
                q_out = tf.layers.dense(
                    q_out, units=hidden, activation=activation)
            return tf.layers.dense(q_out, units=1, activation=None)

        q_out = tf.keras.layers.Lambda(build_q_net)(
            [self.model_out, self.actions])
        self.q_net = tf.keras.Model([self.model_out, self.actions], q_out)
        self.register_variables(self.q_out.variables)

        if twin_q:
            twin_q_out = tf.keras.layers.Lambda(build_q_net)(
                [self.model_out, self.actions])
            self.twin_q_net = tf.keras.Model(
                [self.model_out, self.actions], twin_q_out)
            self.register_variables(self.twin_q_out.variables)

    def forward(self, input_dict, state, seq_lens):
        """This generates the model_out tensor input.

        You must implement this as documented in modelv2.py."""
        raise NotImplementedError

    def get_policy_output(self, model_out):
        """Return the (unscaled) output of the policy network.

        This returns the unscaled outputs of pi(s).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].

        Returns:
            tensor of shape [BATCH_SIZE, action_dim] with range [-inf, inf].
        """
        return self.action_net(model_out)

    def get_q_values(self, model_out, actions):
        """Return the Q estimates for the most recent forward pass.

        This implements Q(s, a).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Tensor): action values that correspond with the most
                recent batch of observations passed through forward(), of shape
                [BATCH_SIZE, action_dim].

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        return self.q_net(model_out, actions)
