from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model
from ray.rllib.models.misc import get_activation_fn

class AutoEncoder(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        """Define the layers of a custom model.

        Arguments:
            input_dict (dict): Dictionary of input tensors, including "obs",
                "prev_action", "prev_reward", "is_training".
            num_outputs (int): Output tensor must be of size
                [BATCH_SIZE, num_outputs].
            options (dict): Model options.

        Returns:
            (outputs, feature_layer): Tensors of size [BATCH_SIZE, num_outputs]
                and [BATCH_SIZE, desired_feature_size].

        When using dict or tuple observation spaces, you can access
        the nested sub-observation batches here as well:

        Examples:
            >>> print(input_dict)
            {'prev_actions': <tf.Tensor shape=(?,) dtype=int64>,
             'prev_rewards': <tf.Tensor shape=(?,) dtype=float32>,
             'is_training': <tf.Tensor shape=(), dtype=bool>,
             'obs': OrderedDict([
                ('sensors', OrderedDict([
                    ('front_cam', [
                        <tf.Tensor shape=(?, 10, 10, 3) dtype=float32>,
                        <tf.Tensor shape=(?, 10, 10, 3) dtype=float32>]),
                    ('position', <tf.Tensor shape=(?, 3) dtype=float32>),
                    ('velocity', <tf.Tensor shape=(?, 3) dtype=float32>)]))])}
        """

        self.inputs = input_dict["obs"]
        # Hyperparameters
        encoder_params = [4, [8,8], 2]
        decoder_params = [4, [8,8], 2]
        print("fuckery")
        self.encoder= slim.conv2d(self.inputs, encoder_params[0], encoder_params[1], encoder_params[2], activation_fn = tf.nn.relu)
        self.decoder = slim.conv2d_transpose(self.encoder, decoder_params[0], decoder_params[1], decoder_params[2], activation_fn = None)
        print(self.encoder)
        print(self.decoder)
        
        return self.decoder, self.encoder

    def loss(self):
        """Builds any built-in (self-supervised) loss for the model.

        For example, this can be used to incorporate auto-encoder style losses.
        Note that this loss has to be included in the policy graph loss to have
        an effect (done for built-in algorithms).

        Returns:
            Scalar tensor for the self-supervised loss.
        """
        return tf.reduce_mean(tf.nn.l2_loss(self.decoder - self.inputs)) 

'''
ray.init()
agent = ppo.PPOAgent(env="CartPole-v0", config={
    "model": {
        "custom_model": "my_model",
        "custom_options": {},  # extra options to pass to your model
    },
})
'''