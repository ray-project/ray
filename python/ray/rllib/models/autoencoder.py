from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model
from ray.rllib.models.misc import get_activation_fn, flatten

class AutoEncoder(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        self.inputs = input_dict["obs"]
        inputs = self.inputs
        filters = [
        #[8, [8,8], 1],
        [16, [4, 4], 2],
        [32, [4, 4], 2],
        [256, [11, 11], 1],
        ]

        deconv_filters_42x42 = [
            #[4, [42, 42], 1],
            [2, [21, 21], 1],
            [4, [22, 22], 1]
        ]

        activation = get_activation_fn(options.get("conv_activation"))

        with tf.name_scope("encoder"):
            for i, (out_size, kernel, stride) in enumerate(filters[:-1], 1):
                print(inputs)
                inputs = slim.conv2d(
                    inputs,
                    out_size,
                    kernel,
                    stride,
                    activation_fn=activation,
                    scope="conv{}".format(i))
                print(inputs)
            out_size, kernel, stride = filters[-1]
            fc1 = slim.conv2d(
                inputs,
                out_size,
                kernel,
                stride,
                activation_fn=activation,
                padding="VALID",
                scope="fc1")
            fc2 = slim.conv2d(
                fc1,
                num_outputs, [1, 1],
                activation_fn=None,
                normalizer_fn=None,
                scope="fc2")
            
            #Building Decoder
            self.encoder = fc1
            print(self.encoder)
            inputs = fc1
            for i, (out_size, kernel, stride) in enumerate(deconv_filters_42x42,1):
                inputs = slim.conv2d_transpose(
                        inputs,
                        out_size,
                        kernel,
                        stride,
                        activation_fn = None,
                        padding = "VALID",
                        scope="deconv{}".format(i)
                    )
            self.decoder = inputs
            print(self.decoder)
        return flatten(fc2), flatten(fc1)

    def loss(self):
        return tf.reduce_mean(tf.nn.l2_loss(self.decoder - self.inputs)) 
