# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================
"""Base model configuration for CNN benchmarks."""
import tensorflow as tf

from . import convnet_builder


class Model(object):
    """Base model configuration for CNN benchmarks."""

    def __init__(self,
                 model,
                 image_size,
                 batch_size,
                 learning_rate,
                 layer_counts=None,
                 fp16_loss_scale=128):
        self.model = model
        self.image_size = image_size
        self.batch_size = batch_size
        self.default_batch_size = batch_size
        self.learning_rate = learning_rate
        self.layer_counts = layer_counts
        self.fp16_loss_scale = fp16_loss_scale

    def get_model(self):
        return self.model

    def get_image_size(self):
        return self.image_size

    def get_batch_size(self):
        return self.batch_size

    def set_batch_size(self, batch_size):
        self.batch_size = batch_size

    def get_default_batch_size(self):
        return self.default_batch_size

    def get_layer_counts(self):
        return self.layer_counts

    def get_fp16_loss_scale(self):
        return self.fp16_loss_scale

    def get_learning_rate(self, global_step, batch_size):
        del global_step
        del batch_size
        return self.learning_rate

    def add_inference(self, unused_cnn):
        raise ValueError('Must be implemented in derived classes')

    def skip_final_affine_layer(self):
        """Returns if the caller of this class should skip the final affine

    Normally, this class adds a final affine layer to the model after calling
    self.add_inference(), to generate the logits. If a subclass override this
    method to return True, the caller should not add the final affine layer.

    This is useful for tests.
    """
        return False

    def build_network(self,
                      images,
                      phase_train=True,
                      nclass=1001,
                      image_depth=3,
                      data_type=tf.float32,
                      data_format='NCHW',
                      use_tf_layers=True,
                      fp16_vars=False):
        """Returns logits and aux_logits from images."""
        if data_format == 'NCHW':
            images = tf.transpose(images, [0, 3, 1, 2])
        var_type = tf.float32
        if data_type == tf.float16 and fp16_vars:
            var_type = tf.float16
        network = convnet_builder.ConvNetBuilder(
            images, image_depth, phase_train, use_tf_layers, data_format,
            data_type, var_type)
        with tf.variable_scope(
                'cg', custom_getter=network.get_custom_getter()):
            self.add_inference(network)
            # Add the final fully-connected class layer
            logits = (network.affine(nclass, activation='linear')
                      if not self.skip_final_affine_layer() else
                      network.top_layer)
            aux_logits = None
            if network.aux_top_layer is not None:
                with network.switch_to_aux_top_layer():
                    aux_logits = network.affine(
                        nclass, activation='linear', stddev=0.001)
        if data_type == tf.float16:
            # TODO(reedwm): Determine if we should do this cast here.
            logits = tf.cast(logits, tf.float32)
            if aux_logits is not None:
                aux_logits = tf.cast(aux_logits, tf.float32)
        return logits, aux_logits

    loss_function = None
