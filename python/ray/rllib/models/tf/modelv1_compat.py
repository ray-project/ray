from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.misc import linear, normc_initializer
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.tf_ops import scope_vars

tf = try_import_tf()

logger = logging.getLogger(__name__)


def make_v1_wrapper(legacy_model_cls):
    class ModelV1Wrapper(TFModelV2):
        """Wrapper that allows V1 models to be used as ModelV2."""

        def __init__(self, obs_space, action_space, num_outputs, model_config,
                     name):
            TFModelV2.__init__(self, obs_space, action_space, num_outputs,
                               model_config, name)
            self.legacy_model_cls = legacy_model_cls

            # Tracks the last v1 model created by the call to forward
            self.cur_instance = None

            # XXX: Try to guess the initial state size. Since the size of the
            # state is known only after forward() for V1 models, it might be
            # wrong.
            if model_config.get("state_shape"):
                self.initial_state = [
                    np.zeros(s, np.float32)
                    for s in model_config["state_shape"]
                ]
            elif model_config.get("use_lstm"):
                cell_size = model_config.get("lstm_cell_size", 256)
                self.initial_state = [
                    np.zeros(cell_size, np.float32),
                    np.zeros(cell_size, np.float32),
                ]
            else:
                self.initial_state = []

            # Tracks branches created so far
            self.branches_created = set()

            # Tracks update ops
            self._update_ops = None

            with tf.variable_scope(self.name) as scope:
                self.variable_scope = scope

        @override(ModelV2)
        def get_initial_state(self):
            return self.initial_state

        @override(ModelV2)
        def __call__(self, input_dict, state, seq_lens):
            if self.cur_instance:
                # create a weight-sharing model copy
                with tf.variable_scope(self.cur_instance.scope, reuse=True):
                    new_instance = self.legacy_model_cls(
                        input_dict, self.obs_space, self.action_space,
                        self.num_outputs, self.model_config, state, seq_lens)
            else:
                # create a new model instance
                with tf.variable_scope(self.name):
                    prev_update_ops = set(
                        tf.get_collection(tf.GraphKeys.UPDATE_OPS))
                    new_instance = self.legacy_model_cls(
                        input_dict, self.obs_space, self.action_space,
                        self.num_outputs, self.model_config, state, seq_lens)
                    self._update_ops = list(
                        set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) -
                        prev_update_ops)
            if len(new_instance.state_init) != len(self.get_initial_state()):
                raise ValueError(
                    "When using a custom recurrent ModelV1 model, you should "
                    "declare the state_shape in the model options. For "
                    "example, set 'state_shape': [256, 256] for a lstm with "
                    "cell size 256. The guessed state shape was {} which "
                    "appears to be incorrect.".format(
                        [s.shape[0] for s in self.get_initial_state()]))
            self.cur_instance = new_instance
            self.variable_scope = new_instance.scope
            return new_instance.outputs, new_instance.state_out

        @override(TFModelV2)
        def update_ops(self):
            if self._update_ops is None:
                raise ValueError(
                    "Cannot get update ops before wrapped v1 model init")
            return list(self._update_ops)

        @override(TFModelV2)
        def variables(self):
            var_list = super(ModelV1Wrapper, self).variables()
            for v in scope_vars(self.variable_scope):
                if v not in var_list:
                    var_list.append(v)
            return var_list

        @override(ModelV2)
        def custom_loss(self, policy_loss, loss_inputs):
            return self.cur_instance.custom_loss(policy_loss, loss_inputs)

        @override(ModelV2)
        def metrics(self):
            return self.cur_instance.custom_stats()

        @override(ModelV2)
        def value_function(self):
            assert self.cur_instance, "must call forward first"

            with self._branch_variable_scope("value_function"):
                # Simple case: sharing the feature layer
                if self.model_config["vf_share_layers"]:
                    return tf.reshape(
                        linear(self.cur_instance.last_layer, 1,
                               "value_function", normc_initializer(1.0)), [-1])

                # Create a new separate model with no RNN state, etc.
                branch_model_config = self.model_config.copy()
                branch_model_config["free_log_std"] = False
                if branch_model_config["use_lstm"]:
                    branch_model_config["use_lstm"] = False
                    logger.warning(
                        "It is not recommended to use a LSTM model with "
                        "vf_share_layers=False (consider setting it to True). "
                        "If you want to not share layers, you can implement "
                        "a custom LSTM model that overrides the "
                        "value_function() method.")
                branch_instance = self.legacy_model_cls(
                    self.cur_instance.input_dict,
                    self.obs_space,
                    self.action_space,
                    1,
                    branch_model_config,
                    state_in=None,
                    seq_lens=None)
                return tf.reshape(branch_instance.outputs, [-1])

        def _branch_variable_scope(self, branch_type):
            if branch_type in self.branches_created:
                reuse = True
            else:
                self.branches_created.add(branch_type)
                reuse = tf.AUTO_REUSE

            with tf.variable_scope(self.variable_scope):
                return tf.variable_scope(branch_type, reuse=reuse)

    return ModelV1Wrapper
