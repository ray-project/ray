from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)


class ModelV1Wrapper(ModelV2):
    """Compatibility wrapper that allows V1 models to be used as ModelV2."""

    def __init__(self, legacy_model_cls, obs_space, action_space, num_outputs,
                 options):
        ModelV2.__init__(self, obs_space, action_space, num_outputs, options)
        self.legacy_model_cls = legacy_model_cls

        # Tracks each weight-sharing copy of the underlying V1 model
        self.instances = []

        # XXX: Try to guess the initial state size. Since the size of the state
        # is known only after forward() for V1 models, it might be wrong.
        if options.get("use_lstm"):
            cell_size = options.get("lstm_cell_size", 256)
            self.initial_state = [
                np.zeros(cell_size, np.float32),
                np.zeros(cell_size, np.float32),
            ]
        else:
            self.initial_state = []

    @override(ModelV2)
    def get_initial_state(self):
        return self.initial_state

    @override(ModelV2)
    def __call__(self, input_dict, state, seq_lens):
        if self.instances:
            # create a weight-sharing model copy
            with tf.variable_scope(self.instances[0].scope, reuse=True):
                new_instance = self.legacy_model_cls(
                    input_dict, self.obs_space, self.action_space,
                    self.num_outputs, self.options, state, seq_lens)
        else:
            # create a new model instance
            new_instance = self.legacy_model_cls(
                input_dict, self.obs_space, self.action_space,
                self.num_outputs, self.options, state, seq_lens)
        self.instances.append(new_instance)
        return (new_instance.outputs, new_instance.last_layer,
                new_instance.state_out)

    @override(ModelV2)
    def get_branch_output(self, branch_type, num_outputs, feature_layer=None):
        assert self.instances, "must call forward first"
        cur_instance = self.instances[-1]

        # Simple case: sharing the feature layer
        if feature_layer is not None:
            with tf.variable_scope(cur_instance.scope):
                return tf.reshape(
                    linear(feature_layer, num_outputs, branch_type,
                           normc_initializer(1.0)), [-1])

        # Create a new separate model with no RNN state, etc.
        branch_options = self.options.copy()
        branch_options["free_log_std"] = False
        if branch_options["use_lstm"]:
            branch_options["use_lstm"] = False
            logger.warning(
                "It is not recommended to use a LSTM model with "
                "vf_share_layers=False (consider setting it to True). "
                "If you want to not share layers, you can implement "
                "a custom LSTM model that overrides the "
                "value_function() method.")
        with tf.variable_scope(cur_instance.scope):
            with tf.variable_scope("branch_" + branch_type):
                branch_instance = self.legacy_model_cls(
                    cur_instance.input_dict,
                    self.obs_space,
                    self.action_space,
                    num_outputs,
                    branch_options,
                    state_in=None,
                    seq_lens=None)
                return tf.reshape(branch_instance.outputs, [-1])

    @override(ModelV2)
    def custom_loss(self, policy_loss, loss_inputs):
        return self.instances[-1].custom_loss(policy_loss, loss_inputs)

    @override(ModelV2)
    def custom_stats(self):
        return self.instances[-1].custom_stats()
