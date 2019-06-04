from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


@PublicAPI
class TFModel(object):
    def __init__(self, obs_space, action_space, num_outputs, options):
        self.obs_space = obs_space
        self.action_space = action_space
        self.num_outputs = num_outputs
        self.options = options

    @PublicAPI
    def forward(self, input_dict, state, seq_lens):
        raise NotImplementedError

    @PublicAPI
    def get_branch_output(self, branch_type, num_outputs, feature_layer=None):
        raise NotImplementedError

    @PublicAPI
    def copy_share_weights(self, new_input_dict, new_name):
        return self  # this default works for keras type models

    @PublicAPI
    def get_initial_state(self):
        return None


class TFModelLegacyWrapper(TFModel):
    def __init__(self,
                 legacy_model_cls,
                 input_dict,
                 obs_space,
                 action_space,
                 num_outputs,
                 options,
                 existing_state_in=None,
                 existing_seq_lens=None):
        TFModel.__init__(self, obs_space, action_space, options)
        self.legacy_model_cls = legacy_model_cls
        self.input_dict = input_dict
        self.existing_state_in = existing_state_in
        self.existing_seq_lens = existing_seq_lens
        self.instance = legacy_model_cls(
            input_dict, obs_space, action_space, num_outputs, options,
            existing_state_in, existing_seq_lens)
        if self.instance.state_out:
            assert len(self.instance.state_out) == 1, self.instance.state_init
            self.state_out_tensor = self.instance.state_out[0]
        else:
            self.state_out_tensor = tf.constant([])

    @override(TFModel)
    def forward(self, input_dict, state, seq_lens):
        return (
            self.instance.outputs,
            self.instance.last_layer,
            self.state_out_tensor)

    @override(TFModel)
    def get_branch_output(self, branch_type, num_outputs, feature_layer=None):
        with tf.variable_scope(self.instance.scope):
            if not feature_layer:
                feature_layer = self.input_dict["obs"]
            return tf.reshape(
                linear(feature_layer, num_outputs, branch_type,
                    normc_initializer(1.0)), [-1])

    @override(TFModel)
    def copy_share_weights(
            self, new_name, new_input_dict, new_state_in, new_seq_lens):
        with tf.variable_scope(new_name):
            return TFModelLegacyWrapper(
                self.legacy_model_cls,
                new_input_dict,
                self.obs_space,
                self.action_space,
                self.num_outputs,
                self.options,
                new_state_in,
                new_seq_lens)

    @override(TFModel)
    def get_initial_state(self):
        if self.instance.state_init:
            assert len(self.instance.state_init) == 1, self.instance.state_init
            return self.instance.state_init[0]
        else:
            return np.array([], dtype=np.float32)
