from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.lstm import add_time_dimension
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class RecurrentTFModelV2(TFModelV2):
    """Helper class to simplify implementing RNN models with TFModelV2."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TFModelV2.__init__(self, obs_space, action_space, num_outputs,
                           model_config, name)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        # add time dimension and run rnn forward
        output, new_state = self.forward_rnn(
            add_time_dimension(input_dict["obs_flat"], seq_lens), state,
            seq_lens)
        # reflatten and return
        return tf.reshape(output, [-1, self.num_outputs]), new_state

    def get_initial_state(self):
        raise NotImplementedError("You must implement this for a RNN model")

    def forward_rnn(self, inputs, state, seq_lens):
        raise NotImplementedError("You must implement this for a RNN model")
