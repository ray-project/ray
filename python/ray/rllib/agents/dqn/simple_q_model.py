from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class SimpleQModel(TFModelV2):
    """Extension of standard TFModel to provide Q values."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super(SimpleQModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        self.feature_input = tf.keras.layers.Input(
            shape=(None, num_outputs), name="features")

        layer_1 = tf.keras.layers.Dense(256, name="layer1")(self.feature_input)
        layer_2 = tf.keras.layers.Dense(256, name="layer2")(layer_1)
        layer_out = tf.keras.layers.Dense(
            num_outputs, name="out", activation=None)(layer_2)

        self.actions_model = tf.keras.Model(self.feature_input, layer_out)
        self.register_model_variables(self.actions_model)

    def get_action_scores(self, state_embedding, hiddens):
        """Returns Q(s, a) given a state embedding tensor.
        
        Arguments:
            state_embedding (Tensor): embedding from the model layers
            hiddens (list): Extra postprocessing layers (e.g., [256, 256])

        Returns:
            action scores Q(s, a) for each action.
        """

        return self.actions_model(state_embedding)
