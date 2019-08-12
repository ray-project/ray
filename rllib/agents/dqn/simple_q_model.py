from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class SimpleQModel(TFModelV2):
    """Extension of standard TFModel to provide Q values.

    Data flow:
        obs -> forward() -> model_out
        model_out -> get_q_values() -> Q(s, a)

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 q_hiddens=(256, )):
        """Initialize variables of this model.

        Extra model kwargs:
            q_hiddens (list): defines size of hidden layers for the q head.
                These will be used to postprocess the model output for the
                purposes of computing Q values.

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the Q head. Those layers for forward()
        should be defined in subclasses of SimpleQModel.
        """

        super(SimpleQModel, self).__init__(obs_space, action_space,
                                           num_outputs, model_config, name)

        # setup the Q head output (i.e., model for get_q_values)
        self.model_out = tf.keras.layers.Input(
            shape=(num_outputs, ), name="model_out")

        if q_hiddens:
            last_layer = self.model_out
            for i, n in enumerate(q_hiddens):
                last_layer = tf.keras.layers.Dense(
                    n, name="q_hidden_{}".format(i),
                    activation=tf.nn.relu)(last_layer)
            q_out = tf.keras.layers.Dense(
                action_space.n, activation=None, name="q_out")(last_layer)
        else:
            q_out = self.model_out

        self.q_value_head = tf.keras.Model(self.model_out, q_out)
        self.register_variables(self.q_value_head.variables)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        """This generates the model_out tensor input.

        You must implement this as documented in modelv2.py."""
        raise NotImplementedError

    def get_q_values(self, model_out):
        """Returns Q(s, a) given a feature tensor for the state.

        Override this in your custom model to customize the Q output head.

        Arguments:
            model_out (Tensor): embedding from the model layers

        Returns:
            action scores Q(s, a) for each action, shape [None, action_space.n]
        """

        return self.q_value_head(model_out)
