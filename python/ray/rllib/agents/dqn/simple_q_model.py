from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class SimpleQModel(TFModelV2):
    """Extension of standard TFModel to provide Q values."""

    def get_action_scores(self, state_embedding, hiddens):
        """Returns Q(s, a) given an state embedding tensor.
        
        Arguments:
            state_embedding (Tensor): embedding from the model layers
            hiddens (list): Extra postprocessing layers (e.g., [256, 256])

        Returns:
            action scores Q(s, a) for each action.
        """

        with self.branch_variable_scope("action_scores"):
            action_out = state_embedding
            num_actions = self.action_space.n

            if hiddens:
                for i in range(len(hiddens)):
                    action_out = tf.layers.dense(
                        action_out,
                        units=hiddens[i],
                        activation=tf.nn.relu,
                        name="hidden_%d" % i)
                action_out = tf.layers.dense(
                    action_out, units=num_actions, activation=None)

            return action_out
