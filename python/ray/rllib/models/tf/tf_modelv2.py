from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class TFModelV2(ModelV2):
    """TF version of ModelV2."""

    def __init__(self, obs_space, action_space, output_spec, model_config,
                 name):
        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            output_spec,
            model_config,
            name,
            framework="tf")

        # Tracks branches created so far
        self.branches_created = set()

        with tf.variable_scope(self.name) as scope:
            self.variable_scope = scope

    @override(ModelV2)
    def get_branch_output(self,
                          branch_type,
                          output_spec=None,
                          feature_layer=None,
                          default_impl=None):
        if branch_type in self.branches_created:
            reuse = True
        else:
            self.branches_created.add(branch_type)
            reuse = tf.AUTO_REUSE

        with tf.variable_scope(self.variable_scope):
            with tf.variable_scope(branch_type, reuse=reuse):
                # Custom implementation with arbitrary build function
                if default_impl:
                    return default_impl()

                raise NotImplementedError

    def _get_branch_fallback(self,
                             branch_type,
                             reuse,
                             output_spec=None,
                             feature_layer=None,
                             default_impl=None):
        raise NotImplementedError

    @override(ModelV2)
    def variables(self):
        return _scope_vars(self.variable_scope)


def _scope_vars(scope, trainable_only=False):
    """
    Get variables inside a scope
    The scope can be specified as a string

    Parameters
    ----------
    scope: str or VariableScope
      scope in which the variables reside.
    trainable_only: bool
      whether or not to return only the variables that were marked as
      trainable.

    Returns
    -------
    vars: [tf.Variable]
      list of variables in `scope`.
    """
    return tf.get_collection(
        tf.GraphKeys.TRAINABLE_VARIABLES
        if trainable_only else tf.GraphKeys.VARIABLES,
        scope=scope if isinstance(scope, str) else scope.name)
