from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override

tf = try_import_tf()


@PublicAPI
class TFModelV2(ModelV2):
    """TF version of ModelV2.

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        """Initialize a TFModelV2.

        Here is an example implementation for a subclass
        ``MyModelClass(TFModelV2)``::

            def __init__(self, *args, **kwargs):
                super(MyModelClass, self).__init__(*args, **kwargs)
                input_layer = tf.keras.layers.Input(...)
                hidden_layer = tf.keras.layers.Dense(...)(input_layer)
                output_layer = tf.keras.layers.Dense(...)(hidden_layer)
                value_layer = tf.keras.layers.Dense(...)(hidden_layer)
                self.base_model = tf.keras.Model(
                    input_layer, [output_layer, value_layer])
                self.register_variables(self.base_model.variables)
        """

        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="tf")
        self.var_list = []
        if tf.executing_eagerly():
            self.graph = None
        else:
            self.graph = tf.get_default_graph()

    def context(self):
        """Returns a contextmanager for the current TF graph."""
        if self.graph:
            return self.graph.as_default()
        else:
            return ModelV2.context(self)

    def update_ops(self):
        """Return the list of update ops for this model.

        For example, this should include any BatchNorm update ops."""
        return []

    def register_variables(self, variables):
        """Register the given list of variables with this model."""
        self.var_list.extend(variables)

    @override(ModelV2)
    def variables(self, as_dict=False):
        if as_dict:
            return {v.name: v for v in self.var_list}
        return list(self.var_list)

    @override(ModelV2)
    def trainable_variables(self, as_dict=False):
        if as_dict:
            return {
                k: v
                for k, v in self.variables(as_dict=True).items() if v.trainable
            }
        return [v for v in self.variables() if v.trainable]
