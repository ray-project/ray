from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class DDPGActionModel(TFModelV2):
    """Extension of standard TFModel to provide DDPG action outputs.

    Data flow:
        obs -> forward() -> model_out
        model_out -> get_policy_output() -> deterministic actions

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            # Extra DDPGActionModel args:
            actor_hiddens,
            actor_hidden_activation,
            add_layer_norm=False):
        """Initialize variables of this model.

        Extra model kwargs:
            actor_hiddens (list): Defines size of hidden layers for the DDPG
                policy head.
                These will be used to postprocess the model output for the
                purposes of computing deterministic actions.

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the DDPG head. Those layers for forward()
        should be defined in subclasses of DDPGActionModel.
        """

        super(DDPGActionModel, self).__init__(obs_space, action_space,
                                              num_outputs, model_config, name)

        actor_hidden_activation = getattr(tf.nn, actor_hidden_activation,
                                          tf.nn.relu)

        # setup the Q head output (i.e., model for get_q_values)
        self.model_out = tf.keras.layers.Input(
            shape=(num_outputs, ), name="model_out")

        if actor_hiddens:
            last_layer = self.model_out
            for i, n in enumerate(actor_hiddens):
                last_layer = tf.keras.layers.Dense(
                    n,
                    name="actor_hidden_{}".format(i),
                    activation=actor_hidden_activation)(last_layer)
                if add_layer_norm:
                    last_layer = tf.keras.layers.LayerNormalization(
                        name="LayerNorm_{}".format(i))(last_layer)
            actor_out = tf.keras.layers.Dense(
                action_space.shape[0], activation=None,
                name="actor_out")(last_layer)
        else:
            actor_out = self.model_out

        # Use sigmoid to scale to [0,1], but also double magnitude of input to
        # emulate behaviour of tanh activation used in DDPG and TD3 papers.
        def lambda_(x):
            sigmoid_out = tf.nn.sigmoid(2 * x)
            # Rescale to actual env policy scale
            # (shape of sigmoid_out is [batch_size, dim_actions], so we reshape
            # to get same dims)
            action_range = (action_space.high - action_space.low)[None]
            low_action = action_space.low[None]
            actions = action_range * sigmoid_out + low_action
            return actions

        actor_out = tf.keras.layers.Lambda(lambda_)(actor_out)

        self.actor_model = tf.keras.Model(self.model_out, actor_out)
        self.register_variables(self.actor_model.variables)

    def get_policy_out(self, model_out):
        return self.actor_model(model_out)
