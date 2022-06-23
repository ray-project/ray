from gym.spaces import Box

from ray.rllib.algorithms.dqn.distributional_q_tf_model import DistributionalQTFModel
from ray.rllib.algorithms.dqn.dqn_torch_model import DQNTorchModel
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFC
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.torch_utils import FLOAT_MIN, FLOAT_MAX

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class ParametricActionsModel(DistributionalQTFModel):
    """Parametric action model that handles the dot product and masking.

    This assumes the outputs are logits for a single Categorical action dist.
    Getting this to work with a more complex output (e.g., if the action space
    is a tuple of several distributions) is also possible but left as an
    exercise to the reader.
    """

    def __init__(
        self,
        obs_space,
        action_space,
        num_outputs,
        model_config,
        name,
        true_obs_shape=(4,),
        action_embed_size=2,
        **kw
    ):
        super(ParametricActionsModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name, **kw
        )
        self.action_embed_model = FullyConnectedNetwork(
            Box(-1, 1, shape=true_obs_shape),
            action_space,
            action_embed_size,
            model_config,
            name + "_action_embed",
        )

    def forward(self, input_dict, state, seq_lens):
        # Extract the available actions tensor from the observation.
        avail_actions = input_dict["obs"]["avail_actions"]
        action_mask = input_dict["obs"]["action_mask"]

        # Compute the predicted action embedding
        action_embed, _ = self.action_embed_model({"obs": input_dict["obs"]["cart"]})

        # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
        # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
        intent_vector = tf.expand_dims(action_embed, 1)

        # Batch dot product => shape of logits is [BATCH, MAX_ACTIONS].
        action_logits = tf.reduce_sum(avail_actions * intent_vector, axis=2)

        # Mask out invalid actions (use tf.float32.min for stability)
        inf_mask = tf.maximum(tf.math.log(action_mask), tf.float32.min)
        return action_logits + inf_mask, state

    def value_function(self):
        return self.action_embed_model.value_function()


class TorchParametricActionsModel(DQNTorchModel):
    """PyTorch version of above ParametricActionsModel."""

    def __init__(
        self,
        obs_space,
        action_space,
        num_outputs,
        model_config,
        name,
        true_obs_shape=(4,),
        action_embed_size=2,
        **kw
    ):
        DQNTorchModel.__init__(
            self, obs_space, action_space, num_outputs, model_config, name, **kw
        )

        self.action_embed_model = TorchFC(
            Box(-1, 1, shape=true_obs_shape),
            action_space,
            action_embed_size,
            model_config,
            name + "_action_embed",
        )

    def forward(self, input_dict, state, seq_lens):
        # Extract the available actions tensor from the observation.
        avail_actions = input_dict["obs"]["avail_actions"]
        action_mask = input_dict["obs"]["action_mask"]

        # Compute the predicted action embedding
        action_embed, _ = self.action_embed_model({"obs": input_dict["obs"]["cart"]})

        # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
        # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
        intent_vector = torch.unsqueeze(action_embed, 1)

        # Batch dot product => shape of logits is [BATCH, MAX_ACTIONS].
        action_logits = torch.sum(avail_actions * intent_vector, dim=2)

        # Mask out invalid actions (use -inf to tag invalid).
        # These are then recognized by the EpsilonGreedy exploration component
        # as invalid actions that are not to be chosen.
        inf_mask = torch.clamp(torch.log(action_mask), FLOAT_MIN, FLOAT_MAX)

        return action_logits + inf_mask, state

    def value_function(self):
        return self.action_embed_model.value_function()


class ParametricActionsModelThatLearnsEmbeddings(DistributionalQTFModel):
    """Same as the above ParametricActionsModel.

    However, this version also learns the action embeddings.
    """

    def __init__(
        self,
        obs_space,
        action_space,
        num_outputs,
        model_config,
        name,
        true_obs_shape=(4,),
        action_embed_size=2,
        **kw
    ):
        super(ParametricActionsModelThatLearnsEmbeddings, self).__init__(
            obs_space, action_space, num_outputs, model_config, name, **kw
        )

        action_ids_shifted = tf.constant(
            list(range(1, num_outputs + 1)), dtype=tf.float32
        )

        obs_cart = tf.keras.layers.Input(shape=true_obs_shape, name="obs_cart")
        valid_avail_actions_mask = tf.keras.layers.Input(
            shape=(num_outputs), name="valid_avail_actions_mask"
        )

        self.pred_action_embed_model = FullyConnectedNetwork(
            Box(-1, 1, shape=true_obs_shape),
            action_space,
            action_embed_size,
            model_config,
            name + "_pred_action_embed",
        )

        # Compute the predicted action embedding
        pred_action_embed, _ = self.pred_action_embed_model({"obs": obs_cart})
        _value_out = self.pred_action_embed_model.value_function()

        # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
        # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
        intent_vector = tf.expand_dims(pred_action_embed, 1)

        valid_avail_actions = action_ids_shifted * valid_avail_actions_mask
        # Embedding for valid available actions which will be learned.
        # Embedding vector for 0 is an invalid embedding (a "dummy embedding").
        valid_avail_actions_embed = tf.keras.layers.Embedding(
            input_dim=num_outputs + 1,
            output_dim=action_embed_size,
            name="action_embed_matrix",
        )(valid_avail_actions)

        # Batch dot product => shape of logits is [BATCH, MAX_ACTIONS].
        action_logits = tf.reduce_sum(valid_avail_actions_embed * intent_vector, axis=2)

        # Mask out invalid actions (use tf.float32.min for stability)
        inf_mask = tf.maximum(tf.math.log(valid_avail_actions_mask), tf.float32.min)

        action_logits = action_logits + inf_mask

        self.param_actions_model = tf.keras.Model(
            inputs=[obs_cart, valid_avail_actions_mask],
            outputs=[action_logits, _value_out],
        )
        self.param_actions_model.summary()

    def forward(self, input_dict, state, seq_lens):
        # Extract the available actions mask tensor from the observation.
        valid_avail_actions_mask = input_dict["obs"]["valid_avail_actions_mask"]

        action_logits, self._value_out = self.param_actions_model(
            [input_dict["obs"]["cart"], valid_avail_actions_mask]
        )

        return action_logits, state

    def value_function(self):
        return self._value_out
