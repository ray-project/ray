import numpy as np
import gym
from gym.spaces import Box, Discrete, MultiDiscrete
import logging
import tree  # pip install dm_tree
from typing import Dict, List, Optional, Type

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.tf_utils import flatten_inputs_to_1d_tensor, one_hot
from ray.rllib.utils.typing import ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


@DeveloperAPI
class RecurrentNetwork(TFModelV2):
    """Helper class to simplify implementing RNN models with TFModelV2.

    Instead of implementing forward(), you can implement forward_rnn() which
    takes batches with the time dimension added already.

    Here is an example implementation for a subclass
    ``MyRNNClass(RecurrentNetwork)``::

        def __init__(self, *args, **kwargs):
            super(MyModelClass, self).__init__(*args, **kwargs)
            cell_size = 256

            # Define input layers
            input_layer = tf.keras.layers.Input(
                shape=(None, obs_space.shape[0]))
            state_in_h = tf.keras.layers.Input(shape=(256, ))
            state_in_c = tf.keras.layers.Input(shape=(256, ))
            seq_in = tf.keras.layers.Input(shape=(), dtype=tf.int32)

            # Send to LSTM cell
            lstm_out, state_h, state_c = tf.keras.layers.LSTM(
                cell_size, return_sequences=True, return_state=True,
                name="lstm")(
                    inputs=input_layer,
                    mask=tf.sequence_mask(seq_in),
                    initial_state=[state_in_h, state_in_c])
            output_layer = tf.keras.layers.Dense(...)(lstm_out)

            # Create the RNN model
            self.rnn_model = tf.keras.Model(
                inputs=[input_layer, seq_in, state_in_h, state_in_c],
                outputs=[output_layer, state_h, state_c])
            self.rnn_model.summary()
    """

    @override(ModelV2)
    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        """Adds time dimension to batch before sending inputs to forward_rnn().

        You should implement forward_rnn() in your subclass."""
        assert seq_lens is not None
        padded_inputs = input_dict["obs_flat"]
        max_seq_len = tf.shape(padded_inputs)[0] // tf.shape(seq_lens)[0]
        output, new_state = self.forward_rnn(
            add_time_dimension(
                padded_inputs, max_seq_len=max_seq_len, framework="tf"), state,
            seq_lens)
        return tf.reshape(output, [-1, self.num_outputs]), new_state

    def forward_rnn(self, inputs: TensorType, state: List[TensorType],
                    seq_lens: TensorType) -> (TensorType, List[TensorType]):
        """Call the model with the given input tensors and state.

        Args:
            inputs (dict): observation tensor with shape [B, T, obs_size].
            state (list): list of state tensors, each with shape [B, T, size].
            seq_lens (Tensor): 1d tensor holding input sequence lengths.

        Returns:
            (outputs, new_state): The model output tensor of shape
                [B, T, num_outputs] and the list of new state tensors each with
                shape [B, size].

        Sample implementation for the ``MyRNNClass`` example::

            def forward_rnn(self, inputs, state, seq_lens):
                model_out, h, c = self.rnn_model([inputs, seq_lens] + state)
                return model_out, [h, c]
        """
        raise NotImplementedError("You must implement this for a RNN model")

    def get_initial_state(self) -> List[TensorType]:
        """Get the initial recurrent state values for the model.

        Returns:
            list of np.array objects, if any

        Sample implementation for the ``MyRNNClass`` example::

            def get_initial_state(self):
                return [
                    np.zeros(self.cell_size, np.float32),
                    np.zeros(self.cell_size, np.float32),
                ]
        """
        raise NotImplementedError("You must implement this for a RNN model")


class LSTMWrapper(RecurrentNetwork):
    """An LSTM wrapper serving as an interface for ModelV2s that set use_lstm.
    """

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):

        super(LSTMWrapper, self).__init__(obs_space, action_space, None,
                                          model_config, name)
        # At this point, self.num_outputs is the number of nodes coming
        # from the wrapped (underlying) model. In other words, self.num_outputs
        # is the input size for the LSTM layer.
        # If None, set it to the observation space.
        if self.num_outputs is None:
            self.num_outputs = int(np.product(self.obs_space.shape))

        self.cell_size = model_config["lstm_cell_size"]
        self.use_prev_action = model_config["lstm_use_prev_action"]
        self.use_prev_reward = model_config["lstm_use_prev_reward"]

        self.action_space_struct = get_base_struct_from_space(
            self.action_space)
        self.action_dim = 0

        for space in tree.flatten(self.action_space_struct):
            if isinstance(space, Discrete):
                self.action_dim += space.n
            elif isinstance(space, MultiDiscrete):
                self.action_dim += np.sum(space.nvec)
            elif space.shape is not None:
                self.action_dim += int(np.product(space.shape))
            else:
                self.action_dim += int(len(space))

        # Add prev-action/reward nodes to input to LSTM.
        if self.use_prev_action:
            self.num_outputs += self.action_dim
        if self.use_prev_reward:
            self.num_outputs += 1

        # Define input layers.
        input_layer = tf.keras.layers.Input(
            shape=(None, self.num_outputs), name="inputs")

        # Set self.num_outputs to the number of output nodes desired by the
        # caller of this constructor.
        self.num_outputs = num_outputs

        state_in_h = tf.keras.layers.Input(shape=(self.cell_size, ), name="h")
        state_in_c = tf.keras.layers.Input(shape=(self.cell_size, ), name="c")
        seq_in = tf.keras.layers.Input(shape=(), name="seq_in", dtype=tf.int32)

        # Preprocess observation with a hidden layer and send to LSTM cell
        lstm_out, state_h, state_c = tf.keras.layers.LSTM(
            self.cell_size,
            return_sequences=True,
            return_state=True,
            name="lstm")(
                inputs=input_layer,
                mask=tf.sequence_mask(seq_in),
                initial_state=[state_in_h, state_in_c])

        # Postprocess LSTM output with another hidden layer and compute values
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(lstm_out)
        values = tf.keras.layers.Dense(
            1, activation=None, name="values")(lstm_out)

        # Create the RNN model
        self._rnn_model = tf.keras.Model(
            inputs=[input_layer, seq_in, state_in_h, state_in_c],
            outputs=[logits, values, state_h, state_c])
        # Print out model summary in INFO logging mode.
        if logger.isEnabledFor(logging.INFO):
            self._rnn_model.summary()

        # Add prev-a/r to this model's view, if required.
        if model_config["lstm_use_prev_action"]:
            self.view_requirements[SampleBatch.PREV_ACTIONS] = \
                ViewRequirement(SampleBatch.ACTIONS, space=self.action_space,
                                shift=-1)
        if model_config["lstm_use_prev_reward"]:
            self.view_requirements[SampleBatch.PREV_REWARDS] = \
                ViewRequirement(SampleBatch.REWARDS, shift=-1)

    @override(RecurrentNetwork)
    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        assert seq_lens is not None
        # Push obs through "unwrapped" net's `forward()` first.
        wrapped_out, _ = self._wrapped_forward(input_dict, [], None)

        # Concat. prev-action/reward if required.
        prev_a_r = []

        # Prev actions.
        if self.model_config["lstm_use_prev_action"]:
            prev_a = input_dict[SampleBatch.PREV_ACTIONS]
            # If actions are not processed yet (in their original form as
            # have been sent to environment):
            # Flatten/one-hot into 1D array.
            if self.model_config["_disable_action_flattening"]:
                prev_a_r.append(
                    flatten_inputs_to_1d_tensor(
                        prev_a,
                        spaces_struct=self.action_space_struct,
                        time_axis=False,
                    ))
            # If actions are already flattened (but not one-hot'd yet!),
            # one-hot discrete/multi-discrete actions here.
            else:
                if isinstance(self.action_space, (Discrete, MultiDiscrete)):
                    prev_a = one_hot(prev_a, self.action_space)
                prev_a_r.append(
                    tf.reshape(
                        tf.cast(prev_a, tf.float32), [-1, self.action_dim]))
        # Prev rewards.
        if self.model_config["lstm_use_prev_reward"]:
            prev_a_r.append(
                tf.reshape(
                    tf.cast(input_dict[SampleBatch.PREV_REWARDS], tf.float32),
                    [-1, 1]))

        # Concat prev. actions + rewards to the "main" input.
        if prev_a_r:
            wrapped_out = tf.concat([wrapped_out] + prev_a_r, axis=1)

        # Push everything through our LSTM.
        input_dict["obs_flat"] = wrapped_out
        return super().forward(input_dict, state, seq_lens)

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs: TensorType, state: List[TensorType],
                    seq_lens: TensorType) -> (TensorType, List[TensorType]):
        model_out, self._value_out, h, c = self._rnn_model([inputs, seq_lens] +
                                                           state)
        return model_out, [h, c]

    @override(ModelV2)
    def get_initial_state(self) -> List[np.ndarray]:
        return [
            np.zeros(self.cell_size, np.float32),
            np.zeros(self.cell_size, np.float32),
        ]

    @override(ModelV2)
    def value_function(self) -> TensorType:
        return tf.reshape(self._value_out, [-1])


class Keras_LSTMWrapper(tf.keras.Model if tf else object):
    """A tf keras auto-LSTM wrapper used when `use_lstm`=True."""

    def __init__(
            self,
            input_space: gym.spaces.Space,
            action_space: gym.spaces.Space,
            num_outputs: Optional[int] = None,
            *,
            name: str,
            wrapped_cls: Type["tf.keras.Model"],
            max_seq_len: int = 20,
            lstm_cell_size: int = 256,
            lstm_use_prev_action: bool = False,
            lstm_use_prev_reward: bool = False,
            **kwargs,
    ):

        super().__init__(name=name)
        self.wrapped_keras_model = wrapped_cls(
            input_space, action_space, None, name="wrapped_" + name, **kwargs)

        self.action_space = action_space
        self.max_seq_len = max_seq_len

        # Guess the number of outputs for the wrapped model by looking
        # at its first output's shape.
        # This will be the input size for the LSTM layer (plus
        # maybe prev-actions/rewards).
        # If no layers in the wrapped model, set it to the
        # observation space.
        if self.wrapped_keras_model.layers:
            assert self.wrapped_keras_model.layers[-1].outputs
            assert len(
                self.wrapped_keras_model.layers[-1].outputs[0].shape) == 2
            wrapped_num_outputs = int(
                self.wrapped_keras_model.layers[-1].outputs[0].shape[1])
        else:
            wrapped_num_outputs = int(np.product(self.obs_space.shape))

        self.lstm_cell_size = lstm_cell_size
        self.lstm_use_prev_action = lstm_use_prev_action
        self.lstm_use_prev_reward = lstm_use_prev_reward

        if isinstance(self.action_space, Discrete):
            self.action_dim = self.action_space.n
        elif isinstance(self.action_space, MultiDiscrete):
            self.action_dim = np.sum(self.action_space.nvec)
        elif self.action_space.shape is not None:
            self.action_dim = int(np.product(self.action_space.shape))
        else:
            self.action_dim = int(len(self.action_space))

        # Add prev-action/reward nodes to input to LSTM.
        if self.lstm_use_prev_action:
            wrapped_num_outputs += self.action_dim
        if self.lstm_use_prev_reward:
            wrapped_num_outputs += 1

        # Define input layers.
        input_layer = tf.keras.layers.Input(
            shape=(None, wrapped_num_outputs), name="inputs")

        state_in_h = tf.keras.layers.Input(
            shape=(self.lstm_cell_size, ), name="h")
        state_in_c = tf.keras.layers.Input(
            shape=(self.lstm_cell_size, ), name="c")
        seq_in = tf.keras.layers.Input(shape=(), name="seq_in", dtype=tf.int32)

        # Preprocess observation with a hidden layer and send to LSTM cell
        lstm_out, state_h, state_c = tf.keras.layers.LSTM(
            self.lstm_cell_size,
            return_sequences=True,
            return_state=True,
            name="lstm")(
                inputs=input_layer,
                mask=tf.sequence_mask(seq_in),
                initial_state=[state_in_h, state_in_c])

        # Postprocess LSTM output with another hidden layer
        # if num_outputs not None.
        if num_outputs:
            logits = tf.keras.layers.Dense(
                num_outputs,
                activation=tf.keras.activations.linear,
                name="logits")(lstm_out)
        else:
            logits = lstm_out
        # Compute values.
        values = tf.keras.layers.Dense(
            1, activation=None, name="values")(lstm_out)

        # Create the RNN model
        self._rnn_model = tf.keras.Model(
            inputs=[input_layer, seq_in, state_in_h, state_in_c],
            outputs=[logits, values, state_h, state_c])

        # Use view-requirements of wrapped model and add own
        # requirements.
        self.view_requirements = \
            getattr(self.wrapped_keras_model, "view_requirements", {
                SampleBatch.OBS: ViewRequirement(space=input_space)
            })

        # Add prev-a/r to this model's view, if required.
        if self.lstm_use_prev_action:
            self.view_requirements[SampleBatch.PREV_ACTIONS] = \
                ViewRequirement(SampleBatch.ACTIONS, space=self.action_space,
                                shift=-1)
        if self.lstm_use_prev_reward:
            self.view_requirements[SampleBatch.PREV_REWARDS] = \
                ViewRequirement(SampleBatch.REWARDS, shift=-1)

        # Internal states view requirements.
        for i in range(2):
            space = Box(-1.0, 1.0, shape=(self.lstm_cell_size, ))
            self.view_requirements["state_in_{}".format(i)] = \
                ViewRequirement(
                    "state_out_{}".format(i),
                    shift=-1,
                    used_for_compute_actions=True,
                    batch_repeat_value=max_seq_len,
                    space=space)
            self.view_requirements["state_out_{}".format(i)] = \
                ViewRequirement(space=space, used_for_training=True)

    def call(self, input_dict: SampleBatch) -> \
            (TensorType, List[TensorType], Dict[str, TensorType]):
        assert input_dict.get(SampleBatch.SEQ_LENS) is not None
        # Push obs through underlying (wrapped) model first.
        wrapped_out, _, _ = self.wrapped_keras_model(input_dict)

        # Concat. prev-action/reward if required.
        prev_a_r = []
        if self.lstm_use_prev_action:
            prev_a = input_dict[SampleBatch.PREV_ACTIONS]
            if isinstance(self.action_space, (Discrete, MultiDiscrete)):
                prev_a = one_hot(prev_a, self.action_space)
            prev_a_r.append(
                tf.reshape(tf.cast(prev_a, tf.float32), [-1, self.action_dim]))
        if self.lstm_use_prev_reward:
            prev_a_r.append(
                tf.reshape(
                    tf.cast(input_dict[SampleBatch.PREV_REWARDS], tf.float32),
                    [-1, 1]))

        if prev_a_r:
            wrapped_out = tf.concat([wrapped_out] + prev_a_r, axis=1)

        max_seq_len = tf.shape(wrapped_out)[0] // tf.shape(
            input_dict[SampleBatch.SEQ_LENS])[0]
        wrapped_out_plus_time_dim = add_time_dimension(
            wrapped_out, max_seq_len=max_seq_len, framework="tf")
        model_out, value_out, h, c = self._rnn_model([
            wrapped_out_plus_time_dim, input_dict[SampleBatch.SEQ_LENS],
            input_dict["state_in_0"], input_dict["state_in_1"]
        ])
        model_out_no_time_dim = tf.reshape(
            model_out, tf.concat([[-1], tf.shape(model_out)[2:]], axis=0))
        return model_out_no_time_dim, [h, c], {
            SampleBatch.VF_PREDS: tf.reshape(value_out, [-1])
        }
