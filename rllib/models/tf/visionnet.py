from typing import Dict, List
import gym

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.utils import get_activation_fn, get_filter_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()


class VisionNetwork(TFModelV2):
    """Generic vision network implemented in ModelV2 API."""

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):
        if not model_config.get("conv_filters"):
            model_config["conv_filters"] = get_filter_config(obs_space.shape)

        super(VisionNetwork, self).__init__(obs_space, action_space,
                                            num_outputs, model_config, name)

        activation = get_activation_fn(
            self.model_config.get("conv_activation"), framework="tf")
        filters = self.model_config["conv_filters"]
        assert len(filters) > 0,\
            "Must provide at least 1 entry in `conv_filters`!"
        no_final_linear = self.model_config.get("no_final_linear")
        vf_share_layers = self.model_config.get("vf_share_layers")
        self.traj_view_framestacking = False

        # Perform Atari framestacking via traj. view API.
        if model_config.get("num_framestacks") != "auto" and \
                model_config.get("num_framestacks", 0) > 1:
            input_shape = obs_space.shape + (model_config["num_framestacks"], )
            self.data_format = "channels_first"
            self.traj_view_framestacking = True
        else:
            input_shape = obs_space.shape
            self.data_format = "channels_last"

        inputs = tf.keras.layers.Input(shape=input_shape, name="observations")
        last_layer = inputs
        # Whether the last layer is the output of a Flattened (rather than
        # a n x (1,1) Conv2D).
        self.last_layer_is_flattened = False

        # Build the action layers
        for i, (out_size, kernel, stride) in enumerate(filters[:-1], 1):
            last_layer = tf.keras.layers.Conv2D(
                out_size,
                kernel,
                strides=(stride, stride),
                activation=activation,
                padding="same",
                data_format="channels_last",
                name="conv{}".format(i))(last_layer)

        out_size, kernel, stride = filters[-1]

        # No final linear: Last layer is a Conv2D and uses num_outputs.
        if no_final_linear and num_outputs:
            last_layer = tf.keras.layers.Conv2D(
                num_outputs,
                kernel,
                strides=(stride, stride),
                activation=activation,
                padding="valid",
                data_format="channels_last",
                name="conv_out")(last_layer)
            conv_out = last_layer
        # Finish network normally (w/o overriding last layer size with
        # `num_outputs`), then add another linear one of size `num_outputs`.
        else:
            last_layer = tf.keras.layers.Conv2D(
                out_size,
                kernel,
                strides=(stride, stride),
                activation=activation,
                padding="valid",
                data_format="channels_last",
                name="conv{}".format(len(filters)))(last_layer)

            # num_outputs defined. Use that to create an exact
            # `num_output`-sized (1,1)-Conv2D.
            if num_outputs:
                conv_out = tf.keras.layers.Conv2D(
                    num_outputs, [1, 1],
                    activation=None,
                    padding="same",
                    data_format="channels_last",
                    name="conv_out")(last_layer)

                if conv_out.shape[1] != 1 or conv_out.shape[2] != 1:
                    raise ValueError(
                        "Given `conv_filters` ({}) do not result in a [B, 1, "
                        "1, {} (`num_outputs`)] shape (but in {})! Please "
                        "adjust your Conv2D stack such that the dims 1 and 2 "
                        "are both 1.".format(self.model_config["conv_filters"],
                                             self.num_outputs,
                                             list(conv_out.shape)))

            # num_outputs not known -> Flatten, then set self.num_outputs
            # to the resulting number of nodes.
            else:
                self.last_layer_is_flattened = True
                conv_out = tf.keras.layers.Flatten(
                    data_format="channels_last")(last_layer)
                self.num_outputs = conv_out.shape[1]

        # Build the value layers
        if vf_share_layers:
            last_layer = tf.keras.layers.Lambda(
                lambda x: tf.squeeze(x, axis=[1, 2]))(last_layer)
            value_out = tf.keras.layers.Dense(
                1,
                name="value_out",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(last_layer)
        else:
            # build a parallel set of hidden layers for the value net
            last_layer = inputs
            for i, (out_size, kernel, stride) in enumerate(filters[:-1], 1):
                last_layer = tf.keras.layers.Conv2D(
                    out_size,
                    kernel,
                    strides=(stride, stride),
                    activation=activation,
                    padding="same",
                    data_format="channels_last",
                    name="conv_value_{}".format(i))(last_layer)
            out_size, kernel, stride = filters[-1]
            last_layer = tf.keras.layers.Conv2D(
                out_size,
                kernel,
                strides=(stride, stride),
                activation=activation,
                padding="valid",
                data_format="channels_last",
                name="conv_value_{}".format(len(filters)))(last_layer)
            last_layer = tf.keras.layers.Conv2D(
                1, [1, 1],
                activation=None,
                padding="same",
                data_format="channels_last",
                name="conv_value_out")(last_layer)
            value_out = tf.keras.layers.Lambda(
                lambda x: tf.squeeze(x, axis=[1, 2]))(last_layer)

        self.base_model = tf.keras.Model(inputs, [conv_out, value_out])

        # Optional: framestacking obs/new_obs for Atari.
        if self.traj_view_framestacking:
            from_ = model_config["num_framestacks"] - 1
            self.view_requirements[SampleBatch.OBS].shift = \
                "-{}:0".format(from_)
            self.view_requirements[SampleBatch.OBS].shift_from = -from_
            self.view_requirements[SampleBatch.OBS].shift_to = 0
            self.view_requirements[SampleBatch.NEXT_OBS] = ViewRequirement(
                data_col=SampleBatch.OBS,
                shift="-{}:1".format(from_ - 1),
                space=self.view_requirements[SampleBatch.OBS].space,
                used_for_compute_actions=False,
            )

    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        obs = input_dict["obs"]
        if self.data_format == "channels_first":
            obs = tf.transpose(obs, [0, 2, 3, 1])
        # Explicit cast to float32 needed in eager.
        model_out, self._value_out = self.base_model(tf.cast(obs, tf.float32))
        # Our last layer is already flat.
        if self.last_layer_is_flattened:
            return model_out, state
        # Last layer is a n x [1,1] Conv2D -> Flatten.
        else:
            return tf.squeeze(model_out, axis=[1, 2]), state

    def value_function(self) -> TensorType:
        return tf.reshape(self._value_out, [-1])
