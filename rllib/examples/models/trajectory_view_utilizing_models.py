from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

# __sphinx_doc_begin__


class FrameStackingCartPoleModel(TFModelV2):
    """A simple FC model that takes the last n observations as input."""

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 num_frames=3):
        super(FrameStackingCartPoleModel, self).__init__(
            obs_space, action_space, None, model_config, name)

        self.num_frames = num_frames
        self.num_outputs = num_outputs

        # Construct actual (very simple) FC model.
        assert len(obs_space.shape) == 1
        input_ = tf.keras.layers.Input(
            shape=(self.num_frames, obs_space.shape[0]))
        reshaped = tf.keras.layers.Reshape(
            [obs_space.shape[0] * self.num_frames])(input_)
        layer1 = tf.keras.layers.Dense(64, activation=tf.nn.relu)(reshaped)
        out = tf.keras.layers.Dense(self.num_outputs)(layer1)
        values = tf.keras.layers.Dense(1)(layer1)
        self.base_model = tf.keras.models.Model([input_], [out, values])

        self._last_value = None

        self.view_requirements["prev_n_obs"] = ViewRequirement(
            data_col="obs",
            shift="-{}:0".format(num_frames - 1),
            space=obs_space)
        self.view_requirements["prev_rewards"] = ViewRequirement(
            data_col="rewards", shift=-1)

    def forward(self, input_dict, states, seq_lens):
        obs = input_dict["prev_n_obs"]
        out, self._last_value = self.base_model(obs)
        return out, []

    def value_function(self):
        return tf.squeeze(self._last_value, -1)


# __sphinx_doc_end__


class TorchFrameStackingCartPoleModel(TorchModelV2, nn.Module):
    """A simple FC model that takes the last n observations as input."""

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 num_frames=3):
        nn.Module.__init__(self)
        super(TorchFrameStackingCartPoleModel, self).__init__(
            obs_space, action_space, None, model_config, name)

        self.num_frames = num_frames
        self.num_outputs = num_outputs

        # Construct actual (very simple) FC model.
        assert len(obs_space.shape) == 1
        self.layer1 = SlimFC(
            in_size=obs_space.shape[0] * self.num_frames,
            out_size=64,
            activation_fn="relu")
        self.out = SlimFC(
            in_size=64, out_size=self.num_outputs, activation_fn="linear")
        self.values = SlimFC(in_size=64, out_size=1, activation_fn="linear")

        self._last_value = None

        self.view_requirements["prev_n_obs"] = ViewRequirement(
            data_col="obs",
            shift="-{}:0".format(num_frames - 1),
            space=obs_space)
        self.view_requirements["prev_rewards"] = ViewRequirement(
            data_col="rewards", shift=-1)

    def forward(self, input_dict, states, seq_lens):
        obs = input_dict["prev_n_obs"]
        obs = torch.reshape(obs,
                            [-1, self.obs_space.shape[0] * self.num_frames])
        features = self.layer1(obs)
        out = self.out(features)
        self._last_value = self.values(features)
        return out, []

    def value_function(self):
        return torch.squeeze(self._last_value, -1)
