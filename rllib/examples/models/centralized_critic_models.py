from gym.spaces import Box

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFC
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class CentralizedCriticModel(TFModelV2):
    """Multi-agent model that implements a centralized value function."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(CentralizedCriticModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)
        # Base of the model
        self.model = FullyConnectedNetwork(obs_space, action_space,
                                           num_outputs, model_config, name)

        # Central VF maps (obs, opp_obs, opp_act) -> vf_pred
        obs = tf.keras.layers.Input(shape=(6, ), name="obs")
        opp_obs = tf.keras.layers.Input(shape=(6, ), name="opp_obs")
        opp_act = tf.keras.layers.Input(shape=(2, ), name="opp_act")
        concat_obs = tf.keras.layers.Concatenate(axis=1)(
            [obs, opp_obs, opp_act])
        central_vf_dense = tf.keras.layers.Dense(
            16, activation=tf.nn.tanh, name="c_vf_dense")(concat_obs)
        central_vf_out = tf.keras.layers.Dense(
            1, activation=None, name="c_vf_out")(central_vf_dense)
        self.central_vf = tf.keras.Model(
            inputs=[obs, opp_obs, opp_act], outputs=central_vf_out)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        return self.model.forward(input_dict, state, seq_lens)

    def central_value_function(self, obs, opponent_obs, opponent_actions):
        return tf.reshape(
            self.central_vf([
                obs, opponent_obs,
                tf.one_hot(tf.cast(opponent_actions, tf.int32), 2)
            ]), [-1])

    @override(ModelV2)
    def value_function(self):
        return self.model.value_function()  # not used


class YetAnotherCentralizedCriticModel(TFModelV2):
    """Multi-agent model that implements a centralized value function.

    It assumes the observation is a dict with 'own_obs' and 'opponent_obs', the
    former of which can be used for computing actions (i.e., decentralized
    execution), and the latter for optimization (i.e., centralized learning).

    This model has two parts:
    - An action model that looks at just 'own_obs' to compute actions
    - A value model that also looks at the 'opponent_obs' / 'opponent_action'
      to compute the value (it does this by using the 'obs_flat' tensor).
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(YetAnotherCentralizedCriticModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        self.action_model = FullyConnectedNetwork(
            Box(low=0, high=1, shape=(6, )),  # one-hot encoded Discrete(6)
            action_space,
            num_outputs,
            model_config,
            name + "_action")

        self.value_model = FullyConnectedNetwork(obs_space, action_space, 1,
                                                 model_config, name + "_vf")

    def forward(self, input_dict, state, seq_lens):
        self._value_out, _ = self.value_model({
            "obs": input_dict["obs_flat"]
        }, state, seq_lens)
        return self.action_model({
            "obs": input_dict["obs"]["own_obs"]
        }, state, seq_lens)

    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class TorchCentralizedCriticModel(TorchModelV2, nn.Module):
    """Multi-agent model that implements a centralized VF."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        # Base of the model
        self.model = TorchFC(obs_space, action_space, num_outputs,
                             model_config, name)

        # Central VF maps (obs, opp_obs, opp_act) -> vf_pred
        input_size = 6 + 6 + 2  # obs + opp_obs + opp_act
        self.central_vf = nn.Sequential(
            SlimFC(input_size, 16, activation_fn=nn.Tanh),
            SlimFC(16, 1),
        )

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        model_out, _ = self.model(input_dict, state, seq_lens)
        return model_out, []

    def central_value_function(self, obs, opponent_obs, opponent_actions):
        input_ = torch.cat([
            obs, opponent_obs,
            torch.nn.functional.one_hot(opponent_actions.long(), 2).float()
        ], 1)
        return torch.reshape(self.central_vf(input_), [-1])

    @override(ModelV2)
    def value_function(self):
        return self.model.value_function()  # not used


class YetAnotherTorchCentralizedCriticModel(TorchModelV2, nn.Module):
    """Multi-agent model that implements a centralized value function.

    It assumes the observation is a dict with 'own_obs' and 'opponent_obs', the
    former of which can be used for computing actions (i.e., decentralized
    execution), and the latter for optimization (i.e., centralized learning).

    This model has two parts:
    - An action model that looks at just 'own_obs' to compute actions
    - A value model that also looks at the 'opponent_obs' / 'opponent_action'
      to compute the value (it does this by using the 'obs_flat' tensor).
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        self.action_model = TorchFC(
            Box(low=0, high=1, shape=(6, )),  # one-hot encoded Discrete(6)
            action_space,
            num_outputs,
            model_config,
            name + "_action")

        self.value_model = TorchFC(obs_space, action_space, 1, model_config,
                                   name + "_vf")
        self._model_in = None

    def forward(self, input_dict, state, seq_lens):
        # Store model-input for possible `value_function()` call.
        self._model_in = [input_dict["obs_flat"], state, seq_lens]
        return self.action_model({
            "obs": input_dict["obs"]["own_obs"]
        }, state, seq_lens)

    def value_function(self):
        value_out, _ = self.value_model({
            "obs": self._model_in[0]
        }, self._model_in[1], self._model_in[2])
        return torch.reshape(value_out, [-1])
