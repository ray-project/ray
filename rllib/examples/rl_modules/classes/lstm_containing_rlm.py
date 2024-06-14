from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.models.torch.misc import normc_initializer
from ray.rllib.models.torch.misc import same_padding, valid_padding
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, nn = try_import_torch()


class LSTMContainingRLModule(TorchRLModule):
    """An example TorchRLModule that contains an LSTM layer."""

    @override(TorchRLModule)
    def setup(self):
        """Use this method to create all the model components that you require.

        Feel free to access the following useful properties in this class:
        - `self.config.model_config_dict`: The config dict for this RLModule class,
        which should contain flxeible settings, for example: {"hiddens": [256, 256]}.
        - `self.config.observation|action_space`: The observation and action space that
        this RLModule is subject to. Note that the observation space might not be the
        exact space from your env, but that it might have already gone through
        preprocessing through a connector pipeline (for example, flattening,
        frame-stacking, mean/std-filtering, etc..).
        """
        # Get the LSTM cell size from our RLModuleConfig's (self.config)
        # `model_config_dict` property:
        cell_size = self.config.model_config_dict.get("lstm_cell_size", 256)
        # Get the dense layer pre-stack configuration from the same config dict.
        dense_layers = self.config.model_config_dict.get("dense_layers", [128, 128])

        # Build a Dense stack.
        layers = []
        # Assume a simple Box(1D) tensor as input shape.
        in_size = self.config.observation_space.shape[0]
        for out_size in dense_layers:
            # Dense layer.
            layers.append(nn.Linear(in_size, out_size))
            # Activation.
            layers.append(nn.ReLU())
            in_size = out_size

        self._dense_stack = nn.Sequential(*layers)

        # Add the LSTM layer.
        self._lstm = nn.LSTM()

        self._logits = nn.Sequential(
            nn.ZeroPad2d(same_padding(in_size, 1, 1)[0]),
            nn.Conv2d(in_depth, self.config.action_space.n, 1, 1, bias=True),
        )
        self._values = nn.Linear(in_depth, 1)

    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        _, logits = self._compute_features_and_logits(batch)
        # Return logits as ACTION_DIST_INPUTS (categorical distribution).
        return {Columns.ACTION_DIST_INPUTS: logits}

    @override(TorchRLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch, **kwargs)

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        features, logits = self._compute_features_and_logits(batch)
        # Besides the action logits, we also have to return value predictions here
        # (to be used inside the loss function).
        values = self._values(features).squeeze(-1)
        return {
            Columns.ACTION_DIST_INPUTS: logits,
            Columns.VF_PREDS: values,
        }

    # TODO (sven): We still need to define the distibution to use here, even though,
    #  we have a pretty standard action space (Discrete), which should simply always map
    #  to a categorical dist. by default.
    @override(TorchRLModule)
    def get_inference_action_dist_cls(self):
        return TorchCategorical

    @override(TorchRLModule)
    def get_exploration_action_dist_cls(self):
        return TorchCategorical

    @override(TorchRLModule)
    def get_train_action_dist_cls(self):
        return TorchCategorical

    # TODO (sven): In order for this RLModule to work with PPO, we must define
    #  our own `_compute_values()` method. This would become more obvious, if we simply
    #  subclassed the `PPOTorchRLModule` directly here (which we didn't do for
    #  simplicity and to keep some generality). We might change even get rid of algo-
    #  specific RLModule subclasses altogether in the future and replace them
    #  by mere algo-specific APIs (w/o any actual implementations).
    def _compute_values(self, batch, device):
        obs = convert_to_torch_tensor(batch[Columns.OBS], device=device)
        features = self._base_cnn_stack(obs.permute(0, 3, 1, 2))
        features = torch.squeeze(features, dim=[-1, -2])
        return self._values(features).squeeze(-1)

    def _compute_features_and_logits(self, batch):
        obs = batch[Columns.OBS].permute(0, 3, 1, 2)
        features = self._base_cnn_stack(obs)
        logits = self._logits(features)
        return (
            torch.squeeze(features, dim=[-1, -2]),
            torch.squeeze(logits, dim=[-1, -2]),
        )


if __name__ == "__main__":
    import numpy as np
    import gymnasium as gym
    from ray.rllib.core.rl_module.rl_module import RLModuleConfig

    rl_module_config = RLModuleConfig(
        observation_space=gym.spaces.Box(-1.0, 1.0, (42, 42, 4), np.float32),
        action_space=gym.spaces.Discrete(4),
    )
    my_net = TinyAtariCNN(rl_module_config)

    B = 10
    w = 42
    h = 42
    c = 4
    data = torch.from_numpy(
        np.random.random_sample(size=(B, w, h, c)).astype(np.float32)
    )
    print(my_net.forward_inference({"obs": data}))
    print(my_net.forward_exploration({"obs": data}))
    print(my_net.forward_train({"obs": data}))

    num_all_params = sum(int(np.prod(p.size())) for p in my_net.parameters())
    print(f"num params = {num_all_params}")
