from typing import Any

import numpy as np

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class LSTMContainingRLModule(TorchRLModule):
    """An example TorchRLModule that contains an LSTM layer.

    .. testcode::

        import numpy as np
        import gymnasium as gym
        from ray.rllib.core.rl_module.rl_module import RLModuleConfig

        B = 10  # batch size
        T = 5  # seq len
        f = 25  # feature dim
        CELL = 32  # LSTM cell size

        # Construct the RLModule.
        rl_module_config = RLModuleConfig(
            observation_space=gym.spaces.Box(-1.0, 1.0, (f,), np.float32),
            action_space=gym.spaces.Discrete(4),
            model_config_dict={"lstm_cell_size": CELL}
        )
        my_net = LSTMContainingRLModule(rl_module_config)

        # Create some dummy input.
        obs = torch.from_numpy(
            np.random.random_sample(size=(B, T, f)
        ).astype(np.float32))
        state_in = my_net.get_initial_state()
        # Repeat state_in across batch.
        state_in = tree.map_structure(
            lambda s: torch.from_numpy(s).unsqueeze(0).repeat(B, 1), state_in
        )
        input_dict = {
            Columns.OBS: obs,
            Columns.STATE_IN: state_in,
        }

        # Run through all 3 forward passes.
        print(my_net.forward_inference(input_dict))
        print(my_net.forward_exploration(input_dict))
        print(my_net.forward_train(input_dict))

        # Print out the number of parameters.
        num_all_params = sum(int(np.prod(p.size())) for p in my_net.parameters())
        print(f"num params = {num_all_params}")
    """

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
        # Assume a simple Box(1D) tensor as input shape.
        in_size = self.config.observation_space.shape[0]

        # Get the LSTM cell size from our RLModuleConfig's (self.config)
        # `model_config_dict` property:
        self._lstm_cell_size = self.config.model_config_dict.get("lstm_cell_size", 256)
        self._lstm = nn.LSTM(in_size, self._lstm_cell_size, batch_first=False)
        in_size = self._lstm_cell_size

        # Build a sequential stack.
        layers = []
        # Get the dense layer pre-stack configuration from the same config dict.
        dense_layers = self.config.model_config_dict.get("dense_layers", [128, 128])
        for out_size in dense_layers:
            # Dense layer.
            layers.append(nn.Linear(in_size, out_size))
            # ReLU activation.
            layers.append(nn.ReLU())
            in_size = out_size

        self._fc_net = nn.Sequential(*layers)

        # Logits layer (no bias, no activation).
        self._logits = nn.Linear(in_size, self.config.action_space.n)
        # Single-node value layer.
        self._values = nn.Linear(in_size, 1)

    @override(TorchRLModule)
    def get_initial_state(self) -> Any:
        return {
            "h": np.zeros(shape=(self._lstm_cell_size,), dtype=np.float32),
            "c": np.zeros(shape=(self._lstm_cell_size,), dtype=np.float32),
        }

    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        _, state_out, logits = self._compute_features_state_out_and_logits(batch)

        # Return logits as ACTION_DIST_INPUTS (categorical distribution).
        # Note that the default `GetActions` connector piece (in the EnvRunner) will
        # take care of argmax-"sampling" from the logits to yield the inference (greedy)
        # action.
        return {
            Columns.STATE_OUT: state_out,
            Columns.ACTION_DIST_INPUTS: logits,
        }

    @override(TorchRLModule)
    def _forward_exploration(self, batch, **kwargs):
        # Exact same as `_forward_inference`.
        # Note that the default `GetActions` connector piece (in the EnvRunner) will
        # take care of stochastic sampling from the Categorical defined by the logits
        # to yield the exploration action.
        return self._forward_inference(batch, **kwargs)

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        features, state_out, logits = self._compute_features_state_out_and_logits(batch)
        # Besides the action logits, we also have to return value predictions here
        # (to be used inside the loss function).
        values = self._values(features).squeeze(-1)
        return {
            Columns.STATE_OUT: state_out,
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
    def _compute_values(self, batch):
        obs = batch[Columns.OBS]
        state_in = batch[Columns.STATE_IN]
        h, c = state_in["h"], state_in["c"]
        # Unsqueeze the layer dim (we only have 1 LSTM layer.
        features, _ = self._lstm(
            obs.permute(1, 0, 2),  # we have to permute, b/c our LSTM is time-major
            (h.unsqueeze(0), c.unsqueeze(0)),
        )
        # Make batch-major again.
        features = features.permute(1, 0, 2)
        # Push through our FC net.
        features = self._fc_net(features)
        return self._values(features).squeeze(-1)

    def _compute_features_state_out_and_logits(self, batch):
        obs = batch[Columns.OBS]
        state_in = batch[Columns.STATE_IN]
        h, c = state_in["h"], state_in["c"]
        # Unsqueeze the layer dim (we only have 1 LSTM layer.
        features, (h, c) = self._lstm(
            obs.permute(1, 0, 2),  # we have to permute, b/c our LSTM is time-major
            (h.unsqueeze(0), c.unsqueeze(0)),
        )
        # Make batch-major again.
        features = features.permute(1, 0, 2)
        # Push through our FC net.
        features = self._fc_net(features)
        logits = self._logits(features)
        return features, {"h": h.squeeze(0), "c": c.squeeze(0)}, logits
