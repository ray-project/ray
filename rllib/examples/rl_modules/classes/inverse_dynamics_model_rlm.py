from typing import Any, Dict

import numpy as np

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class InverseDynamicsModel(TorchRLModule):
    """An inverse-dynamics model as TorchRLModule for curiosity-based exploration.

    For more details, see:
    [1] Curiosity-driven Exploration by Self-supervised Prediction
    Pathak, Agrawal, Efros, and Darrell - UC Berkeley - ICML 2017.
    https://arxiv.org/pdf/1705.05363.pdf

    Learns a simplified model of the environment based on three networks:
    1) Embedding observations into latent space ("feature" network).
    2) Predicting the action, given two consecutive embedded observations
    ("inverse" network).
    3) Predicting the next embedded obs, given an obs and action
    ("forward" network).

    The less the agent is able to predict the actually observed next feature
    vector, given obs and action (through the forwards network), the larger the
    "intrinsic reward", which will be added to the extrinsic reward.
    Therefore, if a state transition was unexpected, the agent becomes
    "curious" and will further explore this transition leading to better
    exploration in sparse rewards environments.

    .. testcode::

        import numpy as np
        import gymnasium as gym
        from ray.rllib.core.rl_module.rl_module import RLModuleConfig

        B = 10  # batch size
        T = 5  # seq len
        f = 25  # feature dim

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
        # Assume a simple Box(1D) tensor as input shape.
        in_size = self.config.observation_space.shape[0]

        # Get the IDM achitecture settings from the our RLModuleConfig's (self.config)
        # `model_config_dict` property:
        cfg = self.config.model_config_dict
        self._feature_dim = cfg.get("feature_dim", 288)
        feature_net_config: Optional[ModelConfigDict] = None,
        self._inverse_net_hiddens = cfg.get("inverse_net_hiddens", (256,))
        self._inverse_net_activation = cfg.get("inverse_net_activation", "relu")
        self._forward_net_hiddens = cfg.get("forward_net_hiddens", (256,))
        self._forward_net_activation = cfg.get("forward_net_activation", "relu")


        # Build the inverse model (predicting .
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

    @override(TorchRLModule)
    def get_train_action_dist_cls(self):
        return TorchCategorical

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

    # Inference and exploration not supported (this is a world-model that should only
    # be used for training).
    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        raise NotImplementedError(
            "InverseDynamicsModel should only be used for training! "
            "Use `forward_train()` instead."
        )

    @override(TorchRLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch)
