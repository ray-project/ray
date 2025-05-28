from typing import Any, Dict, Optional

import numpy as np

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class LSTMContainingRLModule(TorchRLModule, ValueFunctionAPI):
    """An example TorchRLModule that contains an LSTM layer.

    .. testcode::

        import numpy as np
        import gymnasium as gym

        B = 10  # batch size
        T = 5  # seq len
        e = 25  # embedding dim
        CELL = 32  # LSTM cell size

        # Construct the RLModule.
        my_net = LSTMContainingRLModule(
            observation_space=gym.spaces.Box(-1.0, 1.0, (e,), np.float32),
            action_space=gym.spaces.Discrete(4),
            model_config={"lstm_cell_size": CELL}
        )

        # Create some dummy input.
        obs = torch.from_numpy(
            np.random.random_sample(size=(B, T, e)
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
        - `self.model_config`: The config dict for this RLModule class,
        which should contain flxeible settings, for example: {"hiddens": [256, 256]}.
        - `self.observation|action_space`: The observation and action space that
        this RLModule is subject to. Note that the observation space might not be the
        exact space from your env, but that it might have already gone through
        preprocessing through a connector pipeline (for example, flattening,
        frame-stacking, mean/std-filtering, etc..).
        """
        # Assume a simple Box(1D) tensor as input shape.
        in_size = self.observation_space.shape[0]

        # Get the LSTM cell size from the `model_config` attribute:
        self._lstm_cell_size = self.model_config.get("lstm_cell_size", 256)
        self._lstm = nn.LSTM(in_size, self._lstm_cell_size, batch_first=True)
        in_size = self._lstm_cell_size

        # Build a sequential stack.
        layers = []
        # Get the dense layer pre-stack configuration from the same config dict.
        dense_layers = self.model_config.get("dense_layers", [128, 128])
        for out_size in dense_layers:
            # Dense layer.
            layers.append(nn.Linear(in_size, out_size))
            # ReLU activation.
            layers.append(nn.ReLU())
            in_size = out_size

        self._fc_net = nn.Sequential(*layers)

        # Logits layer (no bias, no activation).
        self._pi_head = nn.Linear(in_size, self.action_space.n)
        # Single-node value layer.
        self._values = nn.Linear(in_size, 1)

    @override(TorchRLModule)
    def get_initial_state(self) -> Any:
        return {
            "h": np.zeros(shape=(self._lstm_cell_size,), dtype=np.float32),
            "c": np.zeros(shape=(self._lstm_cell_size,), dtype=np.float32),
        }

    @override(TorchRLModule)
    def _forward(self, batch, **kwargs):
        # Compute the basic 1D embedding tensor (inputs to policy- and value-heads).
        embeddings, state_outs = self._compute_embeddings_and_state_outs(batch)
        logits = self._pi_head(embeddings)

        # Return logits as ACTION_DIST_INPUTS (categorical distribution).
        # Note that the default `GetActions` connector piece (in the EnvRunner) will
        # take care of argmax-"sampling" from the logits to yield the inference (greedy)
        # action.
        return {
            Columns.ACTION_DIST_INPUTS: logits,
            Columns.STATE_OUT: state_outs,
        }

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        # Same logic as _forward, but also return embeddings to be used by value
        # function branch during training.
        embeddings, state_outs = self._compute_embeddings_and_state_outs(batch)
        logits = self._pi_head(embeddings)
        return {
            Columns.ACTION_DIST_INPUTS: logits,
            Columns.STATE_OUT: state_outs,
            Columns.EMBEDDINGS: embeddings,
        }

    # We implement this RLModule as a ValueFunctionAPI RLModule, so it can be used
    # by value-based methods like PPO or IMPALA.
    @override(ValueFunctionAPI)
    def compute_values(
        self, batch: Dict[str, Any], embeddings: Optional[Any] = None
    ) -> TensorType:
        if embeddings is None:
            embeddings, _ = self._compute_embeddings_and_state_outs(batch)
        values = self._values(embeddings).squeeze(-1)
        return values

    def _compute_embeddings_and_state_outs(self, batch):
        obs = batch[Columns.OBS]
        state_in = batch[Columns.STATE_IN]
        h, c = state_in["h"], state_in["c"]
        # Unsqueeze the layer dim (we only have 1 LSTM layer).
        embeddings, (h, c) = self._lstm(obs, (h.unsqueeze(0), c.unsqueeze(0)))
        # Push through our FC net.
        embeddings = self._fc_net(embeddings)
        # Squeeze the layer dim (we only have 1 LSTM layer).
        return embeddings, {"h": h.squeeze(0), "c": c.squeeze(0)}
