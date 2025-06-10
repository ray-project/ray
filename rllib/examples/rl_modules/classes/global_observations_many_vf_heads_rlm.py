from typing import Dict, Any, Optional, List

import torch

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI, ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.torch_distributions import TorchMultiCategorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType


class GlobalObsManyVFHeadsRLModule(TorchRLModule, InferenceOnlyAPI, ValueFunctionAPI):
    """Model taking global obs for multiple agents and with many value heads.

    The input is the global observation, valid for all agents. The output is:
    - Parameters for a single, merged action distribution, for example a MultiDiscrete.
    - n value heads for the individual value functions of the n agents.
    """

    @override(TorchRLModule)
    def setup(self):
        """Use this method to create all the model components that you require.

        Feel free to access the following useful properties in this class:
        - `self.model_config`: The config dict for this RLModule class,
        which should contain flexible settings, for example: {"hiddens": [256, 256]}.
        - `self.observation|action_space`: The observation and action space that
        this RLModule is subject to. Note that the observation space might not be the
        exact space from your env, but that it might have already gone through
        preprocessing through a connector pipeline (for example, flattening,
        frame-stacking, mean/std-filtering, etc..).
        - `self.inference_only`: If True, this model should be built only for inference
        purposes, in which case you may want to exclude any components that are not used
        for computing actions, for example a value function branch.
        """
        num_agents = self.action_space.nvec.shape[0]

        input_dim = self.observation_space.shape[0]
        hidden_dims = self.model_config["hidden_dims"]
        # Note that the env needs to publish its `action_space` to be already the merger
        # of the individual agents' action spaces, for example a MultiDiscrete([...]) in
        # case of n individual Discrete action spaces.
        output_dim = self.get_inference_action_dist_cls().required_input_dim(self.action_space)

        layers = []
        current_in = input_dim
        for hid in hidden_dims:
            layers.append(torch.nn.Linear(current_in, hid))
            layers.append(torch.nn.ReLU())
            current_in = hid

        self._encoder = torch.nn.Sequential(*layers)

        # Single pi-head (multi-discrete).
        self._pi_head = torch.nn.Linear(current_in, output_dim)
        # n value heads.
        self._vf_heads = [
            torch.nn.Sequential(
                torch.nn.Linear(current_in, 256),
                torch.nn.ReLU(),
                torch.nn.Linear(256, 1),
            ) for _ in range(num_agents)
        ]

    def _forward(self, batch, **kwargs):
        embeddings = self._encoder(batch[Columns.OBS])
        action_logits = self._pi_head(embeddings)
        return {
            Columns.ACTION_DIST_INPUTS: action_logits,
            Columns.EMBEDDINGS: embeddings,
        }

        # If you need more granularity between the different forward behaviors during
        # the different phases of the module's lifecycle, implement three different
        # forward methods. Thereby, it is recommended to put the inference and
        # exploration versions inside a `with torch.no_grad()` context for better
        # performance.
        # def _forward_train(self, batch):
        #    ...
        #
        # def _forward_inference(self, batch):
        #    with torch.no_grad():
        #        return self._forward_train(batch)
        #
        # def _forward_exploration(self, batch):
        #    with torch.no_grad():
        #        return self._forward_train(batch)

    @override(ValueFunctionAPI)
    def compute_values(
        self,
        batch: Dict[str, Any],
        embeddings: Optional[Any] = None,
    ) -> TensorType:
        # Compute embeddings, if not provided by user.
        if embeddings is None:
            embeddings = self._encoder(batch[Columns.OBS])

        # Compute all agents' values.
        return torch.stack(
            [vf_head(embeddings).squeeze(-1) for vf_head in self._vf_heads],
            dim=-1,
        )

    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        return ["_vf_heads"]
