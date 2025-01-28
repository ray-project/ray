import torch
import torch.nn as nn
import gymnasium as gym
from typing import Any, Dict, Optional
from ray.rllib.core.columns import Columns
from ray.rllib.utils.annotations import override
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule


class MOGTorchRLModule(TorchRLModule, ValueFunctionAPI):
    """
    Custom rl_module that demonstrates setting up, defining the necessary _forward methods,
    and overriding the ValueFunctionAPI to compute values using mixture of gaussian components.

    This also uses a custom_config from the module_to_load_spec to get the fcnet_hiddens
    as well as custom arg num_mog_components
    """

    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        inference_only: bool = False,
        learner_only: bool = False,
        model_config: Optional[dict] = None,
        catalog_class: Optional[type] = None,
    ) -> None:
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            inference_only=inference_only,
            learner_only=learner_only,
            model_config=model_config,
        )
        # Add mixture of gaussian components to the Columns class
        Columns.NEXT_MOG_COMPONENTS = "next_mog_components"
        Columns.MOG_COMPONENTS = "mog_components"
        Columns.VF_OUT = "vf_out"
        self.inference_only = False

    def setup(self):
        input_dim = self.observation_space.shape[0]
        hidden_dim = self.model_config["fcnet_hiddens"][0]

        # Set failsafe for action space dim
        if isinstance(self.action_space, gym.spaces.Box):
            output_dim = self.action_space.shape[0] * 2
        elif isinstance(self.action_space, gym.spaces.Discrete):
            output_dim = self.action_space.n
        else:
            raise ValueError(
                f"Unsupported action space type: {type(self.action_space)}"
            )

        self.num_gaussians = self.model_config.get(
            "num_mog_components", 1
        )  # Create a base scalar as a value (normal critic)

        self.policy = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LeakyReLU(),
            nn.Linear(hidden_dim, output_dim),
            nn.Tanh(),
        )

        self.mog_critic = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LeakyReLU(),
            nn.Linear(hidden_dim, self.num_gaussians * 3),
        )

    @override(RLModule)
    def _forward_inference(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    @override(RLModule)
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    @override(ValueFunctionAPI)
    def compute_values(self, batch: Dict[str, Any], embeddings: Optional[Any] = None):
        obs = batch["obs"]
        mog_output = self._compute_mog_components(obs)
        # The value of the current state is simply the means * softmax(alphas)
        return torch.sum(
            mog_output["means"]
            * torch.clamp(
                nn.functional.softmax(mog_output["alphas"], dim=-1), 1e-6, None
            ),
            dim=-1,
        )

    def _compute_mog_components(self, obs: torch.Tensor) -> Dict[str, torch.Tensor]:
        """
        Splits the components given back from the value network in the order of
        means, sigmas, and alphas.
        Also, processes the sigmas for further use by passing them through a softplus activation function.
        In this example, the sigmas are not used but this is in further development in terms of model validation.
        Where one could estimate uncertainty and guide agents toward picking safer/better actions.

        Args:
            obs (observations) : torch.Tensor
        Returns:
            Dict(str: torch.Tensor) of:
            means: num_gaussians worth of means specified by the user
            sigmas: num_gaussians worth of sigmas specified by the user
            alphas: num_gaussians worth of alphas specified by the user
        """
        # Get the total output from the critic network which is num_gaussians * 3
        mog_output = self.mog_critic(obs)
        # Slice means, sigmas, and alphas respectively and process them
        means = mog_output[:, : self.num_gaussians]
        sigmas_prev = mog_output[:, self.num_gaussians : self.num_gaussians * 2]
        sigmas = nn.functional.softplus(sigmas_prev) + 1e-6
        # Do not run alphas through softmax yet since in the loss function it uses the
        # log_softmax of the current alphas.
        alphas = mog_output[:, self.num_gaussians * 2 :]
        return {
            "means": means,
            "sigmas": sigmas,
            "alphas": alphas,
        }

    @override(RLModule)
    def _forward_train(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        if self.inference_only:
            raise RuntimeError(
                "Trying to train a module that is not a learner module. Set the "
                "flag `inference_only=False` when building the module."
            )
        obs = batch[Columns.OBS]
        if "new_obs" not in batch:
            next_obs = obs
        else:
            next_obs = batch[Columns.NEXT_OBS]
        action_logits = self.policy(obs)
        value_function_out = self.compute_values(batch)

        return {
            Columns.ACTION_DIST_INPUTS: action_logits,
            Columns.VF_OUT: value_function_out,
            Columns.MOG_COMPONENTS: self._compute_mog_components(obs),
            Columns.NEXT_MOG_COMPONENTS: self._compute_mog_components(next_obs),
        }
