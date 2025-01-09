import torch
import torch.nn as nn
import gymnasium as gym
from typing import Any, Dict, Optional
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.utils.annotations import override



class MOGModule(TorchRLModule, ValueFunctionAPI):
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
        
    def setup(self):
        input_dim = self.observation_space.shape[0]
        hidden_dim = self.model_config['fcnet_hiddens'][0]

        # set failsafe for action space dim
        if isinstance(self.action_space, gym.spaces.Box):
            output_dim = self.action_space.shape[0]  
        elif isinstance(self.action_space, gym.spaces.Discrete):
            output_dim = self.action_space.n 
        else:
            raise ValueError(f"Unsupported action space type: {type(self.action_space)}")

        self.num_gaussians = self.model_config.get('num_mixture_components', 1) # create a base scalar as a value (normal critic)

        self.policy = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LeakyReLU(),
            nn.Linear(hidden_dim, output_dim),
            nn.Tanh(),
        )

        self.mog_critic = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LeakyReLU(),
            nn.Linear(hidden_dim, self.num_gaussians*3),
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
        obs = batch['obs']
        mog_output = self._compute_mog_components(obs)
        return torch.sum(
            mog_output['means'] * torch.clamp(
                nn.functional.softmax(mog_output['alphas'], dim=-1), 1e-6, None
            ),
            dim=-1
        )

    def _compute_mog_components(self, obs: torch.Tensor) -> Dict[str, torch.Tensor]:
        mog_output = self.mog_critic(obs)
        means = mog_output[:, :self.num_gaussians]
        sigmas_prev = mog_output[:, self.num_gaussians:self.num_gaussians*2]
        sigmas = nn.functional.softplus(sigmas_prev) + 1e-6
        alphas = mog_output[:, self.num_gaussians*2:]
        # alphas = torch.clamp(nn.functional.softmax(alphas, dim=-1), 1e-6, None)
        return {
            'means': means,
            'sigmas': sigmas,
            'alphas': alphas,
        }

    @override(RLModule)
    def _forward_train(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        obs = batch["obs"]
        action_logits = self.policy(obs)
        value_function_out = self.compute_values(batch)

        return {
            'action_dist_inputs': action_logits,
            'value_function_out': value_function_out,
            'mog_components': self._compute_mog_components(obs),
        }