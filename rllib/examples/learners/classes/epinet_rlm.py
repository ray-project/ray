import gymnasium as gym
from typing import Any, Dict, Optional

from torch.distributions.normal import Normal
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.examples.learners.classes.epinet_learner import (
    CRITIC_OUTPUTS,
    CRITIC_OUTPUT_BASE,
    CRITIC_OUTPUT_ENN,
    NEXT_CRITIC_OUTPUTS,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.torch.misc import SlimFC

torch, nn = try_import_torch()


class EpinetTorchRLModule(TorchRLModule, ValueFunctionAPI):
    """
    Custom `RLModule` that demonstrates setting up, defining the necessary _forward methods,
    and overriding the `ValueFunctionAPI` to compute values using an epinet for exploration.

    This also uses a custom_config from the module_to_load_spec to get the fcnet_hiddens
    as well as custom args CRITIC_OUTPUTS and NEXT_CRITIC_OUTPUTS to use in the learner.
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

        initializer = model_config["initializer"]
        self.enn_network = model_config["enn_network"]
        self.fcnet_hiddens = model_config["fcnet_hiddens"]
        activation_str = self.model_config["fcnet_activation"]
        # Epinet variables.
        self.std = 1.0
        self.mean = 0.0
        self.step_number = 0
        self.z_indices = None
        self.step_cut_off = 100
        self.z_dim = model_config["z_dim"]
        self.activation_fn = getattr(nn, activation_str)
        self.initializer = getattr(nn.init, initializer)

        self.distribution = Normal(
            torch.full((self.z_dim,), self.mean), torch.full((self.z_dim,), self.std)
        )
        self.enn_output = 0.0
        self.base_output = 0.0

    def setup(self):
        obs_dim = self.observation_space.shape[0]
        hidden_dim = self.model_config["fcnet_hiddens"][0]

        # Set failsafe for action space dim.
        if isinstance(self.action_space, gym.spaces.Box):
            output_dim = self.action_space.shape[0] * 2
        elif isinstance(self.action_space, gym.spaces.Discrete):
            output_dim = self.action_space.n
        else:
            raise ValueError(
                f"Unsupported action space type: {type(self.action_space)}"
            )

        # Build policy with Tanh() at the end for continous action selection.
        self.policy = nn.Sequential(
            *self.build_network_layers(
                input_dim=obs_dim,
                fcnet_hiddens=self.fcnet_hiddens,
                output_dim=output_dim,
                final_activation=False,
            ),
            # Note: change this if the enviroment's actions are not normalized between [1, 1].
            nn.Tanh(),
        )

        # Build the base critic network.
        self.base_critic = nn.Sequential(
            *self.build_network_layers(
                input_dim=obs_dim,
                fcnet_hiddens=self.fcnet_hiddens,
                output_dim=self.fcnet_hiddens[-1],
                final_activation=True,
            )
        )
        # The last layer must be separate to get the logits from the base network.
        self.last_layer_critic = nn.Linear(hidden_dim, 1)
        # Make sure initializer is the same for the last layer.
        self.initializer(self.last_layer_critic.weight)
        self.initializer(self.last_layer_critic.bias)

        # Build small lightweight epinet.
        self.learnable_layers = nn.Sequential(
            *self.build_network_layers(
                input_dim=hidden_dim + 1,
                fcnet_hiddens=self.enn_network,
                final_activation=True,
                output_dim=1,
            )
        )
        # Build prior layers of the epinet.
        self.prior_layers = nn.Sequential(
            *self.build_network_layers(
                input_dim=hidden_dim + 1,
                fcnet_hiddens=self.enn_network,
                final_activation=True,
                output_dim=1,
            )
        )

    def build_network_layers(
        self, input_dim, fcnet_hiddens, output_dim, final_activation=True
    ):
        """
        A simple method to build the ENN learnable / prior, critic and policy network.
        Args:
            -input_dim: the input dimension of the network.
            -num_layers: the number of layers for the network.
            -layer_size: the size of the layers.
            -output_dim: the output dimension.
        Returns:
            -A list of layers that can be unpacked into nn.Sequential()
        """
        layers = []
        current_dim = input_dim

        for hidden_size in fcnet_hiddens:
            layers.append(
                SlimFC(
                    current_dim,
                    hidden_size,
                    initializer=self.initializer,
                    activation_fn=self.activation_fn,
                )
            )
            current_dim = hidden_size
        # Append the final layer.
        layers.append(
            SlimFC(
                current_dim,
                output_dim,
                initializer=self.initializer,
                activation_fn=self.activation_fn if final_activation else None,
            )
        )

        return layers

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
        obs = batch[Columns.OBS]
        # Get base critic network output and epinet output.
        critic_output = self._pass_through_epinet(obs)
        total_output = (
            critic_output["critic_output_base"] + critic_output["critic_output_enn"]
        )
        return total_output

    def _pass_through_epinet(self, obs: torch.Tensor) -> Dict[str, torch.Tensor]:
        """
        The epinet will now take in the `enn_input` which is z_dim number of Gaussian samples
        multiplied by the logits from the critic network. This is effectively having z_dim
        number of priors and testing the network against each prior to see how certain it is
        of the state.

        Parameters:
            -step_cut_off: Is how many timesteps in which the prior network will no longer be learning.
              This captures the initial epistemic uncertainty in the environment and it will be injected
              while the agent learns.
            -step_number: The number of forward passes that have been made.

        Important:
            -The `base_output` is detached before sending into the epinet because gradients should not flow
              from the epinet to the base network as it is learning the epistemic uncertainty.
            -The `base_output` is still passed to the learner so that the loss can be calculated for the base critic.
              This is why it is not detached completely form the computation graph.
        """
        # Forward pass through the epinet.
        intermediate = self.base_critic(obs)
        base_output = self.last_layer_critic(intermediate)
        intermediate_unsqueeze = torch.unsqueeze(intermediate, 1)
        # Draw sample from distribution and cat to logits.
        self.z_samples = (
            self.distribution.sample((obs.shape[0],)).unsqueeze(-1).to(obs.device)
        )
        # Disconnect the base network from the epinet computational graph.
        with torch.no_grad():
            enn_input = torch.cat(
                (self.z_samples, intermediate_unsqueeze.expand(-1, self.z_dim, -1)),
                dim=2,
            )
        # Enn, prior and base network pass.
        if self.step_number < self.step_cut_off:
            # Only update prior for xx timesteps.
            prior_out = self.prior_layers(enn_input)
        else:
            with torch.no_grad():
                # This now encapsulates the uncertainty and will inject into each timestep.
                prior_out = self.prior_layers(enn_input)
        prior_bmm = torch.bmm(torch.transpose(prior_out, 1, 2), self.z_samples)
        prior = prior_bmm.squeeze(-1)
        # Pass through learnable part of the ENN.
        learnable_out = self.learnable_layers(enn_input)
        learnable_bmm = torch.bmm(torch.transpose(learnable_out, 1, 2), self.z_samples)
        learnable = learnable_bmm.squeeze(-1)
        enn_output = learnable + prior
        return {
            CRITIC_OUTPUT_BASE: base_output,
            CRITIC_OUTPUT_ENN: enn_output,
        }

    @override(RLModule)
    def _forward_train(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        obs = batch[Columns.OBS]
        assert Columns.NEXT_OBS in batch, f"No {Columns.NEXT_OBS} in batch."
        next_obs = batch[Columns.NEXT_OBS]
        action_logits = self.policy(obs)
        value_function_out = self.compute_values(batch)

        self.step_number += 1
        return {
            Columns.ACTION_DIST_INPUTS: action_logits,
            Columns.VF_PREDS: value_function_out,
            CRITIC_OUTPUTS: self._pass_through_epinet(obs),
            NEXT_CRITIC_OUTPUTS: self._pass_through_epinet(next_obs),
        }
