# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic


class PPOLagrangeCatalog(PPOCatalog):
    """The Catalog class used to build models for PPO Lagrangian.

    PPOLagrangeCatalog provides the following models:
        - ActorCriticEncoder: The encoder used to encode the observations.
        - Pi Head: The head used to compute the policy logits.
        - Value Function Head: The head used to compute the value function.
        - Cost Value Function Head: The head used to compute the cost value function.

    The ActorCriticEncoder is a wrapper around Encoders to produce separate outputs
    for the policy and value function. See implementations of PPORLModule for
    more details.

    Any custom ActorCriticEncoder can be built by overriding the
    build_actor_critic_encoder() method. Alternatively, the ActorCriticEncoderConfig
    at PPOLagrangeCatalog.actor_critic_encoder_config can be overridden to build
    a custom ActorCriticEncoder during RLModule runtime.

    Any custom head can be built by overriding the build_pi_head() and build_vf_head()
    methods. Alternatively, the PiHeadConfig and VfHeadConfig can be overridden to
    build custom heads during RLModule runtime.
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
    ):
        """Initializes the PPOLagrangeCatalog.

        Args:
            observation_space: The observation space of the Encoder.
            action_space: The action space for the Pi Head.
            model_config_dict: The model config to use.
        """
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )
        self.cvf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_vf_head_hiddens,
            hidden_layer_activation=self.pi_and_vf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=1,
        )

    @OverrideToImplementCustomLogic
    def build_cvf_head(self, framework: str) -> Model:
        """Builds the cost value function head.

        The default behavior is to build the head from the cvf_head_config.
        This can be overridden to build a custom cost value function head as a means of
        configuring the behavior of a PPORLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The cost value function head.
        """
        return self.cvf_head_config.build(framework=framework)


# __sphinx_doc_end__
