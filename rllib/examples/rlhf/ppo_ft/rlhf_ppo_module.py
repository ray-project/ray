
import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog

class RLHFPPOCatalog(PPOCatalog):
    """Custom model components for ppo based RLHF."""
    # Note(jungong) : we wouldn't have to override this __init__(),
    # except that PPOCatalog insists that complex Dict input space
    # is not supported.
    def __init__(
            self,
            observation_space: gym.Space,
            action_space: gym.Space,
            model_config_dict: dict,
        ):
        PPOCatalog.__init__(self, observation_space, action_space, model_config_dict)

        assert isinstance(action_space, gym.spaces.Box)
        pi_output_dim = action_space.shape[0] * 2

        post_fcnet_hiddens = self.model_config_dict["post_fcnet_hiddens"]
        post_fcnet_activation = self.model_config_dict["post_fcnet_activation"]

        self.pi_head_config = MLPHeadConfig(
            input_dim=self.encoder_config.output_dim,
            hidden_layer_dims=post_fcnet_hiddens,
            hidden_layer_activation=post_fcnet_activation,
            output_activation="linear",
            output_dim=pi_output_dim,
        )

        self.vf_head_config = MLPHeadConfig(
            input_dim=self.encoder_config.output_dim,
            hidden_layer_dims=post_fcnet_hiddens,
            hidden_layer_activation=post_fcnet_activation,
            output_activation="linear",
            output_dim=1,
        )

        # Set input- and output dimensions to fit PPO's needs.
        self.pi_head_config.input_dim = self.encoder_config.output_dim
        self.vf_head_config.input_dim = self.encoder_config.output_dim
        self.pi_head_config.output_dim = int(action_space.shape[0] * 2)
        self.vf_head_config.output_dim = 1

    def build_actor_critic_encoder(self, framework: str):
        # Note(jungong) : this is not how everyone else does RLHF.
        # We should LoRa or directly fine-tune the GPT-2 model as
        # actor and critic networks.
        # Instead, we use GPT-2 as a frozen encoder layer, and train
        # the actor and critic networks on top of it.
        # This is just to get the ball rolling.
        # TODO(jungong) : replace this with LoRa or direct LLM fine-tuning.
        return RLHFSharedEncoder(self.model_config_dict)
