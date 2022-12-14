from ray.rllib.core.rl_module import RLModule
from ray.rllib.models.model_catalog import *
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

@ExperimentalAPI
class DefaultRLModule(RLModule):
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self,
                 config,
                 observation_space,
                 action_space,
                 framework,
                 shared_encoder_config,
                 actor_encoder_config,
                 critic_encoder_config,
                 pi_config,
                 v_config,
                 q_config,
                 q_pi_config,
                 pi_q_config,
                 ):
        super().__init__(config)


        if shared_encoder_config:
            self.shared_encoder = get_shared_encoder(shared_encoder_config)
        if actor_encoder_config:
            self.actor_encoder = get_actor_encoder(actor_encoder_config)
        if critic_encoder_config:
            self.critic_encoder = get_critic_encoder(critic_encoder_config)
        if pi_config:
            self.pi_head = get_pi_head(pi_config)
        if v_config:
            self.v_head = get_v_head(v_config)
        if q_config:
            self.q_head = get_q_head(q_config)
        if q_pi_config:
            self.q_pi_head= get_q_pi_head(q_pi_config)
        if pi_q_config:
            self.pi_q_head = get_pi_q_head(pi_q_config)


