from ray.rllib.models.model_catalog import *


class DefaultModelBuilder:
    def __init__(self, obs_space, action_space, framework):
        self.obs_space = obs_space
        self.action_space = action_space
        self.framework = framework
        if framework == "torch":
            pass
            # self.encoder_class = ...
        elif framework in ("tf", "tf2"):
            pass
            # self.encoder_class = ...
        elif framework == "jax":
            pass
            # self.encoder_class = ...

    def build_shared_encoder(
            self,
            use_lstm,
            use_attention,
            conv_filters,
            conv_activations,
            fcnet_hiddens,
            fcnet_activations,
            post_fcnet_hiddens,
            post_fcnet_activations,
            no_final_linear,
            vf_share_layers
        ):
        pass

    def build_actor_encoder(
            self,
            use_lstm,
            use_attention,
            conv_filters,
            conv_activations,
            fcnet_hiddens,
            fcnet_activations,
            post_fcnet_hiddens,
            post_fcnet_activations,
            no_final_linear,
            vf_share_layers
    ):
        pass

    def build_critic_encoder(
            self,
            use_lstm,
            use_attention,
            conv_filters,
            conv_activations,
            fcnet_hiddens,
            fcnet_activations,
            post_fcnet_hiddens,
            post_fcnet_activations,
            no_final_linear,
            vf_share_layers
    ):
        pass

    def build_pi_head(
        self,
        distribution
    ):
        pass

    def build_q_head(self):
        pass

    def build_pi_q_head(
        self,
        distribution
    ):
        pass

    def build_q_pi_head(self):
        pass

