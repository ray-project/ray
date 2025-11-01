from ray.rllib.algorithms.sac.default_sac_rl_module import DefaultSACRLModule
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
)


class DefaultIQLRLModule(DefaultSACRLModule, ValueFunctionAPI):
    @override(DefaultSACRLModule)
    def setup(self):
        # Setup the `DefaultSACRLModule` to get the catalog.
        super().setup()

        # Only, if the `RLModule` is used on a `Learner` we build the value network.
        if not self.inference_only:
            # Build the encoder for the value function.
            self.vf_encoder = self.catalog.build_encoder(framework=self.framework)

            # Build the vf head.
            self.vf = MLPHeadConfig(
                input_dims=self.catalog.latent_dims,
                # Note, we use the same layers as for the policy and Q-network.
                hidden_layer_dims=self.catalog.pi_and_qf_head_hiddens,
                hidden_layer_activation=self.catalog.pi_and_qf_head_activation,
                output_layer_activation="linear",
                output_layer_dim=1,
            ).build(framework=self.framework)

    @override(DefaultSACRLModule)
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def get_non_inference_attributes(self):
        # Use all of `super`'s attributes and add the value function attributes.
        return super().get_non_inference_attributes() + ["vf_encoder", "vf"]
