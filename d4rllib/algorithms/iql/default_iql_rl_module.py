from ray.rllib.algorithms.sac.default_sac_rl_module import DefaultSACRLModule
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

from d4rllib.algorithms.iql.iql_learner import VF_PREDS_NEXT


class DefaultIQLRLModule(DefaultSACRLModule, ValueFunctionAPI):

    @override(DefaultSACRLModule)
    def setup(self):
        super().setup()

        if not self.inference_only:
            # Build the encoder for the value function.
            self.vf_encoder = self.catalog.build_encoder(framework=self.framework)

            # Build the vf head.
            self.vf = MLPHeadConfig(
                input_dims=self.catalog.latent_dims,
                hidden_layer_dims=self.catalog.pi_and_qf_head_hiddens,
                hidden_layer_activation=self.catalog.pi_and_qf_head_activation,
                output_layer_activation="linear",
                output_layer_dim=1,
            ).build(framework=self.framework)

    @override(DefaultSACRLModule)
    def output_specs_train(self):
        output_specs = super().output_specs_train()
        output_specs += [Columns.VF_PREDS, VF_PREDS_NEXT]
        return output_specs

    @override(DefaultSACRLModule)
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def get_non_inference_attributes(self):
        ret = super().get_non_inference_attributes()
        ret += ["vf_encoder", "vf"]
        return ret
