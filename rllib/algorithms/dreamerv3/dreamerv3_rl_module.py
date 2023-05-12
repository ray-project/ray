"""
This file holds framework-agnostic components for DreamerV3's RLModule.
"""

import abc

from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override


@ExperimentalAPI
class DreamerV3RLModule(RLModule, abc.ABC):
    # def __init__(self, config: RLModuleConfig):
    #    super().__init__(config)

    def setup(self):
        catalog = self.config.get_catalog()
        # Build encoder and decoder from catalog.
        self.encoder = catalog.build_encoder(framework=self.framework)
        self.decoder = catalog.build_decoder(framework=self.framework)
        # Build the world model (containing encoder and decoder).
        self.world_model = WorldModel(
            model_dimension=self.model_dimension,
            action_space=self.action_space,
            batch_length_T=self.model_config.batch_length_T,
            num_gru_units=self.model_config.num_gru_units,
            encoder=encoder,
            decoder=decoder,
            symlog_obs=self.model_config.symlog_obs,
        )
        # Build the final dreamer model (containing the world model).
        self.dreamer_model = DreamerModel(
            model_dimension=self.model_dimension,
            action_space=self.action_space,
            world_model=world_model,
            # use_curiosity=use_curiosity,
            # intrinsic_rewards_scale=intrinsic_rewards_scale,
            batch_size_B=self.model_config.batch_size_B,
            batch_length_T=self.model_config.batch_length_T,
            horizon_H=self.model_config.horizon_H,
        )
        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return [SampleBatch.OBS, STATE_IN, "is_first"]

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return [SampleBatch.ACTIONS, STATE_OUT]

    @override(RLModule)
    def input_specs_exploration(self):
        return self.input_specs_inference()

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        return self.output_specs_inference()

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        return [SampleBatch.OBS, SampleBatch.ACTIONS, "is_first"]

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        return [
            "sampled_obs_symlog_BxT",
            "obs_distribution_BxT",
            "reward_logits_BxT",
            "rewards_BxT",
            "continue_distribution_BxT",
            "continues_BxT",

            # Sampled, discrete posterior z-states (t1 to T).
            "z_posterior_states_BxT",
            "z_posterior_probs_BxT",
            "z_prior_probs_BxT",

            # Deterministic, continuous h-states (t1 to T).
            "h_states_BxT",
        ]
