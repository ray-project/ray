"""
This file holds framework-agnostic components for DreamerV3's RLModule.
"""

import abc

from ray.rllib.algorithms.dreamerv3.utils import do_symlog_obs
from ray.rllib.algorithms.dreamerv3.tf.models.dreamer_model import DreamerModel
from ray.rllib.algorithms.dreamerv3.tf.models.world_model import WorldModel
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override


@ExperimentalAPI
class DreamerV3RLModule(RLModule, abc.ABC):
    # def __init__(self, config: RLModuleConfig):
    #    super().__init__(config)

    def setup(self):
        catalog = self.config.get_catalog()

        symlog_obs = do_symlog_obs(
            self.config.observation_space,
            self.config.model_config_dict.get("symlog_obs", "auto"),
        )

        # Build encoder and decoder from catalog.
        self.encoder = catalog.build_encoder(framework=self.framework)
        self.decoder = catalog.build_decoder(framework=self.framework)
        # Build the world model (containing encoder and decoder).
        self.world_model = WorldModel(
            model_dimension=self.config.model_config_dict["model_dimension"],
            action_space=self.config.action_space,
            batch_length_T=self.config.model_config_dict["batch_length_T"],
            #num_gru_units=self.model_config.num_gru_units,
            encoder=self.encoder,
            decoder=self.decoder,
            symlog_obs=symlog_obs,
        )
        # Build the final dreamer model (containing the world model).
        self.dreamer_model = DreamerModel(
            model_dimension=self.config.model_config_dict["model_dimension"],
            action_space=self.config.action_space,
            world_model=self.world_model,
            # use_curiosity=use_curiosity,
            # intrinsic_rewards_scale=intrinsic_rewards_scale,
            batch_size_B=self.config.model_config_dict["batch_size_B"],
            batch_length_T=self.config.model_config_dict["batch_length_T"],
            horizon_H=self.config.model_config_dict["horizon_H"],
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
