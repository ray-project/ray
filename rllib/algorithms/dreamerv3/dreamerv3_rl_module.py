"""
This file holds framework-agnostic components for DreamerV3's RLModule.
"""

import abc

import numpy as np

from ray.rllib.algorithms.dreamerv3.utils import do_symlog_obs
from ray.rllib.algorithms.dreamerv3.tf.models.actor_network import ActorNetwork
from ray.rllib.algorithms.dreamerv3.tf.models.critic_network import CriticNetwork
from ray.rllib.algorithms.dreamerv3.tf.models.dreamer_model import DreamerModel
from ray.rllib.algorithms.dreamerv3.tf.models.world_model import WorldModel
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.eager_tf_policy import _convert_to_tf
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import one_hot


@ExperimentalAPI
class DreamerV3RLModule(RLModule, abc.ABC):
    @override(RLModule)
    def setup(self):
        # Gather model-relevant settings.
        B = 1
        T = self.config.model_config_dict["batch_length_T"]
        horizon_H = self.config.model_config_dict["horizon_H"]
        gamma = self.config.model_config_dict["gamma"]
        symlog_obs = do_symlog_obs(
            self.config.observation_space,
            self.config.model_config_dict.get("symlog_obs", "auto"),
        )
        model_size = self.config.model_config_dict["model_size"]

        # Build encoder and decoder from catalog.
        catalog = self.config.get_catalog()
        self.encoder = catalog.build_encoder(framework=self.framework)
        self.decoder = catalog.build_decoder(framework=self.framework)

        # Build the world model (containing encoder and decoder).
        self.world_model = WorldModel(
            model_size=model_size,
            action_space=self.config.action_space,
            batch_length_T=T,
            encoder=self.encoder,
            decoder=self.decoder,
            symlog_obs=symlog_obs,
        )
        self.actor = ActorNetwork(
            action_space=self.config.action_space,
            model_size=model_size,
        )
        self.critic = CriticNetwork(
            model_size=model_size,
        )
        # Build the final dreamer model (containing the world model).
        self.dreamer_model = DreamerModel(
            model_size=self.config.model_config_dict["model_size"],
            action_space=self.config.action_space,
            world_model=self.world_model,
            actor=self.actor,
            critic=self.critic,
        )
        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

        # Perform a test `call()` to force building the dreamer model's variables.
        test_obs = np.tile(
            np.expand_dims(self.config.observation_space.sample(), (0, 1)),
            reps=(B, T) + (1,) * len(self.config.observation_space.shape),
        )
        test_actions = np.tile(
            np.expand_dims(
                one_hot(
                    self.config.action_space.sample(), depth=self.config.action_space.n
                ),
                (0, 1),
            ),
            reps=(B, T, 1),
        )
        self.dreamer_model(
            inputs=_convert_to_tf(test_obs),
            actions=_convert_to_tf(test_actions.astype(np.float32)),
            is_first=_convert_to_tf(np.ones((B, T), np.float32)),
            start_is_terminated_BxT=_convert_to_tf(np.zeros((B * T,), np.float32)),
            horizon_H=horizon_H,
            gamma=gamma,
        )

        # Initialize the critic EMA net:
        self.critic.init_ema()

    @override(RLModule)
    def get_initial_state(self) -> NestedDict:
        # Use `DreamerModel`'s `get_initial_state` method.
        return self.dreamer_model.get_initial_state()

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
            "obs_distribution_means_BxT",
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
