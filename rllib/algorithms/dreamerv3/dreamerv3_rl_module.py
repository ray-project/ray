"""
This file holds framework-agnostic components for DreamerV3's RLModule.
"""

import abc
from typing import Any, Dict

from ray.rllib.algorithms.dreamerv3.utils import (
    do_symlog_obs,
    get_gru_units,
    get_num_z_categoricals,
    get_num_z_classes,
)
from ray.rllib.algorithms.dreamerv3.torch.models.actor_network import ActorNetwork
from ray.rllib.algorithms.dreamerv3.torch.models.critic_network import CriticNetwork
from ray.rllib.algorithms.dreamerv3.torch.models.dreamer_model import DreamerModel
from ray.rllib.algorithms.dreamerv3.torch.models.world_model import WorldModel
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.util.annotations import DeveloperAPI


ACTIONS_ONE_HOT = "actions_one_hot"


@DeveloperAPI(stability="alpha")
class DreamerV3RLModule(RLModule, abc.ABC):
    @override(RLModule)
    def setup(self):
        super().setup()

        # Gather model-relevant settings.
        B = 1
        T = self.model_config["batch_length_T"]
        horizon_H = self.model_config["horizon_H"]
        gamma = self.model_config["gamma"]
        symlog_obs = do_symlog_obs(
            self.observation_space,
            self.model_config.get("symlog_obs", "auto"),
        )
        model_size = self.model_config["model_size"]

        #if self.model_config["use_float16"]:
        #    tf.compat.v1.keras.layers.enable_v2_dtype_behavior()
        #    tf.keras.mixed_precision.set_global_policy("mixed_float16")

        # Build encoder and decoder from catalog.
        self.encoder = self.catalog.build_encoder(framework=self.framework)
        self.decoder = self.catalog.build_decoder(framework=self.framework)

        # Build the world model (containing encoder and decoder).
        self.world_model = WorldModel(
            model_size=model_size,
            observation_space=self.observation_space,
            action_space=self.action_space,
            batch_length_T=T,
            encoder=self.encoder,
            decoder=self.decoder,
            symlog_obs=symlog_obs,
        )
        input_size = (
            get_gru_units(model_size)
            + get_num_z_classes(model_size) * get_num_z_categoricals(model_size)
        )
        self.actor = ActorNetwork(
            input_size=input_size,
            action_space=self.action_space,
            model_size=model_size,
        )
        self.critic = CriticNetwork(
            input_size=input_size,
            model_size=model_size,
        )
        # Build the final dreamer model (containing the world model).
        self.dreamer_model = DreamerModel(
            model_size=self.model_config["model_size"],
            action_space=self.action_space,
            world_model=self.world_model,
            actor=self.actor,
            critic=self.critic,
            #horizon=horizon_H,
            #gamma=gamma,
        )
        self.action_dist_cls = self.catalog.get_action_dist_cls(
            framework=self.framework
        )

        # Perform a test `call()` to force building the dreamer model's variables.
        #if self.framework == "tf2":
        #    test_obs = np.tile(
        #        np.expand_dims(self.observation_space.sample(), (0, 1)),
        #        reps=(B, T) + (1,) * len(self.observation_space.shape),
        #    )
        #    if isinstance(self.action_space, gym.spaces.Discrete):
        #        test_actions = np.tile(
        #            np.expand_dims(
        #                one_hot(
        #                    self.action_space.sample(),
        #                    depth=self.action_space.n,
        #                ),
        #                (0, 1),
        #            ),
        #            reps=(B, T, 1),
        #        )
        #    else:
        #        test_actions = np.tile(
        #            np.expand_dims(self.action_space.sample(), (0, 1)),
        #            reps=(B, T, 1),
        #        )
        #
        #    self.dreamer_model(
        #        inputs=None,
        #        observations=_convert_to_tf(test_obs, dtype=tf.float32),
        #        actions=_convert_to_tf(test_actions, dtype=tf.float32),
        #        is_first=_convert_to_tf(np.ones((B, T)), dtype=tf.bool),
        #        start_is_terminated_BxT=_convert_to_tf(
        #            np.zeros((B * T,)), dtype=tf.bool
        #        ),
        #        gamma=gamma,
        #    )

        # Initialize the critic EMA net:
        self.critic.init_ema()

    @override(RLModule)
    def get_initial_state(self) -> Dict:
        # Use `DreamerModel`'s `get_initial_state` method.
        return self.dreamer_model.get_initial_state()
