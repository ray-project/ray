import gymnasium as gym
import tensorflow as tf
import tensorflow_probability as tfp
from typing import Any, Mapping, Optional

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec, ModuleID
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.models.specs.typing import SpecType
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict


class DiscreteBCTFModule(TfRLModule):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        output_dim: int,
    ) -> None:
        super().__init__(
            input_dim=input_dim, output_dim=output_dim, hidden_dim=hidden_dim
        )
        layers = []

        layers.append(tf.keras.Input(shape=(input_dim,)))
        layers.append(tf.keras.layers.ReLU())
        layers.append(tf.keras.layers.Dense(hidden_dim))
        layers.append(tf.keras.layers.ReLU())
        layers.append(tf.keras.layers.Dense(output_dim))

        self.policy = tf.keras.Sequential(layers)
        self._input_dim = input_dim

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return ["action_dist"]

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return ["action_dist"]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return ["action_dist"]

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)
        action_logits_inference = tf.argmax(action_logits, axis=-1)
        action_dist = tfp.distributions.Deterministic(action_logits_inference)
        return {"action_dist": action_dist}

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        return self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)
        action_dist = tfp.distributions.Categorical(logits=action_logits)
        return {"action_dist": action_dist}

    @override(RLModule)
    def get_state(self) -> Mapping[str, Any]:
        return {"policy": self.policy.get_weights()}

    @override(RLModule)
    def set_state(self, state: Mapping[str, Any]) -> None:
        self.policy.set_weights(state["policy"])

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: "gym.Space",
        action_space: "gym.Space",
        *,
        model_config_dict: Mapping[str, Any],
    ) -> "DiscreteBCTFModule":

        config = {
            "input_dim": observation_space.shape[0],
            "hidden_dim": model_config_dict["fcnet_hiddens"][0],
            "output_dim": action_space.n,
        }

        return cls(**config)


class BCTfRLModuleWithSharedGlobalEncoder(TfRLModule):
    def __init__(self, encoder, local_dim, hidden_dim, action_dim):
        super().__init__()

        self.encoder = encoder
        self.policy_head = tf.keras.Sequential(
            [
                tf.keras.layers.Dense(
                    hidden_dim + local_dim,
                    input_shape=(hidden_dim + local_dim,),
                    activation="relu",
                ),
                tf.keras.layers.Dense(hidden_dim, activation="relu"),
                tf.keras.layers.Dense(action_dim),
            ]
        )

    @override(RLModule)
    def _default_input_specs(self):
        return [("obs", "global"), ("obs", "local")]

    @override(RLModule)
    def _forward_inference(self, batch):
        return self._common_forward(batch)

    @override(RLModule)
    def _forward_exploration(self, batch):
        return self._common_forward(batch)

    @override(RLModule)
    def _forward_train(self, batch):
        return self._common_forward(batch)

    def _common_forward(self, batch):
        obs = batch["obs"]
        global_enc = self.encoder(obs["global"])
        policy_in = tf.concat([global_enc, obs["local"]], axis=-1)
        action_logits = self.policy_head(policy_in)

        return {"action_dist": tf.distributions.Categorical(logits=action_logits)}


class BCTfMultiAgentSpec(MultiAgentRLModuleSpec):
    def build(self, module_id: Optional[ModuleID] = None):

        self._check_before_build()
        # constructing the global encoder based on the observation_space of the first
        # module
        module_spec = next(iter(self.module_specs.values()))
        global_dim = module_spec.observation_space["global"].shape[0]
        hidden_dim = module_spec.model_config["fcnet_hiddens"][0]
        shared_encoder = tf.keras.Sequential(
            [
                tf.keras.Input(shape=(global_dim,)),
                tf.keras.layers.ReLU(),
                tf.keras.layers.Dense(hidden_dim),
            ]
        )

        if module_id:
            return module_spec.module_class(
                encoder=shared_encoder,
                local_dim=module_spec.observation_space["local"].shape[0],
                hidden_dim=hidden_dim,
                action_dim=module_spec.action_space.n,
            )

        rl_modules = {}
        for module_id, module_spec in self.module_specs.items():
            rl_modules[module_id] = module_spec.module_class(
                encoder=shared_encoder,
                local_dim=module_spec.observation_space["local"].shape[0],
                hidden_dim=hidden_dim,
                action_dim=module_spec.action_space.n,
            )

        return self.marl_module_class(rl_modules)
