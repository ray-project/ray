import gym
import tensorflow as tf
import tensorflow_probability as tfp
from typing import Any, Mapping, Union

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
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
        super().__init__()
        layers = []

        layers.append(tf.keras.Input(shape=(input_dim,)))
        layers.append(tf.keras.layers.ReLU())
        layers.append(tf.keras.layers.Dense(hidden_dim))
        layers.append(tf.keras.layers.ReLU())
        layers.append(tf.keras.layers.Dense(output_dim))

        self.policy = tf.keras.Sequential(layers)
        self._input_dim = input_dim
        self._output_dim = output_dim

    @override(RLModule)
    def input_specs_exploration(self) -> SpecDict:
        return SpecDict(self._default_inputs())

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return SpecDict(self._default_inputs())

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        return SpecDict(self._default_inputs())

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        return SpecDict(self._default_outputs())

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return SpecDict(self._default_outputs())

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        return SpecDict(self._default_outputs())

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

    @override(TfRLModule)
    def trainable_variables(self) -> NestedDict[tf.Tensor]:
        return NestedDict({"policy": self.policy.trainable_variables})

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: "gym.Space",
        action_space: "gym.Space",
        model_config: Mapping[str, Any],
    ) -> Union["RLModule", Mapping[str, Any]]:

        config = {
            "input_dim": observation_space.shape[0],
            "hidden_dim": model_config["hidden_dim"],
            "output_dim": action_space.n,
        }

        return cls(**config)

    def _default_inputs(self) -> SpecDict:
        obs_dim = self._input_dim
        return {"obs": TFTensorSpecs("b, do", do=obs_dim)}

    def _default_outputs(self) -> SpecDict:
        return {"action_dist": tfp.distributions.Distribution}
