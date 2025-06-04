from typing import Any, Collection, Dict, Optional, Type, Union

import gymnasium as gym

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.tf.tf_distributions import (
    TfCategorical,
    TfDiagGaussian,
    TfDistribution,
)
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import StateDict

_, tf, _ = try_import_tf()


class TfRLModule(tf.keras.Model, RLModule):
    """Base class for RLlib TensorFlow RLModules."""

    framework = "tf2"

    def __init__(self, *args, **kwargs) -> None:
        tf.keras.Model.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

    def call(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward pass of the module.

        Note:
            This is aliased to forward_train to follow the Keras Model API.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_train().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_train().

        """
        return self.forward_train(batch)

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        inference_only: bool = False,
        **kwargs,
    ) -> StateDict:
        return self.get_weights()

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def set_state(self, state: StateDict) -> None:
        self.set_weights(state)

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def get_inference_action_dist_cls(self) -> Type[TfDistribution]:
        if self.action_dist_cls is not None:
            return self.action_dist_cls
        elif isinstance(self.action_space, gym.spaces.Discrete):
            return TfCategorical
        elif isinstance(self.action_space, gym.spaces.Box):
            return TfDiagGaussian
        else:
            raise ValueError(
                f"Default action distribution for action space "
                f"{self.action_space} not supported! Either set the "
                f"`self.action_dist_cls` property in your RLModule's `setup()` method "
                f"to a subclass of `ray.rllib.models.tf.tf_distributions."
                f"TfDistribution` or - if you need different distributions for "
                f"inference and training - override the three methods: "
                f"`get_inference_action_dist_cls`, `get_exploration_action_dist_cls`, "
                f"and `get_train_action_dist_cls` in your RLModule."
            )

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def get_exploration_action_dist_cls(self) -> Type[TfDistribution]:
        return self.get_inference_action_dist_cls()

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def get_train_action_dist_cls(self) -> Type[TfDistribution]:
        return self.get_inference_action_dist_cls()
