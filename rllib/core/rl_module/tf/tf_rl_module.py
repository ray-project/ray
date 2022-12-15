import abc
from typing import Any, Mapping

from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict


_, tf, _ = try_import_tf()


class TfRLModule(RLModule, tf.keras.Model):
    """Base class for RLlib TF RLModules."""

    def __init__(
        self,
    ) -> None:
        tf.keras.Model.__init__(self)
        RLModule.__init__(self)

    @override(tf.keras.Model)
    def call(self, batch: Mapping[str, Any], training=False) -> Mapping[str, Any]:
        """forward pass of the module.

        This is aliased to forward_train because Torch DDP requires a forward method to
        be implemented for backpropagation to work.
        """
        return self.forward_train(batch)

    @override(RLModule)
    def get_state(self) -> Mapping[str, Any]:
        return self.get_weights()

    @override(RLModule)
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        self.set_weights(state_dict)

    @override(RLModule)
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Makes the module distributed."""
        # TODO (Avnish): Implement this.
        pass

    @override(RLModule)
    def is_distributed(self) -> bool:
        """Returns True if the module is distributed."""
        # TODO (Avnish): Implement this.
        return False

    @abc.abstractmethod
    def trainable_variables(self) -> NestedDict[tf.Tensor]:
        """Returns the trainable variables of the module.

        Example:
            return {"module": module.trainable_variables}

        Note:
            see tensorflow.org/guide/autodiff#gradients_with_respect_to_a_model
            for more details

        """
