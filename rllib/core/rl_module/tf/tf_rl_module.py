from typing import Any, Mapping

from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf


_, tf, _ = try_import_tf()


class TFRLModule(RLModule, tf.keras.Model):
    def __init__(self, config: Mapping[str, Any]) -> None:
        tf.keras.Model.__init__(self)
        RLModule.__init__(self, config)

    @override(tf.keras.Model)
    def call(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """forward pass of the module.

        This is aliased to forward_train because Torch DDP requires a forward method to
        be implemented for backpropagation to work.
        """
        return self.forward_train(batch, **kwargs)

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

    # @classmethod
    # @override(RLModule)
    # def get_multi_agent_class(cls) -> Type["MultiAgentRLModule"]:
    #     """Returns the multi-agent wrapper class for this module."""
    #     from ray.rllib.core.rl_module.tf.tf_marl_module import (
    #         TFMultiAgentRLModule,
    #     )

    #     return TFMultiAgentRLModule
