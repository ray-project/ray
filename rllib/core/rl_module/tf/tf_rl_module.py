import pathlib
from typing import Any, Dict, Union

from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.utils.annotations import override
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

    @override(RLModule)
    def get_state(self, inference_only: bool = False) -> StateDict:
        return self.get_weights()

    @override(RLModule)
    def set_state(self, state: StateDict) -> None:
        self.set_weights(state)
