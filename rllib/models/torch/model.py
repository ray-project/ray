import torch
from torch import nn
import tree

from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    PublicAPI,
)
from ray.rllib.models.temp_spec_classes import TensorDict, ModelConfig
from ray.rllib.models.base_model import RecurrentModel, Model, ModelIO


class TorchModelIO(ModelIO):
    """Save/Load mixin for torch models

    Examples:
        >>> model.save("/tmp/model_weights.cpt")
        >>> model.load("/tmp/model_weights.cpt")
    """

    @PublicAPI
    @OverrideToImplementCustomLogic
    def save(self, path: str) -> None:
        """Saves the state dict to the specified path

        Args:
            path: Path on disk the checkpoint is saved to

        """
        torch.save(self.state_dict(), path)

    @PublicAPI
    @OverrideToImplementCustomLogic
    def load(self, path: str) -> RecurrentModel:
        """Loads the state dict from the specified path

        Args:
            path: Path on disk to load the checkpoint from
        """
        self.load_state_dict(torch.load(path))


class TorchRecurrentModel(RecurrentModel, nn.Module, TorchModelIO):
    """The base class for recurrent pytorch models.

    If implementing a custom recurrent model, you likely want to inherit
    from this model.

    Args:
        config: The config used to construct the model

    """

    def __init__(self, config: ModelConfig) -> None:
        Model.__init__(self)
        nn.Module.__init__(self)
        TorchModelIO.__init__(self, config)
        self._config = config

    @OverrideToImplementCustomLogic
    def _initial_state(self) -> TensorDict:
        """Returns the initial recurrent state

        This defaults to all zeros and can be overidden to return
        nonzero tensors.

        Returns:
            A TensorDict that matches the initial_state_spec
        """
        return TensorDict(
            tree.map_structure(
                lambda spec: torch.zeros(spec.shape, dtype=spec.dtype),
                self.initial_state_spec,
            )
        )


class TorchModel(Model, nn.Module, TorchModelIO):
    """The base class for non-recurrent pytorch models.

    If implementing a custom pytorch model, you likely want to
    inherit from this class.

    Args:
        config: The config used to construct the model
    """

    def __init__(self, config: ModelConfig) -> None:
        Model.__init__(self)
        nn.Module.__init__(self)
        TorchModelIO.__init__(self, config)
        self._config = config
