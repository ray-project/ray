from typing import TYPE_CHECKING, Any, Dict, Optional

import torch

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint
from ray.air._internal.torch_utils import load_torch_model
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class TorchCheckpoint(Checkpoint):
    """A :class:`~ray.air.checkpoint.Checkpoint` with Torch-specific functionality.

    Create this from a generic :class:`~ray.air.checkpoint.Checkpoint` by calling
    ``TorchCheckpoint.from_checkpoint(ckpt)``.
    """

    @classmethod
    def from_state_dict(
        cls,
        state_dict: Dict[str, Any],
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TorchCheckpoint":
        """Create a :class:`~ray.air.checkpoint.Checkpoint` that stores a model state
        dictionary.

        .. tip::

            This is the recommended method for creating
            :class:`TorchCheckpoints<TorchCheckpoint>`.

        Args:
            state_dict: The model state dictionary to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :class:`TorchCheckpoint` containing the specified state dictionary.

        Examples:
            >>> from ray.train.torch import TorchCheckpoint
            >>> import torch
            >>>
            >>> model = torch.nn.Linear(1, 1)
            >>> checkpoint = TorchCheckpoint.from_state_dict(model.state_dict())

            To load the state dictionary, call
            :meth:`~ray.train.torch.TorchCheckpoint.get_model`.

            >>> checkpoint.get_model(torch.nn.Linear(1, 1))
            Linear(in_features=1, out_features=1, bias=True)
        """
        return cls.from_dict({PREPROCESSOR_KEY: preprocessor, MODEL_KEY: state_dict})

    @classmethod
    def from_model(
        cls,
        model: torch.nn.Module,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TorchCheckpoint":
        """Create a :class:`~ray.air.checkpoint.Checkpoint` that stores a Torch model.

        .. note::

            PyTorch recommends storing state dictionaries. To create a
            :class:`TorchCheckpoint` from a state dictionary, call
            :meth:`~ray.train.torch.TorchCheckpoint.from_state_dict`. To learn more
            about state dictionaries, read
            `Saving and Loading Models <https://pytorch.org/tutorials/beginner/saving_loading_models.html#what-is-a-state-dict>`_.

        Args:
            model: The Torch model to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :class:`TorchCheckpoint` containing the specified model.

        Examples:
            >>> from ray.train.torch import TorchCheckpoint
            >>> import torch
            >>>
            >>> model = torch.nn.Identity()
            >>> checkpoint = TorchCheckpoint.from_model(model)

            You can use a :class:`TorchCheckpoint` to create an
            :class:`~ray.train.torch.TorchPredictor` and perform inference.

            >>> from ray.train.torch import TorchPredictor
            >>>
            >>> predictor = TorchPredictor.from_checkpoint(checkpoint)
        """  # noqa: E501
        # NOTE: For context on this `ValueError`, see issue #27922.
        if model.__module__ == "__main__":
            raise ValueError(
                f"`{cls.__name__}` can't serialize model of type "
                f"`{model.__class__.__name__}` because `{model.__class__.__name__}` "
                "is defined in the top-level environment. To work "
                f"around this error, call `{cls.__name__}.from_state_dict` instead of "
                f"`{cls.__name__}.from_model`. Alternatively, move the definition of "
                f"`{model.__class__.__name__}` to a different module."
            )

        checkpoint = cls.from_dict({PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model})
        return checkpoint

    def get_model(self, model: Optional[torch.nn.Module] = None) -> torch.nn.Module:
        """Retrieve the model stored in this checkpoint.

        Args:
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``.
        """
        saved_model, _ = _load_checkpoint(self, "TorchTrainer")
        model = load_torch_model(saved_model=saved_model, model_definition=model)
        return model
