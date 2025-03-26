import os
import tempfile
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

import torch

from ray.air._internal.torch_utils import (
    consume_prefix_in_state_dict_if_present_not_in_place,
    load_torch_model,
)
from ray.train._internal.framework_checkpoint import FrameworkCheckpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

ENCODED_DATA_KEY = "torch_encoded_data"


@PublicAPI(stability="beta")
class TorchCheckpoint(FrameworkCheckpoint):
    """A :class:`~ray.train.Checkpoint` with Torch-specific functionality."""

    MODEL_FILENAME = "model.pt"

    @classmethod
    def from_state_dict(
        cls,
        state_dict: Dict[str, Any],
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TorchCheckpoint":
        """Create a :class:`~ray.train.Checkpoint` that stores a model state dictionary.

        .. tip::

            This is the recommended method for creating
            :class:`TorchCheckpoints<TorchCheckpoint>`.

        Args:
            state_dict: The model state dictionary to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :class:`TorchCheckpoint` containing the specified state dictionary.

        Examples:

            .. testcode::

                import torch
                import torch.nn as nn
                from ray.train.torch import TorchCheckpoint

                # Set manual seed
                torch.manual_seed(42)

                # Function to create a NN model
                def create_model() -> nn.Module:
                    model = nn.Sequential(nn.Linear(1, 10),
                            nn.ReLU(),
                            nn.Linear(10,1))
                    return model

                # Create a TorchCheckpoint from our model's state_dict
                model = create_model()
                checkpoint = TorchCheckpoint.from_state_dict(model.state_dict())

                # Now load the model from the TorchCheckpoint by providing the
                # model architecture
                model_from_chkpt = checkpoint.get_model(create_model())

                # Assert they have the same state dict
                assert str(model.state_dict()) == str(model_from_chkpt.state_dict())
                print("worked")

            .. testoutput::
                :hide:

                ...
        """
        tempdir = tempfile.mkdtemp()

        model_path = Path(tempdir, cls.MODEL_FILENAME).as_posix()
        stripped_state_dict = consume_prefix_in_state_dict_if_present_not_in_place(
            state_dict, "module."
        )
        torch.save(stripped_state_dict, model_path)

        checkpoint = cls.from_directory(tempdir)
        if preprocessor:
            checkpoint.set_preprocessor(preprocessor)
        return checkpoint

    @classmethod
    def from_model(
        cls,
        model: torch.nn.Module,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TorchCheckpoint":
        """Create a :class:`~ray.train.Checkpoint` that stores a Torch model.

        .. note::

            PyTorch recommends storing state dictionaries. To create a
            :class:`TorchCheckpoint` from a state dictionary, call
            :meth:`~ray.train.torch.TorchCheckpoint.from_state_dict`. To learn more
            about state dictionaries, read
            `Saving and Loading Models <https://pytorch.org/tutorials/beginner/saving_loading_models.html#what-is-a-state-dict>`_. # noqa: E501

        Args:
            model: The Torch model to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :class:`TorchCheckpoint` containing the specified model.

        Examples:

            .. testcode::

                from ray.train.torch import TorchCheckpoint
                import torch

                # Create model identity and send a random tensor to it
                model = torch.nn.Identity()
                input = torch.randn(2, 2)
                output = model(input)

                # Create a checkpoint
                checkpoint = TorchCheckpoint.from_model(model)
                print(checkpoint)

            .. testoutput::
                :hide:

                ...
        """
        tempdir = tempfile.mkdtemp()

        model_path = Path(tempdir, cls.MODEL_FILENAME).as_posix()
        torch.save(model, model_path)

        checkpoint = cls.from_directory(tempdir)
        if preprocessor:
            checkpoint.set_preprocessor(preprocessor)
        return checkpoint

    def get_model(self, model: Optional[torch.nn.Module] = None) -> torch.nn.Module:
        """Retrieve the model stored in this checkpoint.

        Args:
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``. Otherwise, the model will be discarded.
        """
        with self.as_directory() as tempdir:
            model_path = Path(tempdir, self.MODEL_FILENAME).as_posix()
            if not os.path.exists(model_path):
                raise RuntimeError(
                    "`model.pt` not found within this checkpoint. Make sure you "
                    "created this `TorchCheckpoint` from one of its public "
                    "constructors (`from_state_dict` or `from_model`)."
                )
            model_or_state_dict = torch.load(model_path, map_location="cpu")

        if isinstance(model_or_state_dict, torch.nn.Module):
            if model:
                warnings.warn(
                    "TorchCheckpoint already contains all information needed. "
                    "Discarding provided `model` argument. If you are using "
                    "TorchPredictor directly, you should do "
                    "`TorchPredictor.from_checkpoint(checkpoint)` by removing kwargs "
                    "`model=`."
                )
        model = load_torch_model(
            saved_model=model_or_state_dict, model_definition=model
        )
        return model
