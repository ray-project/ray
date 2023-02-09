from typing import TYPE_CHECKING, Any, Dict, Optional
import io

import torch
import warnings

from torch.nn import Module

import ray.cloudpickle
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint_dict
from ray.air._internal.torch_utils import (
    load_torch_model,
    consume_prefix_in_state_dict_if_present_not_in_place,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

ENCODED_DATA_KEY = "torch_encoded_data"


@PublicAPI(stability="beta")
class TorchCheckpoint(Checkpoint):
    """A :class:`~ray.air.checkpoint.Checkpoint` with Torch-specific functionality.

    Create this from a generic :class:`~ray.air.checkpoint.Checkpoint` by calling
    ``TorchCheckpoint.from_checkpoint(ckpt)``.
    """

    # Special encoding logic to avoid serialization errors with torch.
    def _encode_data_dict(self, data_dict: dict) -> dict:
        """Encode data_dict using torch.save."""

        for k, v in data_dict.items():
            # Only check for attribute as we want to support
            # DDP, FSDP and any future approaches
            if isinstance(v, Module) and hasattr(v, "module"):
                data_dict[k] = v.module
            elif isinstance(v, dict):
                # We could limit this only to the MODEL_KEY, but we'd
                # miss any extra user-specified keys. This should be a
                # noop with anything but DDP/FSDP module state dicts.
                data_dict[k] = consume_prefix_in_state_dict_if_present_not_in_place(
                    v, "module."
                )

        # Convert the checkpoint dict to bytes, so that any GPU tensors that
        # are in the checkpoint dict can be properly deserialized on the
        # driver side, even if the driver does not have access to a GPU device.
        _buffer = io.BytesIO()
        torch.save(data_dict, _buffer, pickle_module=ray.cloudpickle)
        return {ENCODED_DATA_KEY: _buffer.getvalue()}

    def _decode_data_dict(self, data_dict: dict) -> dict:
        """Decode data_dict using torch_load if needed."""
        if ENCODED_DATA_KEY not in data_dict:
            return data_dict
        encoded_data = data_dict[ENCODED_DATA_KEY]
        _buffer = io.BytesIO(encoded_data)
        data_dict = torch.load(
            _buffer,
            map_location="cpu"
            # Not using ray.cloudpickle here as it doesn't
            # define an Unpickler (as it is not necessary).
        )
        return data_dict

    def __getstate__(self) -> dict:
        if self._data_dict:
            state = self.__dict__.copy()
            state["_data_dict"] = self._encode_data_dict(self._data_dict)
            return state
        return super().__getstate__()

    def __setstate__(self, state: dict):
        if "_data_dict" in state:
            state = state.copy()
            state["_data_dict"] = self._decode_data_dict(state["_data_dict"])
        super().__setstate__(state)

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
                :options: +ELLIPSIS

                ...
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
            `Saving and Loading Models <https://pytorch.org/tutorials/beginner/saving_loading_models.html#what-is-a-state-dict>`_. # noqa: E501

        Args:
            model: The Torch model to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :class:`TorchCheckpoint` containing the specified model.

        Examples:

            .. testcode::

                from ray.train.torch import TorchCheckpoint
                from ray.train.torch import TorchPredictor
                import torch

                # Set manual seed
                torch.manual_seed(42)

                # Create model identity and send a random tensor to it
                model = torch.nn.Identity()
                input = torch.randn(2, 2)
                output = model(input)

                # Create a checkpoint
                checkpoint = TorchCheckpoint.from_model(model)

                # You can use a class TorchCheckpoint to create an
                # a class ray.train.torch.TorchPredictor and perform inference.
                predictor = TorchPredictor.from_checkpoint(checkpoint)
                pred = predictor.predict(input.numpy())

                # Convert prediction dictionary value into a tensor
                pred = torch.tensor(pred['predictions'])

                # Assert the output from the original and checkoint model are the same
                assert torch.equal(output, pred)
                print("worked")

            .. testoutput::
                :hide:
                :options: +ELLIPSIS

                ...
        """
        return cls.from_dict({PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model})

    def get_model(self, model: Optional[torch.nn.Module] = None) -> torch.nn.Module:
        """Retrieve the model stored in this checkpoint.

        Args:
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``. Otherwise, the model will be discarded.
        """
        saved_model, _ = _load_checkpoint_dict(self, "TorchTrainer")

        if isinstance(saved_model, torch.nn.Module):
            if model:
                warnings.warn(
                    "TorchCheckpoint already contains all information needed. "
                    "Discarding provided `model` argument. This means: "
                    "If you are using BatchPredictor, you should do "
                    "`BatchPredictor.from_checkpoint(checkpoint, TorchPredictor)` by"
                    "removing kwargs `model=`. "
                    "If you are using TorchPredictor directly, you should do "
                    "`TorchPredictor.from_checkpoint(checkpoint)` by removing kwargs "
                    "`model=`."
                )
        model = load_torch_model(saved_model=saved_model, model_definition=model)
        return model
