from pathlib import Path
import torch

from ray.train.mosaic.mosaic_checkpoint import MosaicCheckpoint
from ray.train.torch import TorchPredictor
from ray.train.mosaic.mosaic_checkpoint import load_model_from_path
import os
from typing import Union


class MosaicPredictor(TorchPredictor):
    """A TorchPredictor wrapper for Mosaic's Composer models.

    The state dicts saved by composer ``CheckpointSaver``s have extra
    wrapping around normal PyTorch models' state dict. As such, it is
    necessary to unwrap those to properly load models from saved checkpoints.
    Additionally, existing class methods of TorchPredictor does not allow
    directly using checkpoint path, but rather takes a ray checkpoint object,
    this wrapper provides creating a TorchPredictor from a checkpoint path.
    """

    @classmethod
    def from_save_path(
        cls,
        path: Union[str, Path],
        model: torch.nn.Module,
        use_gpu: bool = False,
        strict: bool = False,
    ):
        """
        This function creates a TorchPredictor from a saved Composer checkpoint.

        Args:
            path: Path to the saved checkpoint object
            model: the model to which the saved checkpoint will be loaded. This
                model should be a bare torch model, without any composer wrapping.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
            strict: the boolean variable for strict state_dict loading onto the model
        """
        model = load_model_from_path(path, model, strict)
        return cls(model=model, use_gpu=use_gpu)

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: MosaicCheckpoint,
        model: torch.nn.Module,
        use_gpu: bool = False,
        strict: bool = False,
    ):
        """This function creates a MosaicPredictor from a MosaicCheckpoint.

        Args:
            checkpoint: MosaicCheckpoint from which the save path will be loaded.
            model: the model to which the saved checkpoint will be loaded. This
                model should be a bare torch model, without any composer wrapping.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
            strict: the boolean variable for strict state_dict loading onto the model
        """
        checkpoint_dict = checkpoint.to_dict()
        save_path = os.path.join(
            checkpoint_dict["working_directory"], checkpoint_dict["all_checkpoints"][-1]
        )
        return cls.from_save_path(save_path, model, use_gpu, strict)
