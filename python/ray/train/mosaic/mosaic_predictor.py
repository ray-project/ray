from ray.train.torch import TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.air.checkpoint import Checkpoint
from pathlib import Path
import torch


class MosaicPredictor(BatchPredictor):
    """A BatchPredictor wrapper for Mosaic's Composer models.

    The state dicts saved by composer ``CheckpointSaver``s have extra
    wrapping around normal PyTorch models' state dict. As such, it is
    necessary to unwrap those to properly load models from saved checkpoints.
    Additionally, existing class methods of BatchPredictor does not allow
    directly using checkpoint path, but rather takes a ray checkpoint object,
    this wrapper provides creating a BatchPredictor from a checkpoint path.
    """

    @classmethod
    def from_save_path(
        cls,
        path: Path,
        model: torch.nn.Module,
        strict: bool = False,
        **predictor_kwargs
    ):
        """
        This function creates a BatchPredictor from a saved Composer checkpoint.
        Because Composer library is built on PyTorch, ``TorchPredictor`` is used
        as the ``predictor_cls``

        Args:
            path: Path to the saved checkpoint object
            model: the model to which the saved checkpoint will be loaded. This
                model should be a bare torch model, without any composer wrapping.
            strict: the boolean variable for strict state_dict loading onto the model
        """
        model_state_dict = torch.load(path)["state"]["model"]

        # remove module prefixes when loading the model weights
        while True:
            prefix_removed = False
            prefix = "module."
            keys = sorted(model_state_dict.keys())
            for key in keys:
                if key.startswith(prefix):
                    newkey = key[len(prefix) :]
                    model_state_dict[newkey] = model_state_dict.pop(key)
                    prefix_removed = True

            if not prefix_removed:
                break
        model.load_state_dict(model_state_dict, strict=strict)
        checkpoint = Checkpoint.from_dict({"model": model.state_dict()})
        return cls(
            checkpoint=checkpoint,
            predictor_cls=TorchPredictor,
            model=model,
            **predictor_kwargs
        )

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model: torch.nn.Module,
        strict: bool = False,
        **predictor_kwargs
    ):
        save_path = checkpoint.to_dict()["last_checkpoint"][-1]
        return cls.from_save_path(save_path, model, strict, **predictor_kwargs)
