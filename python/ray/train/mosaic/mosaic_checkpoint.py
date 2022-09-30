from ray.air.checkpoint import Checkpoint
from pathlib import Path
import torch


def load_model_from_path(
    path: Path,
    model: torch.nn.Module,
    strict: bool = False,
):
    """
    This function loads a torch model from given saved checkpoint path. Note that
    the checkpoint object muse be one saved by composer's CheckpointSaver, as there
    are extra wrappers. If the saved checkpoint is a pure pytorch checkpoint file,
    then use functions provided by PyTorch or TorchCheckpoint instead.

    Args:
        path: Path to the saved checkpoint object. The checkpoint object must be one
            saved by composer's CheckpointSaver.
        model: the model to which the saved checkpoint will be loaded. This
            model should be a bare torch model, without any composer wrapping.
        strict: the boolean variable for strict state_dict loading onto the model
    """
    model_state_dict = torch.load(path)["state"]["model"]

    # remove module prefixes when loading the model weights
    # use this while loop instead of pytorch consume_prefix_in_state_dict_if_present
    # to check whether prefix has been removed.
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
    return model


class MosaicCheckpoint(Checkpoint):
    def get_model(
        self,
        model: torch.nn.Module,
        strict: bool = False,
    ) -> torch.nn.Module:
        """Retrieve the model stored in this checkpoint."""
        with self.to_dict()["all_checkpoints"][-1] as save_path:
            model = load_model_from_path(save_path, model, strict)
        return model
