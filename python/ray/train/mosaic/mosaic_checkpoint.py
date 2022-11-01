import os
import torch

from ray.air.checkpoint import Checkpoint


class MosaicCheckpoint(Checkpoint):
    """A subclass of AIR Checkpoint for Composer models.
    When reporting checkpoint in the Composer model training loop, a native composer
    checkpoint file is loaded and converted to a ``MosaicCheckpoint`` object, which is
    then reported via Ray.
    Loading the saved model via ``MosaicCheckpoint`` involves removing the ``module``
    prefix added via PyTorch DDP.
    """

    def get_model(
        self,
        model: torch.nn.Module,
        strict: bool = False,
    ) -> torch.nn.Module:
        """Retrieve the model stored in this checkpoint."""
        checkpoint_dict = self.to_dict()
        model_state_dict = checkpoint_dict["state"]["model"]
        torch.nn.modules.utils.consume_prefix_in_state_dict_if_present(
            model_state_dict, "module."
        )
        model.load_state_dict(model_state_dict, strict=strict)
        return model

    @classmethod
    def from_path(cls, path: str):
        composer_state_dict = torch.load(path)
        return cls.from_dict(composer_state_dict)

    def to_path(self, path: str):
        with open(path, "wb") as f:
            torch.save(self.to_dict(), f)
        return os.path.join(os.getcwd(), path)
