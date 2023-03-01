from typing import TYPE_CHECKING, Any, Dict, Optional
import os
import pytorch_lightning as pl
from inspect import isclass


from ray.air.checkpoint import Checkpoint

from ray.util.annotations import PublicAPI


ENCODED_DATA_KEY = "torch_encoded_data"

@PublicAPI(stability="beta")
class LightningCheckpoint(Checkpoint):
    """A :class:`~ray.air.checkpoint.Checkpoint` with Lightning-specific functionality.

    Create this from a generic :class:`~ray.air.checkpoint.Checkpoint` by calling
    ``LightningCheckpoint.from_checkpoint(ckpt)``.

    The users will have no access to model in LightningTrainer. We only support file-based checkpoint.
    """

    def get_model(self, model_class: pl.LightningModule, model_init_config: Dict["str", Any]) -> pl.LightningModule:
        """Retrieve the model stored in this checkpoint.

        Args:
            model: The checkpoint contains a model state dict, and
            the state dict will be loaded to this ``model``.
        """
        if not isclass(model_class):
            raise ValueError(
                "'lightning_module' must be a class, not a class instance."
            )

        with self.as_directory() as checkpoint_dir:
            ckpt_path = os.path.join(checkpoint_dir, "checkpoint.ckpt")
            model = model_class.load_from_checkpoint(ckpt_path, **model_init_config)
        return model
