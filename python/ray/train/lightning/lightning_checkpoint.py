import os
import logging
import pytorch_lightning as pl

from inspect import isclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class LightningCheckpoint(Checkpoint):
    """A :class:`~ray.air.checkpoint.Checkpoint` with Lightning-specific functionality.

    LightningCheckpoint only support file based checkpoint loading. Create this by calling
    ``LightningCheckpoint.from_directory(ckpt_dir)`` or ``LightningCheckpoint.from_uri(uri)``.

    LightningCheckpoint loads file ``checkpoint.ckpt`` under the specified directory.
    """

    def get_model(self, model_class: Type[pl.LightningModule], model_init_config: Dict["str", Any] = {}) -> pl.LightningModule:
        """Retrieve the model stored in this checkpoint.

        Args:
            model_class: A subclass of ``pytorch_lightning.LightningModule`` that defines your model and training logic.
            model_init_config: Configurations to pass into ``model_class.__init__`` as kwargs.

        Returns:
            pl.LightningModule: An instance of the loaded model.
        """
        if not isclass(model_class):
            raise ValueError(
                "'lightning_module' must be a class, not a class instance."
            )

        with self.as_directory() as checkpoint_dir:
            ckpt_path = os.path.join(checkpoint_dir, "checkpoint.ckpt")
            if not os.path.exists(ckpt_path):
                raise RuntimeError(
                    f"File checkpoint.ckpt not found under the checkpoint directory."
                )
            model = model_class.load_from_checkpoint(ckpt_path, **model_init_config)
        return model
