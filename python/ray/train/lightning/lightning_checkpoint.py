import os
import pytorch_lightning as pl

from inspect import isclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class LightningCheckpoint(Checkpoint):
    """A :class:`~ray.air.checkpoint.Checkpoint` with Lightning-specific functionality.

    Create this from a generic :class:`~ray.air.checkpoint.Checkpoint` by calling
    ``LightningCheckpoint.from_checkpoint(ckpt)``.

    The users will have no access to model in LightningTrainer. We only support file-based checkpoint.
    """

    # TODO(yunxuanx): We cannot directly disable from_bytes() and from_dict(), the base Checkpoint class still needs them.
    #                 How to warn users not to use LightningCheckpoint.from_dict()?
    # @classmethod
    # def from_bytes(cls, data: bytes) -> "Checkpoint":
    #     raise NotImplementedError(
    #         "LightningCheckpoint doesn't support loading from_bytes()! Please use from_directory() or from_uri() instead.")

    # @classmethod
    # def from_dict(cls, data: dict) -> "Checkpoint":
    #     raise NotImplementedError(
    #         "LightningCheckpoint doesn't support loading from_dict()! Please use from_directory() or from_uri() instead.")

    def get_model(self, model_class: pl.LightningModule, model_init_config: Dict["str", Any] = {}) -> pl.LightningModule:
        """Retrieve the model stored in this checkpoint.

        Args:
            model_class: A class object (not a class instance) that is a subclass of ``pytorch_lightning.LightningModule``.
            model_init_config: Configurations to pass into ``model_class.__init__`` as kwargs.

        Returns:
            A :py:class:`pl.LightningModule` instance containing the loaded model.
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
