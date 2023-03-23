import os
import logging
import pytorch_lightning as pl
import tempfile
import shutil

from inspect import isclass
from typing import Optional, Type, Dict, Any

from ray.air.constants import MODEL_KEY
from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.data import Preprocessor
from ray.train.torch import TorchCheckpoint
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class LightningCheckpoint(TorchCheckpoint):
    """A :class:`~ray.air.checkpoint.Checkpoint` with Lightning-specific functionality.

    LightningCheckpoint only support file based checkpoint loading.
    Create this by calling ``LightningCheckpoint.from_directory(ckpt_dir)``,
    ``LightningCheckpoint.from_uri(uri)`` or ``LightningCheckpoint.from_path(path)``

    LightningCheckpoint loads file named ``model`` under the specified directory.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_dir = None

    @classmethod
    def from_path(
        cls,
        path: str,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightningCheckpoint":
        """Create a ``ray.air.lightning.LightningCheckpoint`` from a checkpoint path.

        Args:
            path: The file path to the PyTorch Lightning checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`LightningCheckpoint` containing the model.

        Examples:
            >>> from ray.train.lightning import LightningCheckpoint
            >>>
            >>> checkpoint = LightningCheckpoint.from_path( # doctest: +SKIP
            ...     path="/path/to/checkpoint.ckpt"
            ... )
        """

        assert os.path.exists(path), f"Lightning checkpoint {path} doesn't exists!"

        cache_dir = tempfile.mkdtemp()
        new_checkpoint_path = os.path.join(cache_dir, MODEL_KEY)
        shutil.copy(path, new_checkpoint_path)
        if preprocessor:
            save_preprocessor_to_dir(preprocessor, cache_dir)
        checkpoint = cls.from_directory(cache_dir)
        checkpoint._cache_dir = cache_dir
        return checkpoint

    def get_model(
        self,
        model_class: Type[pl.LightningModule],
        load_from_checkpoint_kwargs: Optional[Dict[str, Any]] = None,
    ) -> pl.LightningModule:
        """Retrieve the model stored in this checkpoint.

        Args:
            model_class: A subclass of ``pytorch_lightning.LightningModule`` that
                defines your model and training logic.
            load_from_checkpoint_kwargs: A dictionary of arguments to pass into
                ``pl.LightningModule.load_from_checkpoint``

        Returns:
            pl.LightningModule: An instance of the loaded model.
        """
        if not isclass(model_class):
            raise ValueError(
                "'model_class' must be a class, not an instantiated Lightning trainer."
            )

        if not load_from_checkpoint_kwargs:
            load_from_checkpoint_kwargs = {}

        with self.as_directory() as checkpoint_dir:
            ckpt_path = os.path.join(checkpoint_dir, MODEL_KEY)
            if not os.path.exists(ckpt_path):
                raise RuntimeError(
                    f"File {ckpt_path} not found under the checkpoint directory."
                )

            model = model_class.load_from_checkpoint(
                ckpt_path, **load_from_checkpoint_kwargs
            )
        return model

    def __del__(self):
        if self._cache_dir and os.path.exists(self._cache_dir):
            shutil.rmtree(self._cache_dir)
