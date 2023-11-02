import logging
import os
import shutil
import tempfile
from inspect import isclass
from typing import Any, Dict, Optional, Type

from ray.air.constants import MODEL_KEY
from ray.data import Preprocessor
from ray.train._internal.framework_checkpoint import FrameworkCheckpoint
from ray.train.lightning._lightning_utils import import_lightning
from ray.util.annotations import Deprecated

pl = import_lightning()

logger = logging.getLogger(__name__)


LIGHTNING_CHECKPOINT_DEPRECATION_MESSAGE = (
    "`LightningCheckpoint` is deprecated. Please use `ray.train.Checkpoint`" "instead."
)


@Deprecated
class LightningCheckpoint(FrameworkCheckpoint):
    """A :class:`~ray.train.Checkpoint` with Lightning-specific functionality."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raise DeprecationWarning(LIGHTNING_CHECKPOINT_DEPRECATION_MESSAGE)

    @classmethod
    def from_path(
        cls,
        path: str,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightningCheckpoint":
        """Create a ``ray.train.lightning.LightningCheckpoint`` from a checkpoint file.

        Args:
            path: The file path to the PyTorch Lightning checkpoint file.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`LightningCheckpoint` containing the model.
        """

        assert os.path.exists(path), f"Lightning checkpoint {path} doesn't exists!"

        if os.path.isdir(path):
            raise ValueError(
                f"`from_path()` expects a file path, but `{path}` is a directory. "
                "A valid checkpoint file name is normally with .ckpt extension."
                "If you have a Ray checkpoint folder, you can also try to use "
                "`LightningCheckpoint.from_directory()` instead."
            )

        tempdir = tempfile.mkdtemp()
        new_checkpoint_path = os.path.join(tempdir, MODEL_KEY)
        shutil.copy(path, new_checkpoint_path)
        checkpoint = cls.from_directory(tempdir)
        if preprocessor:
            checkpoint.set_preprocessor(preprocessor)
        return checkpoint

    def get_model(
        self,
        model_class: Type[pl.LightningModule],
        **load_from_checkpoint_kwargs: Optional[Dict[str, Any]],
    ) -> pl.LightningModule:
        """Retrieve the model stored in this checkpoint.

        Args:
            model_class: A subclass of ``pytorch_lightning.LightningModule`` that
                defines your model and training logic.
            **load_from_checkpoint_kwargs: Arguments to pass into
                ``pl.LightningModule.load_from_checkpoint``.

        Returns:
            pl.LightningModule: An instance of the loaded model.
        """
        if not isclass(model_class):
            raise ValueError(
                "'model_class' must be a class, not an instantiated Lightning trainer."
            )

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
