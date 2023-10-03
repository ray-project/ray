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
from ray.train._internal.framework_checkpoint import FrameworkCheckpoint
from ray.train.torch import LegacyTorchCheckpoint
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class LightningCheckpoint(FrameworkCheckpoint):
    """A :class:`~ray.train.Checkpoint` with Lightning-specific functionality."""

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

        Example:
            .. testcode::

                import pytorch_lightning as pl
                from ray.train.lightning import LightningCheckpoint, LightningPredictor

                class MyLightningModule(pl.LightningModule):
                    def __init__(self, input_dim, output_dim) -> None:
                        super().__init__()
                        self.linear = nn.Linear(input_dim, output_dim)
                        self.save_hyperparameters()

                    # ...

                # After the training is finished, LightningTrainer saves
                # checkpoints in the result directory, for example:
                # ckpt_dir = "{storage_path}/LightningTrainer_.*/checkpoint_000000"

                # You can load model checkpoint with model init arguments
                def load_checkpoint(ckpt_dir):
                    ckpt = LightningCheckpoint.from_directory(ckpt_dir)

                    # `get_model()` takes the argument list of
                    # `LightningModule.load_from_checkpoint()` as additional kwargs.
                    # Please refer to PyTorch Lightning API for more details.

                    return checkpoint.get_model(
                        model_class=MyLightningModule,
                        input_dim=32,
                        output_dim=10,
                    )

                # You can also load checkpoint with a hyperparameter file
                def load_checkpoint_with_hparams(
                    ckpt_dir, hparam_file="./hparams.yaml"
                ):
                    ckpt = LightningCheckpoint.from_directory(ckpt_dir)
                    return ckpt.get_model(
                        model_class=MyLightningModule,
                        hparams_file=hparam_file
                    )

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


@PublicAPI(stability="alpha")
class LegacyLightningCheckpoint(LegacyTorchCheckpoint):
    """A :class:`~ray.air.checkpoint.Checkpoint` with Lightning-specific functionality.

    LightningCheckpoint only support file based checkpoint loading.
    Create this by calling ``LightningCheckpoint.from_directory(ckpt_dir)``,
    ``LightningCheckpoint.from_uri(uri)`` or ``LightningCheckpoint.from_path(path)``

    LightningCheckpoint loads file named ``model`` under the specified directory.

    Examples:

    .. testcode::
        :skipif: True

        from ray.train.lightning import LightningCheckpoint

        # Suppose we saved a checkpoint in "./checkpoint_000000/model":
        # Option 1 (Preferred): Load from the checkpoint file
        checkpoint = LightningCheckpoint.from_path(
            path="./checkpoint_00000/model"
        )

        # Option 2: Load from a directory
        checkpoint = LightningCheckpoint.from_directory(
            path="./checkpoint_00000/"
        )

        # Suppose we saved a checkpoint in an S3 bucket:
        # Option 3: Load from URI
        checkpoint = LightningCheckpoint.from_uri(
            path="s3://path/to/checkpoint/directory/"
        )


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
        """Create a ``ray.air.lightning.LightningCheckpoint`` from a checkpoint file.

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

    def __del__(self):
        if self._cache_dir and os.path.exists(self._cache_dir):
            shutil.rmtree(self._cache_dir)
