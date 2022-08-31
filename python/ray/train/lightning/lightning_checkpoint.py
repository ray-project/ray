from typing import TYPE_CHECKING, Dict, Any, Optional, Type, Union

from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from pytorch_lightning import LightningModule
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
class LightningCheckpoint(Checkpoint):
    """A ``ray.air.checkpoint.Checkpoint`` with Lightning-specific functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``LightningCheckpoint.from_checkpoint(ckpt)``.
    """

    @classmethod
    def from_model(
        cls,
        model: "LightningModule",
        model_params: Dict[str, Any],
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightningCheckpoint":
        """Create a ``ray.air.checkpoint.Checkpoint`` that stores a LightningModule.

        Args:
            model: The LightningModule to store in the checkpoint.
            model_params: The kwargs to be passed to ``model``'s initialization.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`LightningCheckpoint` containing the specified model.

        Examples:
            >>> from ray.train.lightning import LightningCheckpoint
            >>> from pytorch_lightning import LightningModule
            >>>
            >>> model = LightningModule()
            >>> checkpoint = LightningCheckpoint.from_model(model)
        """
        pass

    @classmethod
    def from_path(
        cls,
        path: str,
        model_cls: Type[LightningModule],
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightningCheckpoint":
        """Create a ``ray.air.checkpoint.Checkpoint`` from a Lightning checkpoint.

        Args:
            path: The file path to the Lightning checkpoint.
            model_cls: The LightningModule to use when loading the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`LightningCheckpoint` containing the model.

        Examples:
            >>> from ray.train.lightning import LightningCheckpoint
            >>> from pytorch_lightning import LightningModule
            >>>
            >>> checkpoint = LightningCheckpoint.from_path("/path/to/checkpoint.ckpt", LightningModule)
        """
        pass

    def get_model(
        self, model: Union[Type["LightningModule"], None]
    ) -> "LightningModule":
        """Retrieve the model stored in this checkpoint.

        If the checkpoint was created with a ``"model_cls"`` key then the ``model``
        argument can be ``None``. If the checkpoint was created from the result of
        ``trainer.fit()`` then you should pass a reference to your LightningModule
        subclass.

        Args:
            model: The class to which the state dict will be loaded.
        """
        pass
