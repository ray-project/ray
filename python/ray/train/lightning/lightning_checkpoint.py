from typing import TYPE_CHECKING, Optional, Type, Union, Dict

from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from pytorch_lightning import LightningModule
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
class LightningCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with Lightning-specific
    functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``LightningCheckpoint.from_checkpoint(ckpt)``.
    """

    @classmethod
    def from_model(
        cls,
        model: "LightningModule",
        model_params: Dict,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightningCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a Lightning
        module.

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

    def get_model(
        self, model: Union[Type["LightningModule"], None]
    ) -> "LightningModule":
        """Retrieve the model stored in this checkpoint.

        If the checkpoint was created using
        ``LightningCheckpoint.from_checkpoint(ckpt)`` then ``model`` can be ``None``.
        If the checkpoint was created from the result of ``trainer.fit()`` then you
        should pass a reference to your LightningModule subclass.

        Args:
            model: The class to which the state dict will be loaded.
        """
        pass
