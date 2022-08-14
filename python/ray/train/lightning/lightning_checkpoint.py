from typing import TYPE_CHECKING, Optional, Type

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.lightning._lightning_utils import load_checkpoint
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
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightningCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a Lightning
        module.

        Args:
            model: The Torch model to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`TorchCheckpoint` containing the specified model.

        Examples:
            >>> from ray.train.lightning import LightningCheckpoint
            >>> from pytorch_lightning import LightningModule
            >>>
            >>> model = LightningModule()
            >>> checkpoint = LightningCheckpoint.from_model(model)
        """
        checkpoint = cls.from_dict({PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model})
        return checkpoint

    def get_model(self, model: Optional[Type["LightningModule"]] = None) -> "LightningModule":
        """Retrieve the model stored in this checkpoint.

        Args:
            model: The state dict will be loaded using the ``model`` class.
        """
        model, _ = load_checkpoint(self, model)
        return model
