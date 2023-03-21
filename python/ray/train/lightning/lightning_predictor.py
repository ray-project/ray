import logging
from typing import Optional, Type

from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.train.torch.torch_predictor import TorchPredictor
from ray.util.annotations import PublicAPI
import pytorch_lightning as pl

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class LightningPredictor(TorchPredictor):
    """A predictor for PyTorch Lightning modules.

    Args:
        model: The PyTorch Lightning module to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
        use_gpu: If set, the model will be moved to GPU on instantiation and
            prediction happens on GPU.
    """

    def __init__(
        self,
        model: pl.LightningModule,
        preprocessor: Optional["Preprocessor"] = None,
        use_gpu: bool = False,
    ):
        super(LightningPredictor, self).__init__(
            model=model, preprocessor=preprocessor, use_gpu=use_gpu
        )

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model_class: Type[pl.LightningModule],
        *,
        preprocessor: Optional[Preprocessor] = None,
        use_gpu: bool = False,
        **load_from_checkpoint_kwargs,
    ) -> "LightningPredictor":
        """Instantiate the LightningPredictor from a Checkpoint.

        The checkpoint is expected to be a result of ``LightningTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and preprocessor from.
                It is expected to be from the result of a ``LightningTrainer`` run.
            model_class: A subclass of ``pytorch_lightning.LightningModule`` that
                defines your model and training logic.
            preprocessor: A preprocessor used to transform data batches prior
                to prediction.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
            **load_from_checkpoint_kwargs: Arguments to pass into
                ``pl.LightningModule.load_from_checkpoint``
        """
        checkpoint = LightningCheckpoint.from_checkpoint(checkpoint)
        model = checkpoint.get_model(model_class, **load_from_checkpoint_kwargs)
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)
