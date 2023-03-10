import logging
from typing import Optional

from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.train.torch.torch_predictor import TorchPredictor
from ray.util.annotations import PublicAPI
import pytorch_lightning as pl

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class LightningPredictor(TorchPredictor):
    """A predictor for PyTorchLightning models.

    Args:
        model: The torch module to use for predictions.
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
        model: pl.LightningModule,
        *,
        preprocessor: Optional[Preprocessor] = None,
        use_gpu: bool = False,
        **load_from_checkpoint_kwargs,
    ) -> "LightningPredictor":
        checkpoint = LightningCheckpoint.from_checkpoint(checkpoint)
        model = checkpoint.get_model(model, **load_from_checkpoint_kwargs)
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)
