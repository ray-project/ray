import logging
from typing import Optional, Type

from ray.data.preprocessor import Preprocessor
from ray.train.lightning._lightning_utils import import_lightning
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.train.torch.torch_predictor import TorchPredictor
from ray.util.annotations import Deprecated

pl = import_lightning()


logger = logging.getLogger(__name__)

LIGHTNING_PREDICTOR_DEPRECATION_MESSAGE = (
    "`LightningPredictor` is deprecated. For batch inference, "
    "see https://docs.ray.io/en/master/data/batch_inference.html"
    "for more details."
)


@Deprecated
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
        raise DeprecationWarning(LIGHTNING_PREDICTOR_DEPRECATION_MESSAGE)

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: LightningCheckpoint,
        model_class: Type[pl.LightningModule],
        *,
        preprocessor: Optional[Preprocessor] = None,
        use_gpu: bool = False,
        **load_from_checkpoint_kwargs
    ) -> "LightningPredictor":
        """Instantiate the LightningPredictor from a Checkpoint.

        The checkpoint is expected to be a result of ``LightningTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and preprocessor from.
                It is expected to be from the result of a ``LightningTrainer`` run.
            model_class: A subclass of ``pytorch_lightning.LightningModule`` that
                defines your model and training logic. Note that this is a class type
                instead of a model instance.
            preprocessor: A preprocessor used to transform data batches prior
                to prediction.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
            **load_from_checkpoint_kwargs: Arguments to pass into
                ``pl.LightningModule.load_from_checkpoint``.
        """

        model = checkpoint.get_model(
            model_class=model_class,
            **load_from_checkpoint_kwargs,
        )
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)
