import logging
from typing import Optional, Type, Dict, Any

from ray.data.preprocessor import Preprocessor
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.train.torch.torch_predictor import TorchPredictor
from ray.util.annotations import PublicAPI
import pytorch_lightning as pl

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class LightningPredictor(TorchPredictor):
    """A predictor for PyTorch Lightning modules.

    Example:
        .. testcode:: python

            import torch
            import numpy as np
            import pytorch_lightning as pl
            from ray.train.lightning import LightningPredictor


            class MyModel(pl.LightningModule):
                def __init__(self, input_dim, output_dim):
                    super().__init__()
                    self.linear = torch.nn.Linear(input_dim, output_dim)

                def forward(self, x):
                    out = self.linear(x)
                    return out

                def training_step(self, batch, batch_idx):
                    x, y = batch
                    y_hat = self.forward(x)
                    loss = torch.nn.functional.mse_loss(y_hat, y)
                    self.log("train_loss", loss)
                    return loss

                def configure_optimizers(self):
                    optimizer = torch.optim.Adam(self.parameters(), lr=1e-3)
                    return optimizer


            batch_size, input_dim, output_dim = 10, 3, 5
            model = MyModel(input_dim=input_dim, output_dim=output_dim)
            predictor = LightningPredictor(model=model, use_gpu=False)
            batch = np.random.rand(batch_size, input_dim).astype(np.float32)

            # Internally, LightningPredictor.predict()  invokes the forward() method
            # of the model to generate predictions
            output = predictor.predict(batch)

            assert output["predictions"].shape == (batch_size, output_dim)


        .. testoutput::
            :hide:
            :options: +ELLIPSIS

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
        checkpoint: LightningCheckpoint,
        model_class: Type[pl.LightningModule],
        *,
        preprocessor: Optional[Preprocessor] = None,
        use_gpu: bool = False,
        load_from_checkpoint_kwargs: Optional[Dict[str, Any]] = None
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
            load_from_checkpoint_kwargs: A dictionary of arguments to pass into
                ``pl.LightningModule.load_from_checkpoint``
        """
        if not load_from_checkpoint_kwargs:
            load_from_checkpoint_kwargs = {}

        model = checkpoint.get_model(
            model_class=model_class,
            load_from_checkpoint_kwargs=load_from_checkpoint_kwargs,
        )
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)
