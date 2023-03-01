import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Type, Union

from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor

import numpy as np
import torch

from ray.air._internal.torch_utils import convert_ndarray_batch_to_torch_tensor_batch
from ray.train._internal.dl_predictor import DLPredictor
from ray.train.predictor import DataBatchType
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.util import log_once
from ray.util.annotations import DeveloperAPI, PublicAPI
import pytorch_lightning as pl

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class LightningPredictor(DLPredictor):
    """A predictor for PyTorch models.

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
        self.model = model
        self.model.eval()
        self.use_gpu = use_gpu

        self.device = torch.device("cpu")
        if use_gpu:
            # TODO (jiaodong): #26249 Use multiple GPU devices with sharded input
            self.device = torch.device("cuda")

        # Ensure input tensor and model live on the same device
        self.model.to(self.device)

        if (
            not use_gpu
            and torch.cuda.device_count() > 0
            and log_once("lightning_predictor_not_using_gpu")
        ):
            logger.warning(
                "You have `use_gpu` as False but there are "
                f"{torch.cuda.device_count()} GPUs detected on host where "
                "prediction will only use CPU. Please consider explicitly "
                "setting `LightningPredictor(use_gpu=True)` or "
                "`batch_predictor.predict(ds, num_gpus_per_worker=1)` to "
                "enable GPU prediction."
            )

        super().__init__(preprocessor)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(model={self.model!r}, "
            f"preprocessor={self._preprocessor!r}, use_gpu={self.use_gpu!r})"
        )

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model: pl.LightningModule,
        model_init_config: Dict["str", Any] = {},
        preprocessor: Optional[Preprocessor] = None,
        use_gpu: bool = False,
    ) -> "LightningPredictor":
        checkpoint = LightningCheckpoint.from_checkpoint(checkpoint)
        model = checkpoint.get_model(model, model_init_config)
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)

    @DeveloperAPI
    def call_model(
        self, inputs: Union[torch.Tensor, Dict[str, torch.Tensor]]
    ) -> Union[torch.Tensor, Dict[str, torch.Tensor]]:
        """Runs inference on a single batch of tensor data.

        This method is called by `LightningPredictor.predict` after converting the
        original data batch to torch tensors.

        Override this method to add custom logic for processing the model input or
        output.

        Args:
            inputs: A batch of data to predict on, represented as either a single
                PyTorch tensor or for multi-input models, a dictionary of tensors.

        Returns:
            The model outputs, either as a single tensor or a dictionary of tensors.

        """
        with torch.no_grad():
            output = self.model.predict_step(inputs, batch_idx=0)
        return output

    def predict(
        self,
        data: DataBatchType,
        dtype: Optional[Union[torch.dtype, Dict[str, torch.dtype]]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.
        """
        return super(LightningPredictor, self).predict(data=data, dtype=dtype)

    def _arrays_to_tensors(
        self,
        numpy_arrays: Union[np.ndarray, Dict[str, np.ndarray]],
        dtype: Optional[Union[torch.dtype, Dict[str, torch.dtype]]],
    ) -> Union[torch.Tensor, Dict[str, torch.Tensor]]:
        return convert_ndarray_batch_to_torch_tensor_batch(
            numpy_arrays,
            dtypes=dtype,
            device=self.device,
        )

    def _tensor_to_array(self, tensor: torch.Tensor) -> np.ndarray:
        if not isinstance(tensor, torch.Tensor):
            raise ValueError(
                "Expected the model to return either a torch.Tensor or a "
                f"dict of torch.Tensor, but got {type(tensor)} instead. "
                f"To support models with different output types, subclass "
                f"LightningPredictor and override the `call_model` method to "
                f"process the output into either torch.Tensor or Dict["
                f"str, torch.Tensor]."
            )
        return tensor.cpu().detach().numpy()
