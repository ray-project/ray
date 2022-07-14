import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import numpy as np
import torch

from ray.util import log_once
from ray.train.predictor import DataBatchType
from ray.air.checkpoint import Checkpoint
from ray.train.torch.utils import load_checkpoint
from ray.train._internal.dl_predictor import DLPredictor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class TorchPredictor(DLPredictor):
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
        model: torch.nn.Module,
        preprocessor: Optional["Preprocessor"] = None,
        use_gpu: bool = False,
    ):
        self.model = model
        self.model.eval()
        self.preprocessor = preprocessor

        # TODO (jiaodong): #26249 Use multiple GPU devices with sharded input
        self.use_gpu = use_gpu
        if use_gpu:
            # Ensure input tensor and model live on GPU for GPU inference
            self.model.to(torch.device("cuda"))

        if (
            not use_gpu
            and torch.cuda.device_count() > 0
            and log_once("torch_predictor_not_using_gpu")
        ):
            logger.warning(
                "You have `use_gpu` as False but there are "
                f"{torch.cuda.device_count()} GPUs detected on host where "
                "prediction will only use CPU. Please consider explicitly "
                "setting `TorchPredictor(use_gpu=True)` or "
                "`batch_predictor.predict(ds, num_gpus_per_worker=1)` to "
                "enable GPU prediction."
            )

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model: Optional[torch.nn.Module] = None,
        use_gpu: bool = False,
    ) -> "TorchPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``TorchTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``TorchTrainer`` run.
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
        """
        model, preprocessor = load_checkpoint(checkpoint, model)
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)

    def _array_to_tensor(
        self, numpy_array: np.ndarray, dtype: torch.dtype
    ) -> torch.Tensor:
        torch_tensor = torch.from_numpy(numpy_array).to(dtype)
        if self.use_gpu:
            torch_tensor = torch_tensor.to(device="cuda")

        # Off-the-shelf torch Modules expect the input size to have at least 2
        # dimensions (batch_size, feature_size). If the tensor for the column
        # is flattened, then we unqueeze it to add an extra dimension.
        if len(torch_tensor.size()) == 1:
            torch_tensor = torch_tensor.unsqueeze(dim=1)

        return torch_tensor

    def _tensor_to_array(self, tensor: torch.Tensor) -> np.ndarray:
        return tensor.cpu().detach().numpy()

    def _model_predict(
        self, tensor: Union[torch.Tensor, Dict[str, torch.Tensor]]
    ) -> Union[
        torch.Tensor, Dict[str, torch.Tensor], List[torch.Tensor], Tuple[torch.Tensor]
    ]:
        with torch.no_grad():
            output = self.model(tensor)
        return output

    def predict(
        self,
        data: DataBatchType,
        dtype: Optional[Union[torch.dtype, Dict[str, torch.dtype]]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        If the provided data is a single array or a dataframe/table with a single
        column, it will be converted into a single PyTorch tensor before being
        inputted to the model.

        If the provided data is a multi-column table or a dict of numpy arrays,
        it will be converted into a dict of tensors before being inputted to the
        model. This is useful for multi-modal inputs (for example your model accepts
        both image and text).

        Args:
            data: A batch of input data of ``DataBatchType``.
            dtype: The dtypes to use for the tensors. Either a single dtype for all
                tensors or a mapping from column name to dtype.

        Examples:

        .. code-block:: python

            import numpy as np
            import torch
            from ray.train.torch import TorchPredictor

            model = torch.nn.Linear(2, 1)
            predictor = TorchPredictor(model=model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data, dtype=torch.float)

        .. code-block:: python

            import pandas as pd
            import torch
            from ray.train.torch import TorchPredictor

            class CustomModule(torch.nn.Module):
                def __init__(self):
                    super().__init__()
                    self.linear1 = torch.nn.Linear(1, 1)
                    self.linear2 = torch.nn.Linear(1, 1)

                def forward(self, input_dict: dict):
                    out1 = self.linear1(input_dict["A"])
                    out2 = self.linear2(input_dict["B"])
                    return out1 + out2

            predictor = TorchPredictor(model=CustomModule())

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])

            predictions = predictor.predict(data)

        Returns:
            DataBatchType: Prediction result. The return type will be the same as the
                input type.
        """
        return super(TorchPredictor, self).predict(data=data, dtype=dtype)
