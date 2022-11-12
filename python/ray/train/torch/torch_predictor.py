import logging
from typing import TYPE_CHECKING, Dict, Optional, Union

import numpy as np
import torch

from ray.util import log_once
from ray.train.predictor import DataBatchType
from ray.air.checkpoint import Checkpoint
from ray.air._internal.torch_utils import convert_ndarray_batch_to_torch_tensor_batch
from ray.train.torch.torch_checkpoint import TorchCheckpoint
from ray.train._internal.dl_predictor import DLPredictor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
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
                ``model``. If the checkpoint already contains the model itself,
                this model argument will be discarded.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
        """
        checkpoint = TorchCheckpoint.from_checkpoint(checkpoint)
        model = checkpoint.get_model(model)
        preprocessor = checkpoint.get_preprocessor()
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)

    def call_model(
        self, tensor: Union[torch.Tensor, Dict[str, torch.Tensor]]
    ) -> Union[torch.Tensor, Dict[str, torch.Tensor]]:
        """Runs inference on a single batch of tensor data.

        This method is called by `TorchPredictor.predict` after converting the
        original data batch to torch tensors.

        Override this method to add custom logic for processing the model input or
        output.

        Args:
            tensor: A batch of data to predict on, represented as either a single
                PyTorch tensor or for multi-input models, a dictionary of tensors.

        Returns:
            The model outputs, either as a single tensor or a dictionary of tensors.

        Example:

            .. testcode::

                # List outputs are not supported by default TorchPredictor.
                # So let's define a custom TorchPredictor and override call_model
                class MyModel(torch.nn.Module):
                    def forward(self, input_tensor):
                        return [input_tensor, input_tensor]

                # Use a custom predictor to format model output as a dict.
                class CustomPredictor(TorchPredictor):
                    def call_model(self, tensor):
                        model_output = super().call_model(tensor)
                        return {
                            str(i): model_output[i] for i in range(len(model_output))
                        }

                # create our data batch
                data_batch = np.array([1, 2])
                # create custom predictor and predict
                predictor = CustomPredictor(model=MyModel())
                predictions = predictor.predict(data_batch)
                print(f"Predictions: {predictions.get('0')}, {predictions.get('1')}")

            .. testoutput::

                Predictions: [1 2], [1 2]
        """
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

        Returns:
            DataBatchType: Prediction result. The return type will be the same as the
                input type.

        Example:

            .. testcode::

                    import numpy as np
                    import pandas as pd
                    import torch
                    import ray
                    from ray.train.torch import TorchPredictor

                    # Define a custom PyTorch module
                    class CustomModule(torch.nn.Module):
                        def __init__(self):
                            super().__init__()
                            self.linear1 = torch.nn.Linear(1, 1)
                            self.linear2 = torch.nn.Linear(1, 1)

                        def forward(self, input_dict: dict):
                            out1 = self.linear1(input_dict["A"].unsqueeze(1))
                            out2 = self.linear2(input_dict["B"].unsqueeze(1))
                            return out1 + out2

                    # Set manul seed so we get consistent output
                    torch.manual_seed(42)

                    # Use Standard PyTorch model
                    model = torch.nn.Linear(2, 1)
                    predictor = TorchPredictor(model=model)
                    # Define our data
                    data = np.array([[1, 2], [3, 4]])
                    predictions = predictor.predict(data, dtype=torch.float)
                    print(f"Standard model predictions: {predictions}")
                    print("---")

                    # Use Custom PyTorch model with TorchPredictor
                    predictor = TorchPredictor(model=CustomModule())
                    # Define our data and predict Customer model with TorchPredictor
                    data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
                    predictions = predictor.predict(data, dtype=torch.float)
                    print(f"Custom model predictions: {predictions}")

            .. testoutput::

                Standard model predictions: {'predictions': array([[1.5487633],
                       [3.8037925]], dtype=float32)}
                ---
                Custom model predictions:     predictions
                0  [0.61623406]
                1    [2.857038]
        """
        return super(TorchPredictor, self).predict(data=data, dtype=dtype)

    def _arrays_to_tensors(
        self,
        numpy_arrays: Union[np.ndarray, Dict[str, np.ndarray]],
        dtypes: Union[torch.dtype, Dict[str, torch.dtype]],
    ) -> Union[torch.Tensor, Dict[str, torch.Tensor]]:
        return convert_ndarray_batch_to_torch_tensor_batch(
            numpy_arrays,
            dtypes=dtypes,
            device="cuda" if self.use_gpu else None,
        )

    def _tensor_to_array(self, tensor: torch.Tensor) -> np.ndarray:
        if not isinstance(tensor, torch.Tensor):
            raise ValueError(
                "Expected the model to return either a torch.Tensor or a "
                f"dict of torch.Tensor, but got {type(tensor)} instead. "
                f"To support models with different output types, subclass "
                f"TorchPredictor and override the `call_model` method to "
                f"process the output into either torch.Tensor or Dict["
                f"str, torch.Tensor]."
            )
        return tensor.cpu().detach().numpy()
