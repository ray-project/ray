import logging
from typing import TYPE_CHECKING, Callable, Dict, Optional, Type, Union

import numpy as np
import tensorflow as tf

from ray.util import log_once
from ray.train.predictor import DataBatchType
from ray.air.checkpoint import Checkpoint
from ray.air._internal.tensorflow_utils import convert_ndarray_batch_to_tf_tensor_batch
from ray.train._internal.dl_predictor import DLPredictor
from ray.train.tensorflow.tensorflow_checkpoint import TensorflowCheckpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class TensorflowPredictor(DLPredictor):
    """A predictor for TensorFlow models.

    Args:
        model_definition: A callable that returns a TensorFlow Keras model
            to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
        model_weights: List of weights to use for the model.
        use_gpu: If set, the model will be moved to GPU on instantiation and
            prediction happens on GPU.
    """

    def __init__(
        self,
        model_definition: Union[Callable[[], tf.keras.Model], Type[tf.keras.Model]],
        preprocessor: Optional["Preprocessor"] = None,
        model_weights: Optional[list] = None,
        use_gpu: bool = False,
    ):
        self.model_definition = model_definition
        self.model_weights = model_weights

        self.use_gpu = use_gpu
        # TensorFlow model objects cannot be pickled, therefore we use
        # a callable that returns the model and initialize it here,
        # instead of having an initialized model object as an attribute.
        # Predictors are not serializable (see the implementation of __reduce__)
        # in the Predictor class, so we can safely store the initialized model
        # as an attribute.
        if use_gpu:
            # TODO (jiaodong): #26249 Use multiple GPU devices with sharded input
            with tf.device("GPU:0"):
                self._model = self.model_definition()
        else:
            self._model = self.model_definition()
            gpu_devices = tf.config.list_physical_devices("GPU")
            if len(gpu_devices) > 0 and log_once("tf_predictor_not_using_gpu"):
                logger.warning(
                    "You have `use_gpu` as False but there are "
                    f"{len(gpu_devices)} GPUs detected on host where "
                    "prediction will only use CPU. Please consider explicitly "
                    "setting `TensorflowPredictor(use_gpu=True)` or "
                    "`batch_predictor.predict(ds, num_gpus_per_worker=1)` to "
                    "enable GPU prediction."
                )

        if model_weights is not None:
            self._model.set_weights(model_weights)
        super().__init__(preprocessor)

    def __repr__(self):
        fn_name = getattr(self.model_definition, "__name__", self.model_definition)
        return (
            f"{self.__class__.__name__}("
            f"model_definition={fn_name}, "
            f"preprocessor={self._preprocessor!r}, "
            f"model_weights={self.model_weights!r}, "
            f"use_gpu={self.use_gpu!r})"
        )

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model_definition: Union[Callable[[], tf.keras.Model], Type[tf.keras.Model]],
        use_gpu: bool = False,
    ) -> "TensorflowPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``TensorflowTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``TensorflowTrainer`` run.
            model_definition: A callable that returns a TensorFlow Keras model
                to use. Model weights will be loaded from the checkpoint.
        """
        checkpoint = TensorflowCheckpoint.from_checkpoint(checkpoint)
        model_weights = checkpoint.get_model_weights()
        preprocessor = checkpoint.get_preprocessor()
        return cls(
            model_definition=model_definition,
            model_weights=model_weights,
            preprocessor=preprocessor,
            use_gpu=use_gpu,
        )

    def call_model(
        self, tensor: Union[tf.Tensor, Dict[str, tf.Tensor]]
    ) -> Union[tf.Tensor, Dict[str, tf.Tensor]]:
        """Runs inference on a single batch of tensor data.

        This method is called by `TorchPredictor.predict` after converting the
        original data batch to torch tensors.

        Override this method to add custom logic for processing the model input or
        output.

        Example:

            .. code-block:: python

                # List outputs are not supported by default TensorflowPredictor.
                def build_model() -> tf.keras.Model:
                    input = tf.keras.layers.Input(shape=1)
                    model = tf.keras.models.Model(inputs=input, outputs=[input, input])
                    return model

                # Use a custom predictor to format model output as a dict.
                class CustomPredictor(TensorflowPredictor):
                    def call_model(self, tensor):
                        model_output = super().call_model(tensor)
                        return {
                            str(i): model_output[i] for i in range(len(model_output))
                        }

                predictor = CustomPredictor(model_definition=build_model)
                predictions = predictor.predict(data_batch)

        Args:
            tensor: A batch of data to predict on, represented as either a single
                PyTorch tensor or for multi-input models, a dictionary of tensors.

        Returns:
            The model outputs, either as a single tensor or a dictionary of tensors.

        """
        if self.use_gpu:
            with tf.device("GPU:0"):
                return self._model(tensor)
        else:
            return self._model(tensor)

    def predict(
        self,
        data: DataBatchType,
        dtype: Optional[Union[tf.dtypes.DType, Dict[str, tf.dtypes.DType]]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        If the provided data is a single array or a dataframe/table with a single
        column, it will be converted into a single Tensorflow tensor before being
        inputted to the model.

        If the provided data is a multi-column table or a dict of numpy arrays,
        it will be converted into a dict of tensors before being inputted to the
        model. This is useful for multi-modal inputs (for example your model accepts
        both image and text).

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            dtype: The dtypes to use for the tensors. Either a single dtype for all
                tensors or a mapping from column name to dtype.

        Examples:

        .. code-block:: python

            import numpy as np
            import tensorflow as tf
            from ray.train.predictors.tensorflow import TensorflowPredictor

            def build_model(self):
                return tf.keras.Sequential(
                    [
                        tf.keras.layers.InputLayer(input_shape=(2,)),
                        tf.keras.layers.Dense(1),
                    ]
                )

            predictor = TensorflowPredictor(model_definition=build_model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

        .. code-block:: python

            import pandas as pd
            import tensorflow as tf
            from ray.train.predictors.tensorflow import TensorflowPredictor

            def build_model(self):
                input1 = tf.keras.layers.Input(shape=(1,), name="A")
                input2 = tf.keras.layers.Input(shape=(1,), name="B")
                merged = keras.layers.Concatenate(axis=1)([input1, input2])
                output = keras.layers.Dense(2, input_dim=2)(merged)
                return keras.models.Model(inputs=[input1, input2], output=output)

            predictor = TensorflowPredictor(model_definition=build_model)

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])

            predictions = predictor.predict(data)

        Returns:
            DataBatchType: Prediction result. The return type will be the same as the
                input type.
        """
        return super(TensorflowPredictor, self).predict(data=data, dtype=dtype)

    def _arrays_to_tensors(
        self,
        numpy_arrays: Union[np.ndarray, Dict[str, np.ndarray]],
        dtypes: Union[tf.dtypes.DType, Dict[str, tf.dtypes.DType]],
    ) -> Union[tf.Tensor, Dict[str, tf.Tensor]]:
        return convert_ndarray_batch_to_tf_tensor_batch(numpy_arrays, dtypes=dtypes)

    def _tensor_to_array(self, tensor: tf.Tensor) -> np.ndarray:
        if not isinstance(tensor, tf.Tensor):
            raise ValueError(
                "Expected the model to return either a tf.Tensor or a "
                f"dict of tf.Tensor, but got {type(tensor)} instead. "
                f"To support models with different output types, subclass "
                f"TensorflowPredictor and override the `call_model` method "
                f"to process the output into either torch.Tensor or Dict["
                f"str, torch.Tensor]."
            )
        return tensor.numpy()
