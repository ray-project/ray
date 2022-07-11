from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Type, Union

import numpy as np
import tensorflow as tf

from ray.air.checkpoint import Checkpoint
from ray.train.data_parallel_trainer import _load_checkpoint
from ray.train.predictor import DataBatchType
from ray.train._internal.dl_predictor import DLPredictor

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


class TensorflowPredictor(DLPredictor):
    """A predictor for TensorFlow models.

    Args:
        model_definition: A callable that returns a TensorFlow Keras model
            to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
        model_weights: List of weights to use for the model.
    """

    def __init__(
        self,
        model_definition: Union[Callable[[], tf.keras.Model], Type[tf.keras.Model]],
        preprocessor: Optional["Preprocessor"] = None,
        model_weights: Optional[list] = None,
    ):
        self.model_definition = model_definition
        self.model_weights = model_weights
        self.preprocessor = preprocessor

        # TensorFlow model objects cannot be pickled, therefore we use
        # a callable that returns the model and initialize it here,
        # instead of having an initialized model object as an attribute.
        # Predictors are not serializable (see the implementation of __reduce__) in the
        # Predictor class, so we can safely store the initialized model as an attribute.
        self._model = self.model_definition()

        if model_weights is not None:
            self._model.set_weights(model_weights)

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model_definition: Union[Callable[[], tf.keras.Model], Type[tf.keras.Model]],
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
        # Cannot use TensorFlow load_checkpoint here
        # due to instantiated models not being pickleable
        model_weights, preprocessor = _load_checkpoint(checkpoint, "TensorflowTrainer")
        return TensorflowPredictor(
            model_definition=model_definition,
            model_weights=model_weights,
            preprocessor=preprocessor,
        )

    def _array_to_tensor(
        self, numpy_array: np.ndarray, dtype: tf.dtypes.DType
    ) -> tf.Tensor:
        tf_tensor = tf.convert_to_tensor(numpy_array, dtype=dtype)

        # Off-the-shelf Keras Modules expect the input size to have at least 2
        # dimensions (batch_size, feature_size). If the tensor for the column
        # is flattened, then we unqueeze it to add an extra dimension.
        if len(tf_tensor.shape) == 1:
            tf_tensor = tf.expand_dims(tf_tensor, axis=1)
        return tf_tensor

    def _tensor_to_array(self, tensor: tf.Tensor) -> np.ndarray:
        return tensor.numpy()

    def _model_predict(
        self, tensor: Union[tf.Tensor, Dict[str, tf.Tensor]]
    ) -> Union[tf.Tensor, Dict[str, tf.Tensor], List[tf.Tensor], Tuple[tf.Tensor]]:

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
