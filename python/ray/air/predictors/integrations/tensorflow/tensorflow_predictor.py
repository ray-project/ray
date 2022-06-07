from typing import Callable, Optional, Union, List, Type

import pandas as pd
import tensorflow as tf

from ray.air.predictor import Predictor, DataBatchType
from ray.air.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint
from ray.air.train.data_parallel_trainer import _load_checkpoint
from ray.air._internal.tensorflow_utils import convert_pandas_to_tf_tensor


class TensorflowPredictor(Predictor):
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
        preprocessor: Optional[Preprocessor] = None,
        model_weights: Optional[list] = None,
    ):
        self.model_definition = model_definition
        self.model_weights = model_weights
        self.preprocessor = preprocessor

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

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        dtype: Optional[tf.dtypes.DType] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a TensorFlow Tensor before being inputted to
        the model.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, then use
                all columns in ``data``.
            dtype: The TensorFlow dtype to use when creating the TensorFlow tensor.
                If set to None, then automatically infer the dtype.

        Examples:

        .. code-block:: python

            import numpy as np
            import tensorflow as tf
            from ray.air.predictors.tensorflow import TensorflowPredictor

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
            from ray.air.predictors.tensorflow import TensorflowPredictor

            def build_model(self):
                return tf.keras.Sequential(
                    [
                        tf.keras.layers.InputLayer(input_shape=(1,)),
                        tf.keras.layers.Dense(1),
                    ]
                )

            predictor = TensorflowPredictor(model_definition=build_model)

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])

            predictions = predictor.predict(data)

            # Only use first column as the feature
            predictions = predictor.predict(data, feature_columns=["A"])


        Returns:
            DataBatchType: Prediction result.
        """
        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if isinstance(data, pd.DataFrame):
            if feature_columns:
                data = data[feature_columns]
            tensor = convert_pandas_to_tf_tensor(data, dtype=dtype)
        else:
            tensor = tf.convert_to_tensor(data, dtype=dtype)

        # TensorFlow model objects cannot be pickled, therefore we use
        # a callable that returns the model and initialize it here,
        # instead of having an initialized model object as an attribute.
        model = self.model_definition()

        if self.model_weights is not None:
            input_shape = list(tensor.shape)
            # The batch axis can contain varying number of elements, so we set
            # the shape along the axis to `None`.
            input_shape[0] = None

            model.build(input_shape=input_shape)
            model.set_weights(self.model_weights)

        prediction = list(model(tensor).numpy())
        return pd.DataFrame({"predictions": prediction}, columns=["predictions"])
