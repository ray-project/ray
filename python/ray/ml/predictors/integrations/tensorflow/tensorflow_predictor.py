from typing import Callable, Optional, Union, List, Type

import pandas as pd
import tensorflow as tf

from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import MODEL_KEY, PREPROCESSOR_KEY


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
        checkpoint_dict = checkpoint.to_dict()
        preprocessor = checkpoint_dict.get(PREPROCESSOR_KEY, None)
        if MODEL_KEY not in checkpoint_dict:
            raise RuntimeError(
                f"No item with key: {MODEL_KEY} is found in the "
                f"Checkpoint. Make sure this key exists when saving the "
                f"checkpoint in ``TensorflowTrainer``."
            )
        model_weights = checkpoint_dict[MODEL_KEY]
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
            from ray.ml.predictors.tensorflow import TensorflowPredictor

            def build_model(self):
                return tf.keras.Sequential(
                    [
                        tf.keras.layers.InputLayer(input_shape=(1,)),
                        tf.keras.layers.Dense(1),
                    ]
                )

            predictor = TensorflowPredictor(model_definition=build_model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

            # Only use first column as the feature
            predictions = predictor.predict(data, feature_columns=[0])

        .. code-block:: python

            import pandas as pd
            import tensorflow as tf
            from ray.ml.predictors.tensorflow import TensorflowPredictor

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
            data = data.values
        else:
            data = data[:, feature_columns]

        tensor = tf.convert_to_tensor(data, dtype=dtype)

        # TensorFlow model objects cannot be pickled, therefore we use
        # a callable that returns the model and initialize it here,
        # instead of having an initialized model object as an attribute.
        model = self.model_definition()
        if self.model_weights:
            model.set_weights(self.model_weights)

        prediction = model(tensor).numpy().ravel()
        return pd.DataFrame(prediction, columns=["predictions"])
