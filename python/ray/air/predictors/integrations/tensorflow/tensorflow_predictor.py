from typing import Callable, Optional, Union, Type, Dict

import numpy as np
import pandas as pd
import tensorflow as tf

from ray.air.predictor import Predictor, DataBatchType
from ray.air.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint
from ray.air.train.data_parallel_trainer import _load_checkpoint
from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type


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

    def _predict_pandas(
        self, data: "pd.DataFrame", dtype: Optional[tf.dtypes.DType] = None
    ) -> "pd.DataFrame":
        # TensorFlow model objects cannot be pickled, therefore we use
        # a callable that returns the model and initialize it here,
        # instead of having an initialized model object as an attribute.
        model = self.model_definition()

        def tensorize(numpy_array, dtype):
            return tf.convert_to_tensor(numpy_array, dtype=dtype)

        if len(data.columns) == 1:
            column_name = data.columns[0]
            if isinstance(dtype, dict):
                dtype = dtype[column_name]
            model_input = tensorize(
                convert_pandas_to_batch_type(data, np.ndarray), dtype
            )
        else:
            array_dict = convert_pandas_to_batch_type(data, type=dict)
            model_input = {
                k: tensorize(v, dtype=dtype[k] if isinstance(dtype, dict) else dtype)
                for k, v in array_dict.items()
            }

        if self.model_weights is not None:
            if not model.built:
                if not isinstance(model_input, dict):
                    input_shape = list(model_input.shape)
                    # The batch axis can contain varying number of elements, so we set
                    # the shape along the axis to `None`.
                    input_shape[0] = None

                    model.build(input_shape=input_shape)
                else:
                    raise RuntimeError(
                        "The provided Keras model definition does not "
                        "specify an input shape and the input shape "
                        "could not be inferred. You must specify the "
                        "input shape in your model."
                    )

            model.set_weights(self.model_weights)

        model.set_weights(self.model_weights)
        prediction = list(model(model_input).numpy())
        return pd.DataFrame({"predictions": prediction}, columns=["predictions"])

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
            dtype: The TensorFlow dtype to use for the tensors. Either a single dtype
                for all tensors or a mapping from column name to dtype.
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
            DataBatchType: Prediction result.
        """
        return super(TensorflowPredictor, self).predict(data=data, dtype=dtype)
