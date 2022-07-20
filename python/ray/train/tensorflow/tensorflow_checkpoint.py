from typing import TYPE_CHECKING, Optional, Union, Callable, Type, Tuple

import tensorflow as tf
from tensorflow import keras

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


class TensorflowCheckpoint(Checkpoint):
    """Wrapper class that adds Tensorflow-specific accessors to a Checkpoint."""

    def __init__(self, checkpoint: Checkpoint):
        """Construct a `TensorflowCheckpoint` from a generic `Checkpoint` object."""
        self._clone_storage_from(checkpoint)

    @staticmethod
    def from_model(
        model: keras.Model,
        *,
        preprocessor: Optional["Preprocessor"] = None,
        **kwargs
    ) -> "TensorflowCheckpoint":
        """Create a (Tensorflow)Checkpoint from a Keras model.

        Args:
            model: A pretrained model.
            preprocessor: A fitted preprocessor. The preprocessing logic will
                be applied to the inputs for serving/inference.

        Returns:
            A checkpoint that can be loaded by TensorflowPredictor.
        """
        checkpoint = Checkpoint.from_dict(
            {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model.get_weights(), **kwargs}
        )
        return TensorflowCheckpoint(checkpoint)

    def get_model(
        self, model: Union[Callable[[], tf.keras.Model], Type[tf.keras.Model], tf.keras.Model],
    ) -> tf.keras.Model:
        """Return the model stored in this checkpoint.

        Args:
            model: A callable that returns a TensorFlow Keras model
                to use, or an instantiated model.
                Model weights will be loaded from the checkpoint.

        Returns:
            The model with set weights.
        """
        model_weights, _ = _load_checkpoint(self, "TensorflowTrainer")
        if isinstance(model, type) or callable(model):
            model = model()
        model.set_weights(model_weights)
        return model
