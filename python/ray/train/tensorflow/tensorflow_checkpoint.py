from typing import TYPE_CHECKING, Callable, Optional, Type, Union

import tensorflow as tf
from tensorflow import keras

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
class TensorflowCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with TensorFlow-specific
    functionality.
    """

    @classmethod
    def from_model(
        cls,
        model: keras.Model,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TensorflowCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a Keras 
        model.

        Args:
            model: The Keras model to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :py:class:`TensorflowCheckpoint` containing the specified model.

        Examples:
            >>> from ray.train.tensorflow import TensorflowCheckpoint
            >>> import tensorflow as tf
            >>>
            >>> model = tf.keras.applications.resnet.ResNet101()
            >>> checkpoint = TensorflowCheckpoint.from_model(model, path=".")

            You can use a :py:class:`TensorflowCheckpoint` to create an
            :py:class:`~ray.train.tensorflow.TensorflowPredictor` and preform inference.

            >>> from ray.train.tensorflow import TensorflowPredictor
            >>>
            >>> predictor = TensorflowPredictor.from_checkpoint(checkpoint)
        """
        checkpoint = cls.from_dict(
            {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model.get_weights()}
        )
        return checkpoint

    def get_model_weights(self) -> tf.keras.Model:
        """Retrieve the model weights stored in this checkpoint."""
        model_weights, _ = _load_checkpoint(self, "TensorflowTrainer")
        return model_weights
