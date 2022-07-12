from typing import TYPE_CHECKING, Optional, Union, Callable, Type, Tuple

import tensorflow as tf
from tensorflow import keras

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


def to_air_checkpoint(
    model: keras.Model, preprocessor: Optional["Preprocessor"] = None
) -> Checkpoint:
    """Convert a pretrained model to AIR checkpoint for serve or inference.

    Args:
        model: A pretrained model.
        preprocessor: A fitted preprocessor. The preprocessing logic will
            be applied to serve/inference.
    Returns:
        A Ray Air checkpoint.
    """
    checkpoint = Checkpoint.from_dict(
        {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model.get_weights()}
    )
    return checkpoint


def load_checkpoint(
    checkpoint: Checkpoint,
    model: Union[Callable[[], tf.keras.Model], Type[tf.keras.Model], tf.keras.Model],
) -> Tuple[tf.keras.Model, Optional["Preprocessor"]]:
    """Load a Checkpoint from ``TensorflowTrainer``.

    Args:
        checkpoint: The checkpoint to load the model and
            preprocessor from. It is expected to be from the result of a
            ``TensorflowTrainer`` run.
        model: A callable that returns a TensorFlow Keras model
            to use, or an instantiated model.
            Model weights will be loaded from the checkpoint.

    Returns:
        The model with set weights and AIR preprocessor contained within.
    """
    model_weights, preprocessor = _load_checkpoint(checkpoint, "TensorflowTrainer")
    if isinstance(model, type) or callable(model):
        model = model()
    model.set_weights(model_weights)
    return model, preprocessor
