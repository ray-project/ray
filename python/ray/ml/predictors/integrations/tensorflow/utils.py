from typing import Optional

from tensorflow import keras

from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.ml.preprocessor import Preprocessor


def to_air_checkpoint(
    model: keras.Model, preprocessor: Optional[Preprocessor] = None
) -> Checkpoint:
    """Convert a pretrained model to AIR checkpoint for serve or inference.

    Args:
        model: A pretrained model.
        preprocessor: Stateless preprocessor only. The preprocessing logic will
            be applied to serve/inference.
        """
    checkpoint = Checkpoint.from_dict(
        {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model.get_weights()}
    )
    return checkpoint
