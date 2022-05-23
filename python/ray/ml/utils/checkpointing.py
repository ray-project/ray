import os
from pathlib import Path
from typing import Dict, Optional

import ray.cloudpickle as cpickle
from ray.ml import Checkpoint
from ray.ml.preprocessor import Preprocessor
from ray.ml.constants import PREPROCESSOR_KEY


def save_preprocessor_to_dir(
    preprocessor: Preprocessor,
    parent_dir: os.PathLike,
) -> os.PathLike:
    """Save preprocessor to file. Returns path saved to."""
    parent_dir = Path(parent_dir)
    with open(parent_dir.joinpath(PREPROCESSOR_KEY), "wb") as f:
        cpickle.dump(preprocessor, f)


def load_preprocessor_from_dir(
    parent_dir: os.PathLike,
) -> Optional[Preprocessor]:
    """Loads preprocessor from directory, if file exists."""
    parent_dir = Path(parent_dir)
    preprocessor_path = parent_dir.joinpath(PREPROCESSOR_KEY)
    if preprocessor_path.exists():
        with open(preprocessor_path, "rb") as f:
            preprocessor = cpickle.load(f)
    else:
        preprocessor = None
    return preprocessor


def create_compatible_checkpoint(
    checkpoint_dict: Dict, preprocessor: Optional[Preprocessor]
) -> Checkpoint:
    """Creates checkpoint as expected by Predictor to be used for batch inference
    or serve in a piecemeal fashion.

    Args:
        checkpoint_dict: Usually contains model weights and needs to be compatible with
            what is expected by the downstream Predictor to be used. For example,
            AIR native predictors like TorchPredictor and TensorflowPredictor
            expect {"model": model}.
        preprocessor: The preprocessing steps that data goes through before being fed
            into models in Predictor. Note, the preprocessor will need to be fitted
            beforehand if relevant.
    """
    checkpoint_dict.update({PREPROCESSOR_KEY: preprocessor})
    return Checkpoint.from_dict(checkpoint_dict)
