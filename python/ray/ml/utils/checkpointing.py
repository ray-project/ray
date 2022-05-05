import os
from typing import Optional

import ray.cloudpickle as cpickle
from ray.ml.preprocessor import Preprocessor
from ray.ml.constants import PREPROCESSOR_KEY


def save_preprocessor_to_dir(
    preprocessor: Preprocessor,
    parent_dir: os.PathLike,
) -> os.PathLike:
    """Save preprocessor to file. Returns path saved to."""
    with open(os.path.join(parent_dir, PREPROCESSOR_KEY), "wb") as f:
        cpickle.dump(preprocessor, f)


def load_preprocessor_from_dir(
    parent_dir: os.PathLike,
) -> Optional[Preprocessor]:
    """Loads preprocessor from directory, if file exists."""
    preprocessor_path = os.path.join(parent_dir, PREPROCESSOR_KEY)
    if os.path.exists(preprocessor_path):
        with open(preprocessor_path, "rb") as f:
            preprocessor = cpickle.load(f)
    else:
        preprocessor = None
    return preprocessor
