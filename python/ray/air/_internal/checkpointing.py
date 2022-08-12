import os
from pathlib import Path
from typing import Optional, Union, TYPE_CHECKING

import ray.cloudpickle as cpickle
from ray.air.constants import PREPROCESSOR_KEY

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


def save_preprocessor_to_dir(
    preprocessor: "Preprocessor",
    parent_dir: Union[os.PathLike, str],
) -> None:
    """Save preprocessor to file. Returns path saved to."""
    parent_dir = Path(parent_dir)
    with open(parent_dir.joinpath(PREPROCESSOR_KEY), "wb") as f:
        cpickle.dump(preprocessor, f)


def load_preprocessor_from_dir(
    parent_dir: os.PathLike,
) -> Optional["Preprocessor"]:
    """Loads preprocessor from directory, if file exists."""
    parent_dir = Path(parent_dir)
    preprocessor_path = parent_dir.joinpath(PREPROCESSOR_KEY)
    if preprocessor_path.exists():
        with open(preprocessor_path, "rb") as f:
            preprocessor = cpickle.load(f)
    else:
        preprocessor = None
    return preprocessor
