import random
from typing import Optional

import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.torch_utils import set_torch_seed


@DeveloperAPI
def update_global_seed_if_necessary(
    framework: Optional[str] = None, seed: Optional[int] = None
) -> None:
    """Seed global modules such as random, numpy, torch, or tf.

    This is useful for debugging and testing.

    Args:
        framework: The framework specifier (may be None).
        seed: An optional int seed. If None, will not do
            anything.
    """
    if seed is None:
        return

    # Python random module.
    random.seed(seed)
    # Numpy.
    np.random.seed(seed)

    # Torch.
    if framework == "torch":
        set_torch_seed(seed=seed)
    elif framework == "tf2":
        tf1, tf, tfv = try_import_tf()
        # Tf2.x.
        if tfv == 2:
            tf.random.set_seed(seed)
        # Tf1.x.
        else:
            tf1.set_random_seed(seed)
