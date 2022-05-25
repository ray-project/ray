import numpy as np
import os
import random
from typing import Optional

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf, try_import_torch


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
        torch, _ = try_import_torch()
        torch.manual_seed(seed)
        # See https://github.com/pytorch/pytorch/issues/47672.
        cuda_version = torch.version.cuda
        if cuda_version is not None and float(torch.version.cuda) >= 10.2:
            os.environ["CUBLAS_WORKSPACE_CONFIG"] = "4096:8"
        else:
            from distutils.version import LooseVersion

            if LooseVersion(torch.__version__) >= LooseVersion("1.8.0"):
                # Not all Operations support this.
                torch.use_deterministic_algorithms(True)
            else:
                torch.set_deterministic(True)
        # This is only for Convolution no problem.
        torch.backends.cudnn.deterministic = True
    elif framework == "tf2" or framework == "tfe":
        tf1, tf, _ = try_import_tf()
        # Tf2.x.
        if framework == "tf2":
            tf.random.set_seed(seed)
        # Tf-eager.
        elif framework == "tfe":
            tf1.set_random_seed(seed)
