import numpy as np

import ray
from ray.rllib.examples.envs.classes.simple_corridor import SimpleCorridor
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class GPURequiringEnv(SimpleCorridor):
    """A dummy env that requires a GPU in order to work.

    The env here is a simple corridor env that additionally simulates a GPU
    check in its constructor via `ray.get_gpu_ids()`. If this returns an
    empty list, we raise an error.

    To make this env work, use `num_gpus_per_env_runner > 0` (RolloutWorkers
    requesting this many GPUs each) and - maybe - `num_gpus > 0` in case
    your local worker/driver must have an env as well. However, this is
    only the case if `create_local_env_runner`=True (default is False).
    """

    def __init__(self, config=None):
        super().__init__(config)

        # Fake-require some GPUs (at least one).
        # If your local worker's env (`create_local_env_runner`=True) does not
        # necessarily require a GPU, you can perform the below assertion only
        # if `config.worker_index != 0`.
        gpus_available = ray.get_gpu_ids()
        print(f"{type(self).__name__} can see GPUs={gpus_available}")

        # Create a dummy tensor on the GPU.
        if len(gpus_available) > 0 and torch:
            self._tensor = torch.from_numpy(np.random.random_sample(size=(42, 42))).to(
                f"cuda:{gpus_available[0]}"
            )
