import os
import sys
from typing import List

import torch


def launched_by_torchrun() -> bool:
    """Return True if this process looks like it came from `torchrun`."""
    env_markers = {
        "LOCAL_RANK",
        "LOCAL_WORLD_SIZE",
        "WORLD_SIZE",
        "TORCHELASTIC_RUN_ID",
    }  # torchrun ≥1.10
    argv_markers = (
        "--local-rank",
        "--local_rank",
    )  # torchrun always passes one of these

    # Any of the env vars *or* the CLI flag counts as evidence
    return bool(
        (env_markers & os.environ.keys())
        or any(a.startswith(argv_markers) for a in sys.argv)
    )


def local_running_get_devices() -> List[torch.device]:
    """Return a list of devices to use for training."""
    if torch.cuda.is_available():
        return [torch.device(f"cuda:{i}") for i in range(torch.cuda.device_count())]
    else:
        return [torch.device("cpu")]
