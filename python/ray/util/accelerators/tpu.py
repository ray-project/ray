from typing import Optional
from ray._private import accelerator


def pod_name() -> Optional[str]:
    """Return the name of the TPU pod that the worker is a part of.

    Returns:
      str: the name of the TPU pod. Returns None if not part of a TPU pod.

    """
    tpu_id = accelerator.get_tpu_id()
    if tpu_id == "":
        tpu_id = None
    return tpu_id


def pod_worker_count() -> int:
    """Count the number of workers associated with the TPU pod that the worker belongs to.

    Returns:
      int: the total number of workers in the TPU pod. Returns 0 if the worker is not
        part of a TPU pod.

    """
    return accelerator.num_workers_in_tpu_pod()
