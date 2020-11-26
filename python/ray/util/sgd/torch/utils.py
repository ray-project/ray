import os
import logging

import ray
import torch.distributed as dist
from ray.util.sgd.utils import find_free_port

logger = logging.getLogger(__name__)


def setup_address():
    ip = ray.services.get_node_ip_address()
    port = find_free_port()
    return f"tcp://{ip}:{port}"


def setup_process_group(url, world_rank, world_size, timeout, backend="gloo"):
    """Connects the distributed PyTorch backend.

    Args:
        url (str): the URL used to connect to distributed PyTorch.
        world_rank (int): the index of the runner.
        world_size (int): the total number of runners.
        timeout (timedelta): Timeout for operations executed against
            the process group.
        backend (str): One of gloo or nccl. Depending on
            build-time configuration
    """
    logger.debug("Connecting to {} world_rank: {} world_size: {}".format(
        url, world_rank, world_size))
    logger.debug(f"using {backend}")
    if backend == "nccl" and "NCCL_BLOCKING_WAIT" not in os.environ:
        logger.debug(
            "Setting NCCL_BLOCKING_WAIT for detecting node failure. "
            "To override this behavior, you can set NCCL_BLOCKING_WAIT=0.")
        os.environ["NCCL_BLOCKING_WAIT"] = "1"

    dist.init_process_group(
        backend=backend,
        init_method=url,
        rank=world_rank,
        world_size=world_size,
        timeout=timeout)
