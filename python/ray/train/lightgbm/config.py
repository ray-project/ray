import logging
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional

import ray
from ray._common.network_utils import build_address
from ray.train._internal.base_worker_group import BaseWorkerGroup
from ray.train._internal.utils import get_address_and_port
from ray.train.backend import Backend, BackendConfig

logger = logging.getLogger(__name__)


# Global LightGBM distributed network configuration for each worker process.
_lightgbm_network_params: Optional[Dict[str, Any]] = None
_lightgbm_network_params_lock = threading.Lock()


def get_network_params() -> Dict[str, Any]:
    """Returns the network parameters to enable LightGBM distributed training."""
    global _lightgbm_network_params

    with _lightgbm_network_params_lock:
        if not _lightgbm_network_params:
            logger.warning(
                "`ray.train.lightgbm.get_network_params` was called outside "
                "the context of a `ray.train.lightgbm.LightGBMTrainer`. "
                "The current process has no knowledge of the distributed training "
                "worker group, so this method will return an empty dict. "
                "Please call this within the training loop of a "
                "`ray.train.lightgbm.LightGBMTrainer`. "
                "If you are in fact calling this within a `LightGBMTrainer`, "
                "this is unexpected: please file a bug report to the Ray Team."
            )
            return {}

        return _lightgbm_network_params.copy()


def _set_network_params(
    num_machines: int,
    local_listen_port: int,
    machines: str,
):
    global _lightgbm_network_params

    with _lightgbm_network_params_lock:
        assert (
            _lightgbm_network_params is None
        ), "LightGBM network params are already initialized."
        _lightgbm_network_params = dict(
            num_machines=num_machines,
            local_listen_port=local_listen_port,
            machines=machines,
        )


@dataclass
class LightGBMConfig(BackendConfig):
    """Configuration for LightGBM distributed data-parallel training setup.

    See the LightGBM docs for more information on the "network parameters"
    that Ray Train sets up for you:
    https://lightgbm.readthedocs.io/en/latest/Parameters.html#network-parameters
    """

    @property
    def backend_cls(self):
        return _LightGBMBackend


class _LightGBMBackend(Backend):
    def on_training_start(
        self, worker_group: BaseWorkerGroup, backend_config: LightGBMConfig
    ):
        node_ips_and_ports = worker_group.execute(get_address_and_port)
        ports = [port for _, port in node_ips_and_ports]
        machines = ",".join(
            [build_address(node_ip, port) for node_ip, port in node_ips_and_ports]
        )
        num_machines = len(worker_group)
        ray.get(
            [
                worker_group.execute_single_async(
                    rank, _set_network_params, num_machines, ports[rank], machines
                )
                for rank in range(len(worker_group))
            ]
        )
