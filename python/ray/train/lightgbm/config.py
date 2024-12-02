import logging
from dataclasses import dataclass

import ray
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig

logger = logging.getLogger(__name__)


NETWORK_PARAMS_KEY = "LIGHTGBM_NETWORK_PARAMS"
network_params = None


def set_network_params(
    num_machines: int,
    local_listen_port: int,
    machines: str,
):
    global network_params
    assert network_params is None, "LightGBM context is already initialized."
    network_params = dict(
        num_machines=num_machines,
        local_listen_port=local_listen_port,
        machines=machines,
    )


def get_network_params() -> dict:
    global network_params
    if network_params is None:
        logger.warning(
            "`ray.train.lightgbm.get_network_params` was called outside the "
            " context of a `ray.train.lightgbm.LightGBMTrainer`. "
            "The current process has no knowledge of the distributed training "
            "group, so returning an empty dict. Please call this within the "
            "training loop of a `ray.train.lightgbm.LightGBMTrainer`."
        )
        return {}
    return network_params.copy()


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
        self, worker_group: WorkerGroup, backend_config: LightGBMConfig
    ):
        node_ips_and_ports = worker_group.execute(get_address_and_port)
        ports = [port for _, port in node_ips_and_ports]
        machines = ",".join(
            [f"{node_ip}:{port}" for node_ip, port in node_ips_and_ports]
        )
        num_machines = len(worker_group)
        ray.get(
            [
                worker_group.execute_single_async(
                    rank, set_network_params, num_machines, ports[rank], machines
                )
                for rank in range(len(worker_group))
            ]
        )
