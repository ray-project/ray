import torch
import ray
import requests
from typing import Optional
from transformers import AutoModelForCausalLM


def stateless_init_process_group(master_address, master_port, rank, world_size, device):
    """Create a stateless process group for NCCL communication.

    vLLM provides StatelessProcessGroup to create a process group
    without considering the global process group in torch.distributed.
    """
    from vllm.distributed.device_communicators.pynccl import PyNcclCommunicator
    from vllm.distributed.utils import StatelessProcessGroup

    pg = StatelessProcessGroup.create(
        host=master_address, port=master_port, rank=rank, world_size=world_size
    )
    pynccl = PyNcclCommunicator(pg, device=device)
    return pynccl


class WorkerExtension:
    """Extension class for vLLM workers to enable weight updates.

    This class is inherited by vLLM workers when worker_extension_cls is set.
    It provides methods for initializing NCCL process groups and receiving
    weight updates from an external trainer.
    """

    def init_weight_update_group(
        self, master_address, master_port, rank_offset, world_size
    ):
        """Initialize the NCCL process group for weight synchronization."""
        from vllm.distributed.parallel_state import get_world_group

        rank = get_world_group().rank + rank_offset
        self.model_update_group = stateless_init_process_group(
            master_address,
            master_port,
            rank,
            world_size,
            self.device,
        )

    def update_weight(self, name, dtype_name, shape):
        """Receive a weight tensor broadcast from the trainer."""
        dtype = getattr(torch, dtype_name)
        weight = torch.empty(shape, dtype=dtype, device="cuda")
        self.model_update_group.broadcast(
            weight, src=0, stream=torch.cuda.current_stream()
        )
        self.model_runner.model.load_weights(weights=[(name, weight)])
        del weight

    def check_weights_changed(self):
        """Check if weights have been updated to zero (for testing)."""
        weights_updated = True
        for name, p in self.model_runner.model.named_parameters():
            weights_updated = weights_updated and torch.allclose(p, torch.zeros_like(p))
        return weights_updated


@ray.remote(num_gpus=1)
class TrainerActor:
    """Simulates a trainer that updates model weights via RLHF.

    This actor:
    1. Loads the same model as the inference engine
    2. Sets up an NCCL process group with all inference workers
    3. Broadcasts weight updates to all workers
    """

    def __init__(self, model_id: str, base_url: str):
        self.model_id = model_id
        self._base_url = base_url
        self.weight_sync_group = None
        self.model = AutoModelForCausalLM.from_pretrained(model_id)
        self.model.to("cuda:0")

    def setup_weight_sync_group(
        self,
        tp_size: int,
        num_replicas: int,
    ):
        """Set up the NCCL process group between trainer and inference workers.

        Args:
            tp_size: Tensor parallel size of each replica
            num_replicas: Number of inference replicas
        """
        import concurrent.futures

        from vllm.utils.network_utils import get_ip, get_open_port

        # World size = 1 trainer + (tp_size * num_replicas) inference workers
        world_size = 1 + (tp_size * num_replicas)
        rank_offset = 1  # Inference workers start at rank 1

        master_address = get_ip()
        master_port = get_open_port()

        print(
            f"Setting up weight sync group: master={master_address}:{master_port}, "
            f"world_size={world_size}"
        )

        # Use ThreadPoolExecutor to run both operations concurrently
        # One thread calls the HTTP endpoint, another initializes local NCCL
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Start HTTP call to init weight update group on inference workers
            http_future = executor.submit(
                self._call_collective_rpc_sync,
                "init_weight_update_group",
                [master_address, master_port, rank_offset, world_size],
            )

            # Initialize trainer's side of the process group (rank 0)
            nccl_future = executor.submit(
                stateless_init_process_group,
                master_address,
                master_port,
                0,
                world_size,
                torch.device("cuda:0"),
            )

            # Wait for both to complete
            self.weight_sync_group = nccl_future.result(timeout=120)
            http_result = http_future.result(timeout=120)
            print(f"Weight sync group initialized. HTTP response: {http_result}")

    def update_weights(self):
        """Zero out all weights and broadcast to inference workers.

        In a real RLHF loop, this would broadcast the actual trained weights.
        For testing, we zero out the weights to verify the sync worked.
        """
        import concurrent.futures

        # Use a single ThreadPoolExecutor for all parameters to avoid
        # creating/destroying many thread pools (one per parameter)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            for name, p in self.model.named_parameters():
                # Zero out weights for testing
                p.data.zero_()
                dtype_name = str(p.dtype).split(".")[-1]

                # Start HTTP call to trigger update_weight on inference workers
                http_future = executor.submit(
                    self._call_collective_rpc_sync,
                    "update_weight",
                    [name, dtype_name, list(p.shape)],
                )

                # Broadcast the tensor via NCCL
                self.weight_sync_group.broadcast(
                    p, src=0, stream=torch.cuda.current_stream()
                )

                # Wait for HTTP call to complete before next parameter
                http_future.result(timeout=60)

        # Ensure all NCCL operations have completed
        torch.cuda.synchronize()

    def _call_collective_rpc_sync(
        self, method: str, args: Optional[list] = None, kwargs: Optional[dict] = None
    ):
        """Call the /collective_rpc endpoint synchronously."""
        url = f"{self._base_url}/collective_rpc"
        data = {
            "model": self.model_id,
            "method": method,
            "args": args or [],
            "kwargs": kwargs or {},
        }
        response = requests.post(url, json=data, timeout=120)
        return response.json()
