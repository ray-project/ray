"""Test collective_rpc control plane API for Ray Serve LLM.

This test verifies that the DevIngress /collective_rpc endpoint works correctly
for RLHF-style weight synchronization workflows:

1. Server starts with worker_extension_cls for weight update methods
2. Trainer initializes NCCL process group with all inference workers
3. Trainer broadcasts weight updates to all workers via collective_rpc
4. Workers receive and apply the weight updates
5. Inference continues to work with updated weights

This demonstrates the core RLHF workflow where:
- Trainer and inference engine form a single NCCL communicator
- Weights are synchronized via high-bandwidth GPU-to-GPU transfer
- The /collective_rpc endpoint orchestrates the RPC across all replicas/workers

NOTE (Kourosh): This is part of a design in progress for integrating Ray Serve
LLM with RL workloads. The API is not public and won't be documented until the
end-to-end story is finalized. Class names and endpoint names may change.
"""

import time
from typing import List

import pytest
import ray
import requests
import torch
from openai import OpenAI
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.core.ingress.dev_ingress import build_dev_openai_app
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.serve.schema import ApplicationStatus
from transformers import AutoModelForCausalLM

MODEL_ID = "facebook/opt-125m"
BASE_URL = "http://localhost:8000"
TENSOR_PARALLEL_SIZE = 2
NUM_REPLICAS = 1


# =============================================================================
# Worker Extension for RLHF Weight Updates
# =============================================================================


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


# =============================================================================
# Trainer Actor
# =============================================================================


@ray.remote(num_gpus=1)
class TrainerActor:
    """Simulates a trainer that updates model weights via RLHF.

    This actor:
    1. Loads the same model as the inference engine
    2. Sets up an NCCL process group with all inference workers
    3. Broadcasts weight updates to all workers
    """

    def __init__(self, model_id: str):
        self.model_id = model_id
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
        self, method: str, args: List = None, kwargs: dict = None
    ):
        """Call the /collective_rpc endpoint synchronously."""
        url = f"{BASE_URL}/collective_rpc"
        data = {
            "model": self.model_id,
            "method": method,
            "args": args or [],
            "kwargs": kwargs or {},
        }
        response = requests.post(url, json=data, timeout=120)
        return response.json()


# =============================================================================
# Test Utilities
# =============================================================================


def get_llm_config() -> LLMConfig:
    """Create LLMConfig for collective_rpc testing."""
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=MODEL_ID,
        ),
        deployment_config=dict(
            num_replicas=NUM_REPLICAS,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=TENSOR_PARALLEL_SIZE,
            enforce_eager=True,
            enable_sleep_mode=True,
            # Worker extension for RLHF weight updates
            worker_extension_cls=f"{__name__}.WorkerExtension",
        ),
    )


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def wait_for_server_ready(timeout: int = 240) -> None:
    """Wait for the server to be ready to handle requests."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            test_data = {
                "model": MODEL_ID,
                "prompt": "Hello",
                "max_tokens": 5,
            }
            response = requests.post(
                f"{BASE_URL}/v1/completions", json=test_data, timeout=10
            )
            if response.status_code == 200:
                print(f"Server at {BASE_URL} is ready!")
                return
        except Exception as e:
            print(f"Waiting for server... ({e})")

        time.sleep(2)

    raise TimeoutError(f"Server not ready within {timeout} seconds")


def call_collective_rpc_sync(method: str, args: list = None) -> dict:
    """Synchronously call the /collective_rpc endpoint."""
    response = requests.post(
        f"{BASE_URL}/collective_rpc",
        json={
            "model": MODEL_ID,
            "method": method,
            "args": args or [],
        },
        timeout=60,
    )
    return response.json()


# =============================================================================
# Test
# =============================================================================


def test_collective_rpc_weight_sync():
    """Test the complete RLHF weight synchronization workflow."""
    # Initialize Ray
    ray.init(ignore_reinit_error=True)

    # Start Ray Serve with DevIngress
    llm_config = get_llm_config()
    app = build_dev_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    # Wait for application to be running
    wait_for_condition(is_default_app_running, timeout=300)
    wait_for_server_ready(timeout=240)

    trainer = None  # Initialize before try block to avoid NameError in finally
    try:
        # Step 1: Verify model serves requests before weight update
        print("\n=== Step 1: Verifying model serves requests before update ===")
        client = OpenAI(base_url=f"{BASE_URL}/v1", api_key="fake-key")
        response = client.completions.create(
            model=MODEL_ID,
            prompt="Hello, my name is",
            max_tokens=10,
            temperature=0,
        )
        assert response.choices[0].text is not None
        original_output = response.choices[0].text
        print(f"✓ Original output: {original_output!r}")

        # Step 2: Create trainer and set up weight sync group
        print("\n=== Step 2: Setting up trainer and NCCL process group ===")
        trainer = TrainerActor.remote(MODEL_ID)
        ray.get(
            trainer.setup_weight_sync_group.remote(
                tp_size=TENSOR_PARALLEL_SIZE,
                num_replicas=NUM_REPLICAS,
            )
        )
        print("✓ Weight sync group established")

        # Step 3: Broadcast weight updates (zero out weights)
        print("\n=== Step 3: Broadcasting weight updates ===")
        start_time = time.time()
        ray.get(trainer.update_weights.remote())
        elapsed = time.time() - start_time
        print(f"✓ Weight update completed in {elapsed:.2f}s")

        # Step 4: Verify weights changed on inference workers
        print("\n=== Step 4: Verifying weights changed on workers ===")
        result = call_collective_rpc_sync("check_weights_changed")
        print(f"check_weights_changed response: {result}")

        # Verify all workers report weights changed
        assert "results" in result, f"Expected 'results' in response: {result}"
        for replica_result in result["results"]:
            worker_results = replica_result.get("worker_results", [])
            for worker_result in worker_results:
                assert (
                    worker_result
                ), f"Worker reported weights not changed: {replica_result}"
        print("✓ All workers confirmed weights updated")

        # Step 5: Verify model still serves requests (with zeroed weights)
        print("\n=== Step 5: Verifying inference works with updated weights ===")
        response = client.completions.create(
            model=MODEL_ID,
            prompt="Hello, my name is",
            max_tokens=10,
            temperature=0,
        )
        assert response.choices[0].text is not None
        updated_output = response.choices[0].text
        print(f"✓ Output with zeroed weights: {updated_output!r}")

        # Output should be different since weights are now zero
        # (model produces garbage/different output)
        print(f"\nOriginal: {original_output!r}")
        print(f"Updated:  {updated_output!r}")

        print("\n=== All tests passed! ===")

    finally:
        # Cleanup
        if trainer is not None:
            ray.kill(trainer)
        serve.shutdown()
        ray.shutdown()
        time.sleep(1)


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
