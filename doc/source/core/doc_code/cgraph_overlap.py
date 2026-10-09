# __cgraph_overlap_start__
import ray
import time
import torch
from ray.dag import InputNode, MultiOutputNode


@ray.remote(num_cpus=0, num_gpus=1)
class TorchTensorWorker:
    def send(self, shape, dtype, value: int, send_tensor=True):
        if not send_tensor:
            return 1
        return torch.ones(shape, dtype=dtype, device="cuda") * value

    def recv_and_matmul(self, two_d_tensor):
        """
        Receive the tensor and do some expensive computation (matmul).

        Args:
            two_d_tensor: a 2D tensor that has the same size for its dimensions
        """
        # Check that tensor got loaded to the correct device.
        assert two_d_tensor.dim() == 2
        assert two_d_tensor.size(0) == two_d_tensor.size(1)
        torch.matmul(two_d_tensor, two_d_tensor)
        return (two_d_tensor[0][0].item(), two_d_tensor.shape, two_d_tensor.dtype)


def test(overlap_gpu_communication):
    num_senders = 3
    senders = [TorchTensorWorker.remote() for _ in range(num_senders)]
    receiver = TorchTensorWorker.remote()

    shape = (10000, 10000)
    dtype = torch.float16

    with InputNode() as inp:
        branches = [sender.send.bind(shape, dtype, inp) for sender in senders]
        branches = [
            branch.with_tensor_transport(
                transport="nccl", _static_shape=True, _direct_return=True
            )
            # For a ray version before 2.42, use `with_type_hint()` instead.
            # branch.with_type_hint(
            #     TorchTensorType(
            #         transport="nccl", _static_shape=True, _direct_return=True
            #     )
            # )
            for branch in branches
        ]
        branches = [receiver.recv_and_matmul.bind(branch) for branch in branches]
        dag = MultiOutputNode(branches)

    compiled_dag = dag.experimental_compile(
        _overlap_gpu_communication=overlap_gpu_communication
    )

    start = time.monotonic()
    for i in range(5):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == [(i, shape, dtype)] * num_senders
    duration = time.monotonic() - start
    print(f"{overlap_gpu_communication=}, {duration=}")
    compiled_dag.teardown(kill_actors=True)


for overlap_gpu_communication in [False, True]:
    test(overlap_gpu_communication)
# __cgraph_overlap_end__
