import os
import sys
import torch

import pytest

import ray
import ray.cluster_utils
from ray.exceptions import RayChannelError, RayTaskError
from ray.experimental.channel.cpu_communicator import CPUCommunicator
from ray.dag import InputNode
import ray.experimental.collective as collective
from ray.dag.output_node import MultiOutputNode
from ray.tests.conftest import *  # noqa


@ray.remote
class CPUTorchTensorWorker:
    def __init__(self):
        self.device = torch.device(type="cpu")

    def send(self, shape, dtype, value: int, send_tensor=True):
        if not send_tensor:
            return 1
        return torch.ones(shape, dtype=dtype) * value

    def send_dict(self, entries):
        results = {}
        for key, entry in entries.items():
            value, shape, dtype = entry
            results[key] = torch.ones(shape, dtype=dtype) * value
        return results

    def send_or_raise(self, shape, dtype, value: int, raise_exception=False):
        if raise_exception:
            raise RuntimeError()
        return torch.ones(shape, dtype=dtype) * value

    def recv(self, tensor):
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)

    def recv_dict(self, tensor_dict):
        vals = {}
        for i, tensor in tensor_dict.items():
            assert tensor.device == self.device
            vals[i] = self.recv(tensor)
        return vals

    def compute_with_tuple_args(self, args, i: int):
        shape, dtype, value = args[i]
        tensor = torch.ones(shape, dtype=dtype) * value
        return tensor

    def recv_tensor(self, tensor):
        assert tensor.device == self.device
        return tensor

    def return_tensor(self, size: int) -> torch.Tensor:
        return torch.ones(size)


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p_basic(ray_start_cluster):
    sender = CPUTorchTensorWorker.remote()
    receiver = CPUTorchTensorWorker.remote()

    cpu_group = CPUCommunicator(2, [sender, receiver])

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport=cpu_group)
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_allreduce_basic(ray_start_cluster):
    num_workers = 2
    workers = [CPUTorchTensorWorker.remote() for _ in range(num_workers)]

    cpu_group = CPUCommunicator(num_workers, workers)

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, transport=cpu_group)
        recvs = [
            worker.recv.bind(collective)
            for worker, collective in zip(workers, collectives)
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        ref = compiled_dag.execute(
            [(shape, dtype, i + idx) for idx in range(num_workers)]
        )
        result = ray.get(ref)
        reduced_val = sum(i + idx for idx in range(num_workers))
        assert result == [(reduced_val, shape, dtype) for _ in workers]


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_allreduce_get_partial(ray_start_cluster):
    num_workers = 2
    workers = [CPUTorchTensorWorker.remote() for _ in range(num_workers)]

    cpu_group = CPUCommunicator(num_workers, workers)

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, transport=cpu_group)
        recv = workers[0].recv.bind(collectives[0])
        tensor = workers[1].recv_tensor.bind(collectives[0])
        dag = MultiOutputNode([recv, tensor, collectives[1]])

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(
            [(shape, dtype, i + idx + 1) for idx in range(num_workers)]
        )
        result = ray.get(ref)
        metadata, tensor, _ = result
        reduced_val = sum(i + idx + 1 for idx in range(num_workers))
        assert metadata == (reduced_val, shape, dtype)
        expected_tensor_val = torch.ones(shape, dtype=dtype) * reduced_val
        assert torch.equal(tensor, expected_tensor_val)


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_allreduce_wrong_shape(ray_start_cluster):
    """
    Test an error is thrown when the tensors in an all-reduce have different shapes.
    """
    num_workers = 2
    workers = [CPUTorchTensorWorker.remote() for _ in range(num_workers)]

    cpu_group = CPUCommunicator(num_workers, workers)

    dtype = torch.float16

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, transport=cpu_group)
        recvs = [
            worker.recv.bind(collective)
            for worker, collective in zip(workers, collectives)
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute([((20,), dtype, idx + 1) for idx in range(num_workers)])
    reduced_val = (1 + num_workers) * num_workers / 2
    assert ray.get(ref) == [(reduced_val, (20,), dtype) for _ in range(num_workers)]

    ref = compiled_dag.execute(
        [((10 * (idx + 1),), dtype, idx + 1) for idx in range(num_workers)]
    )
    # Execution hangs because of shape mismatch and a task error is raised.
    with pytest.raises(RayTaskError):
        ray.get(ref)

    # Since we have buffered channels, the execution should not error, but the
    # get should error, as the dag should no longer work after the application-
    # level exception.
    ref = compiled_dag.execute([((20,), dtype, 1) for _ in workers])
    with pytest.raises(RayChannelError):
        ray.get(ref)


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_allreduce_scheduling(ray_start_cluster):
    """
    Test scheduling avoids potential deadlocks that arise from all-reduce operations.

    inp --> x(0) --> +------------+
        |            | all-reduce |
        --> y(1) --> +------------+
        |
        --> t(0) --> recv(1)

    In the above graph, x, y, t are tensors, and the numbers inside parentheses
    identify the actors. If actor 1 launches an all-reduce with tensor y while
    actor 0 starts sending t, then actor 1 waits for actor 0 to join the all-reduce
    while actor 1 waits for actor 0 to receive t.
    """
    num_workers = 2
    workers = [CPUTorchTensorWorker.remote() for _ in range(num_workers)]

    cpu_group = CPUCommunicator(num_workers, workers)

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        # Tensors in the all-reduce.
        x = workers[0].send.bind(shape, dtype, inp)
        y = workers[1].send.bind(shape, dtype, inp)

        # Tensor to be sent from workers[0] to workers[1].
        t = workers[0].send.bind(shape, dtype, inp)
        t = t.with_tensor_transport(transport=cpu_group)

        collectives = collective.allreduce.bind([x, y], transport=cpu_group)
        recv = workers[1].recv.bind(t)
        dag = MultiOutputNode([collectives[0], collectives[1], recv])

    compiled_dag = dag.experimental_compile()

    value = 10
    ref = compiled_dag.execute(value)
    result = ray.get(ref)
    reduced_value = value * 2
    expected_tensor_val = torch.ones(shape, dtype=dtype) * reduced_value
    assert torch.equal(result[0], expected_tensor_val)
    assert torch.equal(result[1], expected_tensor_val)
    assert result[2] == (value, shape, dtype)


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_allreduce_duplicate_actors(ray_start_cluster):
    """
    Test an error is thrown when two input nodes from the same actor bind to
    an all-reduce.
    """
    num_workers = 2
    worker = CPUTorchTensorWorker.remote()

    cpu_group = CPUCommunicator(num_workers, [worker, worker])

    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for _ in range(2)]
        with pytest.raises(
            ValueError,
            match="Expected unique actor handles for a collective operation",
        ):
            collective.allreduce.bind(computes, transport=cpu_group)

    with InputNode() as inp:
        compute = worker.return_tensor.bind(inp)
        computes = [compute for _ in range(2)]
        with pytest.raises(
            ValueError,
            match="Expected unique input nodes for a collective operation",
        ):
            collective.allreduce.bind(computes, transport=cpu_group)


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_allreduce_wrong_actors(ray_start_cluster):
    """
    Test an error is thrown when an all-reduce binds to a wrong set of actors.
    """
    num_workers = 2
    workers = [CPUTorchTensorWorker.remote() for _ in range(num_workers * 2)]

    cpu_group = CPUCommunicator(num_workers, workers[:2])

    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers[2:]]
        with pytest.raises(
            ValueError,
            match="Expected actor handles to match the custom communicator group",
        ):
            collective.allreduce.bind(computes, transport=cpu_group)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
