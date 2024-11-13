import os
import sys
import torch

import pytest

import ray
import ray.cluster_utils
import ray.experimental.collective as collective
from ray.exceptions import RayChannelError
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.conftest import start_nccl_mock
from ray.experimental.channel.cpu_nccl_group import CPUNcclGroup
from ray.tests.conftest import *  # noqa
from ray.tests.conftest import wait_for_condition
from ray.dag import InputNode, MultiOutputNode

def error_logged(capsys, msg):
    out, err = capsys.readouterr()
    # Write captured back to stdout, stderr for easier test debugging.
    sys.stdout.write(out)
    sys.stderr.write(err)
    return msg in err


@ray.remote
class MockedWorker:
    def __init__(self):
        self.chan = None
        self.device = 'cpu'
    
    def start_mock(self):
        """
        Patch methods that require CUDA.
        """
        start_nccl_mock()

    def send(self, shape, dtype, value: int, send_as_dict=False):
        if send_as_dict:
            return self.send_dict([(value, value, shape, dtype)])

        return torch.ones(shape, dtype=dtype) * value

    def recv(self, tensor):
        if isinstance(tensor, dict):
            assert len(tensor) == 1
            tensor = list(tensor.values())[0]

        return (tensor[0].item(), tensor.shape, tensor.dtype)

    def send_dict(self, entries):
        results = {}
        for key, value, shape, dtype in entries:
            results[key] = torch.ones(shape, dtype=dtype) * value
        return results

    def recv_dict(self, tensor_dict):
        results = []
        for key in sorted(tensor_dict.keys()):
            tensor = tensor_dict[key]
            results.append((key, tensor[0].item(), tensor.shape, tensor.dtype))
        return results

    def return_tensor(self, size: int):
        return torch.ones(size, device=self.device)

    def compute_with_tuple_args(self, args, i: int):
        shape, dtype, value = args[i]
        tensor = torch.ones(shape, dtype=dtype, device=self.device) * value
        return tensor

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
def test_p2p(ray_start_cluster):
    """
    Test simple sender -> receiver pattern. Check that receiver receives
    correct results.
    """
    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()
    nccl_group = CPUNcclGroup(world_size=2, rank=0, actor_handles=[sender, receiver])

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0], inp.send_as_dict)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype, send_as_dict=False)
        assert ray.get(ref) == (i, shape, dtype)

    # Sending tensors of different shape also works.
    for i in range(3):
        ref = compiled_dag.execute(i, shape=(20,), dtype=dtype, send_as_dict=False)
        assert ray.get(ref) == (i, (20,), dtype)

    # Sending tensors inside a dictionary also works.
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype, send_as_dict=True)
        assert ray.get(ref) == (i, shape, dtype)

    compiled_dag.teardown()


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
@pytest.mark.parametrize("send_as_dict", [True, False])
def test_p2p_static_shape(ray_start_cluster, send_as_dict):
    """
    Test simple send -> recv pattern with
    _static_shape=True. If sender always sends tensors of
    the same shape, then it works.
    """
    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()
    nccl_group = CPUNcclGroup(world_size=2, rank=0, actor_handles=[sender, receiver])

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0], send_as_dict=send_as_dict)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group, _static_shape=True))
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
def test_p2p_direct_return(ray_start_cluster):
    """
    Test simple sender -> receiver pattern with _direct_return=True
    """
    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()
    nccl_group = CPUNcclGroup(world_size=2, rank=0, actor_handles=[sender, receiver])

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_as_dict)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group, _direct_return=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    dtype = torch.float16
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(
            shape=shape, dtype=dtype, value=i, send_as_dict=False
        )
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
@pytest.mark.parametrize("check_static_shape", [True, False])
def test_p2p_static_shape_and_direct_return(
    capsys, ray_start_cluster, check_static_shape
):
    """
    Test simple sender -> receiver pattern with both _static_shape=True and
    _direct_return=True. Check errors are thrown if tensors with wrong shape
    are passed (check_static_shape=True) OR if non-tensor value is returned
    (check_static_shape=False).
    """
    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()
    nccl_group = CPUNcclGroup(world_size=2, rank=0, actor_handles=[sender, receiver])

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_as_dict)
        dag = dag.with_type_hint(
            TorchTensorType(transport=nccl_group, _static_shape=True, _direct_return=True)
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    shape = (10,)
    dtype = torch.float16
    for i in range(3):
        ref = compiled_dag.execute(
            shape=shape, dtype=dtype, value=i, send_as_dict=False
        )
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
def test_allreduce(ray_start_cluster):
    """
    Test basic all-reduce.
    """
    world_size = 2
    actors = [MockedWorker.remote() for _ in range(world_size)]
    nccl_group = CPUNcclGroup(world_size=world_size, rank=None, actor_handles=actors)

    ray.get([actor.start_mock.remote() for actor in actors])

    with InputNode() as inp:
        computes = [actor.return_tensor.bind(inp) for actor in actors]
        coll = collective.allreduce.bind(computes, transport=nccl_group)
        dag = MultiOutputNode(coll)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        res = ray.get(compiled_dag.execute(i))
        assert res == torch.ones(i) * world_size

if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))