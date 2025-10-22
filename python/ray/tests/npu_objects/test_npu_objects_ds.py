'''
# DSTensorTransport UT:

import pytest
import torch
import ray
from unittest.mock import patch, MagicMock
from ray.util.collective.collective_group.ds_backend import DSBackend
from ray.experimental.collective.ds_tensor_transport import DSTensorTransport
from ray.util.collective.types import (
    DS_GROUP_NAME,
    DSTransportMetadata,
    DSCommunicatorMetadata
)

class TestDSTensorTransport:
    @pytest.fixture
    def mock_backend(self):
        """创建 mock backend fixture"""
        backend = DSBackend()
        with patch('ray.util.collective.collective.get_group_handle') as mock_get:
            mock_get.return_value = backend
            yield backend

    def test_extract_metadata(self, mock_backend):
        """测试元数据提取"""
        tensors = [torch.randn(10, 10), torch.randn(5, 5)]

        metadata = DSTensorTransport.extract_tensor_transport_metadata(tensors)

        # 验证 metadata 结构
        assert len(metadata.tensor_meta) == 2
        assert metadata.tensor_meta[0] == (torch.Size([10, 10]), tensors[0].dtype)
        assert metadata.ds_serialized_keys is not None

    def test_recv_multiple_tensors(self, mock_backend):
        """测试接收操作"""
        tensors = [torch.zeros(10, 10), torch.zeros(5, 5)]

        metadata = DSTransportMetadata(
            tensor_meta=[(t.shape, t.dtype) for t in tensors],
            tensor_device=torch.device('cpu'),
            ds_serialized_keys=['key1', 'key2']
        )

        comm_metadata = DSCommunicatorMetadata(communicator_name=DS_GROUP_NAME)

        DSTensorTransport.recv_multiple_tensors(tensors, metadata, comm_metadata)

        # 验证 mock backend 的 recv 被调用,tensors 被填充为 1
        assert torch.all(tensors[0] == 1)
        assert torch.all(tensors[1] == 1)

    def test_garbage_collect(self, mock_backend):
        """测试垃圾回收"""
        metadata = DSTransportMetadata(
            tensor_meta=[],
            tensor_device=None,
            ds_serialized_keys=['key1', 'key2']
        )

        # 应该调用 deregister_memory 而不抛出异常
        DSTensorTransport.garbage_collect(metadata)

    def test_actor_has_tensor_transport(self, mock_backend):
        """测试 actor 是否有 tensor transport"""

        mock_actor = MagicMock()
        mock_actor.__ray_call__ = MagicMock()
        mock_actor.__ray_call__.options = MagicMock(
            return_value=MagicMock(
                remote=MagicMock(return_value=ray.put(True))
            )
        )

        transport = DSTensorTransport()
        result = transport.actor_has_tensor_transport(mock_actor)

        assert result == True
        mock_actor.__ray_call__.options.assert_called_with(concurrency_group="_ray_system")

    def test_get_tensor_transport_metadata(self, mock_backend):
        """测试获取 tensor transport metadata"""

        expected_metadata = DSTransportMetadata(
            tensor_meta=[(torch.Size([10, 10]), torch.float32)],
            tensor_device=torch.device('cpu'),
            ds_serialized_keys=['key1']
        )

        mock_actor = MagicMock()
        mock_actor.__ray_call__ = MagicMock()
        mock_actor.__ray_call__.options = MagicMock(
            return_value=MagicMock(
                remote=MagicMock(return_value=ray.put(expected_metadata))
            )
        )

        result_ref = DSTensorTransport.get_tensor_transport_metadata(mock_actor, "test_obj_id")
        result = ray.get(result_ref)

        assert result.tensor_meta == expected_metadata.tensor_meta
        assert result.ds_serialized_keys == expected_metadata.ds_serialized_keys
        mock_actor.__ray_call__.options.assert_called_with(concurrency_group="_ray_system")

# 端到端测试

import pytest
import torch
import ray
import sys

@ray.remote(num_cpus=0.5, enable_tensor_transport=True)
class DSTestActor:
    def __init__(self):
        # 在 actor 初始化时注册 mock backend
        from ray.util.collective.collective import _group_mgr
        from ray.util.collective.collective_group.ds_backend import DSBackend
        from ray.util.collective.types import DS_GROUP_NAME

        mock_backend = DSBackend()
        _group_mgr._name_group_map[DS_GROUP_NAME] = mock_backend
        _group_mgr._group_name_map[mock_backend] = DS_GROUP_NAME

    @ray.method(tensor_transport="ds")
    def echo(self, data):
        return data

    def sum(self, data):
        return data.sum().item()

def test_ds_metadata_flow(ray_start_regular):
    """测试从 driver 到 actor 的元数据流程"""
    actors = [DSTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    tensor = torch.randn(10, 10)
    ref = src_actor.echo.remote(tensor)
    result = ray.get(dst_actor.sum.remote(ref))

    # 由于 mock backend 的 recv 会将 tensor 填充为 1
    # 所以结果应该是 100 (10x10 的全 1 tensor)
    assert result == 100

def test_p2p_with_mock_ds(ray_start_regular):
    """测试 P2P 传输，使用 mock DS backend"""
    actors = [DSTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    tensor = torch.tensor([1, 2, 3])
    ref = src_actor.echo.remote(tensor)

    # dstActor 接收并处理
    # 由于 mock backend 的 recv 会填充为 1
    # 所以 sum 应该返回 3 (3个1的和)
    result = ray.get(dst_actor.sum.remote(ref))
    assert result == 3

'''
import sys

import pytest
import torch

import ray


@ray.remote(num_cpus=0.5, enable_tensor_transport=True)
class DSTestActor:
    def __init__(self):
        # 在 actor 初始化时注册 mock backend
        from ray.util.collective.collective import _group_mgr
        from ray.util.collective.collective_group.ds_backend import DSBackend
        from ray.util.collective.types import DS_GROUP_NAME

        mock_backend = DSBackend()
        _group_mgr._name_group_map[DS_GROUP_NAME] = mock_backend
        _group_mgr._group_name_map[mock_backend] = DS_GROUP_NAME

    @ray.method(tensor_transport="ds")
    def echo(self, data):
        # 由于使用 CPU 测试，不需要 .to(device)
        return data

    def sum(self, data):
        # mock backend 的 recv 会将 tensor 填充为 1
        return data.sum().item()

    def produce(self, tensors):
        refs = []
        for t in tensors:
            refs.append(ray.put(t, _tensor_transport="ds"))
        return refs

    def consume_with_ds(self, refs):
        tensors = [ray.get(ref) for ref in refs]
        sum_val = 0
        for t in tensors:
            assert t.device.type == "cpu"
            sum_val += t.sum().item()
        return sum_val

    def consume_with_object_store(self, refs):
        tensors = [ray.get(ref, _tensor_transport="object_store") for ref in refs]
        sum_val = 0
        for t in tensors:
            assert t.device.type == "cpu"
            sum_val += t.sum().item()
        return sum_val

    def gc(self):
        tensor = torch.tensor([1, 2, 3])
        ref = ray.put(tensor, _tensor_transport="ds")
        gpu_manager = ray._private.worker.global_worker.gpu_object_manager
        assert gpu_manager.gpu_object_store.has_tensor(tensor)
        del ref
        gpu_manager.gpu_object_store.wait_tensor_freed(tensor, timeout=10)
        assert not gpu_manager.gpu_object_store.has_tensor(tensor)
        return "Success"


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_ray_get_ds_ref_created_by_actor_task(ray_start_regular):
    """测试从 actor 任务创建的 DS ObjectRef 的 ray.get 操作"""
    actor = DSTestActor.remote()
    tensor = torch.tensor([1, 2, 3])
    ref1 = actor.echo.remote(tensor)
    ref2 = actor.echo.remote(tensor)
    ref3 = actor.echo.remote(tensor)

    # 测试使用默认 tensor transport 的 ray.get，应该使用 DS
    result1 = ray.get(ref1)
    # mock backend 的 recv 会填充为 1，所以结果是 [1, 1, 1]
    assert torch.equal(result1, torch.tensor([1, 1, 1]))

    # 测试使用 DS tensor transport 的 ray.get
    result2 = ray.get(ref2, _tensor_transport="ds")
    assert torch.equal(result2, torch.tensor([1, 1, 1]))

    # 测试使用 object store tensor transport 的 ray.get
    result3 = ray.get(ref3, _tensor_transport="object_store")
    assert torch.equal(result3, tensor)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_p2p(ray_start_regular):
    """测试 P2P tensor 传输"""
    num_actors = 2
    actors = [DSTestActor.remote() for _ in range(num_actors)]

    src_actor, dst_actor = actors[0], actors[1]

    # 创建测试 tensor
    tensor = torch.tensor([1, 2, 3])

    # 测试 CPU 到 CPU 的传输
    ref = src_actor.echo.remote(tensor)

    # 触发从 src 到 dst actor 的 tensor 传输
    result = dst_actor.sum.remote(ref)
    # mock backend 的 recv 会填充为 1，所以 sum 是 3
    assert ray.get(result) == 3


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 1}], indirect=True)
def test_intra_tensor_transfer(ray_start_regular):
    """测试 intra-actor tensor 传输"""
    actor = DSTestActor.remote()

    tensor = torch.tensor([1, 2, 3])

    # Intra-actor 通信, 不调用recv, 不改变tensor
    ref = actor.echo.remote(tensor)
    result = actor.sum.remote(ref)
    assert ray.get(result) == 6


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_put_and_get_object_with_ds(ray_start_regular):
    """测试使用 DS 的 ray.put 和 ray.get"""
    actors = [DSTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    tensor1 = torch.tensor([1, 2, 3])
    tensor2 = torch.tensor([4, 5, 6, 0])
    tensor3 = torch.tensor([7, 8, 9, 0, 0])
    tensors = [tensor1, tensor2, tensor3]

    ref = src_actor.produce.remote(tensors)
    ref1 = dst_actor.consume_with_ds.remote(ref)
    result1 = ray.get(ref1)
    # 每个 tensor 的元素数量：3 + 4 + 5 = 12
    # mock backend 填充为 1，所以总和是 12
    assert result1 == 12


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_put_and_get_object_with_object_store(ray_start_regular):
    """测试使用 object store 的 ray.put 和 ray.get"""
    actors = [DSTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    tensor1 = torch.tensor([1, 2, 3])
    tensor2 = torch.tensor([4, 5, 6, 0])
    tensor3 = torch.tensor([7, 8, 9, 0, 0])
    tensors = [tensor1, tensor2, tensor3]

    ref = src_actor.produce.remote(tensors)
    ref1 = dst_actor.consume_with_object_store.remote(ref)
    result1 = ray.get(ref1)
    # 使用 object store，保留原始值：6 + 15 + 24 = 45
    assert result1 == 45


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 1}], indirect=True)
def test_put_gc(ray_start_regular):
    """测试垃圾回收"""
    actor = DSTestActor.remote()
    ref = actor.gc.remote()
    assert ray.get(ref) == "Success"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
