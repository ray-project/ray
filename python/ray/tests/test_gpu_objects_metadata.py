import gc
import sys
import random
import torch
import pytest
import threading
import ray
import time
from ray.experimental.collective import create_collective_group
from ray._private.custom_types import TensorTransportEnum
from ray._common.test_utils import wait_for_condition
from ray._common.test_utils import SignalActor
from ray.tests.test_gpu_objects_gloo import GPUTestActor

# tensordict is not supported on macos ci, so we skip the tests
support_tensordict = sys.platform != "darwin"

if support_tensordict:
    from tensordict import TensorDict


def test_gpu_object_store_timeout(ray_start_regular):
    """测试GPU对象存储的超时机制"""
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store

    # 尝试获取不存在的对象，验证超时异常
    non_existent_id = "non_existent_id"

    with pytest.raises(TimeoutError):
        gpu_object_store.wait_and_get_object(non_existent_id, timeout=0.5)

    with pytest.raises(TimeoutError):
        gpu_object_store.wait_and_pop_object(non_existent_id, timeout=0.5)


def test_unsupported_tensor_transport_backend(ray_start_regular):
    """测试不支持的张量传输后端处理"""
    from ray.experimental.gpu_object_manager.gpu_object_store import (
        _tensor_transport_to_collective_backend,
    )

    # 验证有效后端转换
    valid_backend = _tensor_transport_to_collective_backend(TensorTransportEnum.GLOO)
    assert valid_backend == "torch_gloo"

    # 验证无效后端处理
    class CustomEnum:
        name = "INVALID"

    with pytest.raises(ValueError, match="Invalid tensor transport"):
        _tensor_transport_to_collective_backend(CustomEnum())


def test_gpu_object_reference_leak(ray_start_regular):
    """测试失败场景下GPU对象引用的回收（模拟真实传输异常）"""
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="torch_gloo")

    sender = actors[0]
    receiver = actors[1]

    # 创建GPU对象并获取引用
    tensor = torch.randn((10, 10))
    gpu_ref = sender.echo.remote(tensor)
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
    obj_id = gpu_ref.hex()

    # 初始添加引用
    gpu_object_manager.add_gpu_object_ref(gpu_ref, sender, TensorTransportEnum.GLOO)

    # 验证元数据已注册
    with gpu_object_manager._metadata_lock:
        assert obj_id in gpu_object_manager.managed_gpu_object_metadata
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 1

    # 模拟传输过程中抛出异常（更真实的失败场景）
    try:
        # 触发传输（会增加引用计数）
        gpu_object_manager.trigger_out_of_band_tensor_transfer(receiver, (gpu_ref,))
        # 在传输后立即抛出异常
        raise Exception("Simulate failure during tensor transfer")
    except Exception:
        pass

    # 主动删除引用并触发GC
    del gpu_ref
    import gc

    gc.collect()
    time.sleep(1)  # 等待弱引用回调执行

    # 验证元数据已完全清理
    with gpu_object_manager._metadata_lock:
        assert obj_id not in gpu_object_manager.managed_gpu_object_metadata
        assert obj_id not in gpu_object_manager._gpu_object_ref_counts


def test_gpu_object_store_concurrent_access(ray_start_regular):
    """测试GPU对象存储的并发访问安全性"""
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store

    # 准备测试数据
    num_tensors = 10
    tensors = [torch.randn((10, 10)) for _ in range(num_tensors)]
    obj_ids = [f"concurrent_test_id_{i}" for i in range(num_tensors)]

    # 线程协调工具
    start_event = threading.Event()
    results = {"errors": 0}
    results_lock = threading.Lock()

    def add_objects(thread_id):
        start_event.wait()  # 等待所有线程就绪
        try:
            idx = thread_id % num_tensors
            obj_id = obj_ids[idx]
            tensor = tensors[idx]

            # 并发添加对象
            gpu_object_store.add_object(obj_id, [tensor], is_primary=True)
            # 并发读取对象
            result = gpu_object_store.get_object(obj_id)
            assert torch.equal(result[0], tensor), f"Thread {thread_id} data mismatch"
            # 并发删除对象
            gpu_object_store.pop_object(obj_id)
        except Exception as e:
            print(f"Thread {thread_id} error: {e}")
            with results_lock:
                results["errors"] += 1

    # 启动多线程并发操作
    num_threads = 20
    threads = [
        threading.Thread(target=add_objects, args=(i,)) for i in range(num_threads)
    ]
    for t in threads:
        t.start()

    start_event.set()  # 触发所有线程同时执行
    for t in threads:
        t.join()

    # 验证无并发错误且对象已清理
    assert results["errors"] == 0
    for obj_id in obj_ids:
        assert not gpu_object_store.has_object(obj_id)


def test_duplicate_gpu_object_ref_add(ray_start_regular):
    """测试重复添加相同GPU对象引用时的计数处理"""
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="torch_gloo")

    # 创建测试对象
    tensor = torch.randn((10, 10))
    gpu_ref = actor.echo.remote(tensor)
    obj_id = gpu_ref.hex()
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager

    # 第一次添加引用
    gpu_object_manager.add_gpu_object_ref(gpu_ref, actor, TensorTransportEnum.GLOO)
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 1

    # 重复添加相同引用（预期：计数不应重置，此处模拟代码可能的警告）
    with pytest.warns(UserWarning, match="Duplicate add for GPU object ref"):
        gpu_object_manager.add_gpu_object_ref(gpu_ref, actor, TensorTransportEnum.GLOO)
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 1  # 确保计数未被覆盖

    # 主动增加计数
    gpu_object_manager.increment_ref_count(obj_id)
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 2

    # 删除引用并触发GC
    del gpu_ref
    import gc

    gc.collect()
    time.sleep(1)

    # 验证计数正确减少
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 1

    # 模拟最后一次引用清理
    gpu_object_manager._cleanup_gpu_object_metadata(obj_id)
    with gpu_object_manager._metadata_lock:
        assert obj_id not in gpu_object_manager.managed_gpu_object_metadata
        assert obj_id not in gpu_object_manager._gpu_object_ref_counts


def test_gpu_object_manager_ref_counting(ray_start_regular):
    """测试多接收者场景下的引用计数正确性"""
    world_size = 3
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="torch_gloo")

    # 创建对象并获取引用
    tensor = torch.randn((10, 10))
    sender = actors[0]
    receiver1 = actors[1]
    receiver2 = actors[2]
    gpu_ref = sender.echo.remote(tensor)
    obj_id = gpu_ref.hex()

    # 初始化引用管理
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
    gpu_object_manager.add_gpu_object_ref(gpu_ref, sender, TensorTransportEnum.GLOO)
    initial_count = gpu_object_manager._gpu_object_ref_counts[obj_id]

    # 向两个接收者传输（每次传输增加计数）
    gpu_object_manager.trigger_out_of_band_tensor_transfer(receiver1, (gpu_ref,))
    gpu_object_manager.trigger_out_of_band_tensor_transfer(receiver2, (gpu_ref,))

    # 验证计数增加
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == initial_count + 2

    # 删除原始引用
    del gpu_ref
    gc.collect()
    time.sleep(1)

    # 验证元数据仍存在（因接收者持有引用）
    with gpu_object_manager._metadata_lock:
        assert obj_id in gpu_object_manager.managed_gpu_object_metadata
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 2  # 剩余两个接收者引用


def test_gpu_object_store_tensor_lifecycle(ray_start_regular):
    """测试GPU对象的完整生命周期管理"""
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="torch_gloo")

    # 手动管理对象生命周期
    tensor = torch.randn((100, 100))
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store
    obj_id = "test_tensor_lifecycle_id"

    # 添加对象
    gpu_object_store.add_object(obj_id, [tensor], is_primary=True)
    assert gpu_object_store.has_object(obj_id)
    assert gpu_object_store.is_primary_copy(obj_id)

    # 验证未释放时wait_tensor_freed超时
    with pytest.raises(TimeoutError):
        ray.experimental.wait_tensor_freed(tensor, timeout=0.5)

    # 后台线程删除对象
    def remove_tensor():
        time.sleep(0.2)
        gpu_object_store.pop_object(obj_id)

    cleanup_thread = threading.Thread(target=remove_tensor)
    cleanup_thread.start()

    # 验证对象释放后wait成功
    ray.experimental.wait_tensor_freed(tensor)
    cleanup_thread.join()

    # 验证对象已删除
    assert not gpu_object_store.has_object(obj_id)


def test_gpu_object_manager_communicator_check(ray_start_regular):
    """测试通信器存在性检查"""
    actor = GPUTestActor.remote()
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager

    # 初始无通信器
    assert not gpu_object_manager.actor_has_tensor_transport(
        actor, TensorTransportEnum.GLOO
    )

    # 创建通信器后验证
    create_collective_group([actor], backend="torch_gloo")
    assert gpu_object_manager.actor_has_tensor_transport(
        actor, TensorTransportEnum.GLOO
    )

    # 验证其他后端仍不可用
    assert not gpu_object_manager.actor_has_tensor_transport(
        actor, TensorTransportEnum.NCCL
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
