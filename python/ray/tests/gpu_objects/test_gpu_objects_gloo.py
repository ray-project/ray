import logging
import random
import re
import sys
import threading
import time

import pytest
import torch

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.custom_types import TensorTransportEnum
from ray.experimental.collective import create_collective_group

# tensordict is not supported on macos ci, so we skip the tests
support_tensordict = sys.platform != "darwin"

if support_tensordict:
    from tensordict import TensorDict


# TODO: check whether concurrency groups are created correctly if
# enable_tensor_transport is True or if any methods are decorated with
# @ray.method(tensor_transport=...). Check that specifying
# .options(tensor_transport=...) fails if enable_tensor_transport is False.
@ray.remote
class GPUTestActor:
    @ray.method(tensor_transport="gloo")
    def echo(self, data):
        return data

    def add(self, a, b):
        return a + b

    def double(self, data):
        if isinstance(data, list):
            return [self.double(d) for d in data]
        if support_tensordict and isinstance(data, TensorDict):
            return data.apply(lambda x: x * 2)
        return data * 2

    def increment(self, data):
        data += 1
        return data

    def get_out_of_band_tensors(self, obj_id: str, timeout=None):
        gpu_object_store = (
            ray._private.worker.global_worker.gpu_object_manager.gpu_object_store
        )
        if timeout is None:
            timeout = 0
        return gpu_object_store.wait_and_get_object(obj_id, timeout)

    def get_num_gpu_objects(self):
        gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
        return gpu_object_manager.gpu_object_store.get_num_objects()

    def fail(self, error_message):
        raise Exception(error_message)


@ray.remote
class ErrorActor:
    @ray.method(tensor_transport="gloo")
    def send(self, tensor):
        return tensor

    def recv(self, tensor):
        return tensor

    def clear_gpu_object_store(self):
        gpu_object_store = (
            ray._private.worker.global_worker.gpu_object_manager.gpu_object_store
        )

        with gpu_object_store._lock:
            assert len(gpu_object_store._gpu_object_store) > 0
            gpu_object_store._gpu_object_store.clear()

    @ray.method(concurrency_group="_ray_system")
    def block_background_thread(self):
        time.sleep(100)


@pytest.mark.parametrize("data_size_bytes", [100])
def test_gc_gpu_object(ray_start_regular, data_size_bytes):
    """
    For small data, GPU objects are inlined, but the actual data lives
    on the remote actor. Therefore, if we decrement the reference count
    upon inlining, we may cause the tensors on the sender actor to be
    freed before transferring to the receiver actor.

    # TODO(kevin85421): Add a test for large CPU data that is not inlined
    # after https://github.com/ray-project/ray/issues/54281 is fixed.
    """
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    small_tensor = torch.randn((1,))
    cpu_data = b"1" * data_size_bytes
    data = [small_tensor, cpu_data]
    sender = actors[0]
    receiver = actors[1]

    ref1 = sender.echo.remote(data)
    ref2 = receiver.double.remote(ref1)
    ref3 = receiver.double.remote(ref1)

    result = ray.get(ref2)
    assert result[0] == pytest.approx(small_tensor * 2)
    assert result[1] == cpu_data * 2
    result = ray.get(ref3)
    assert result[0] == pytest.approx(small_tensor * 2)
    assert result[1] == cpu_data * 2

    wait_for_condition(
        lambda: ray.get(receiver.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    del ref1

    wait_for_condition(
        lambda: ray.get(sender.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


def test_gc_gpu_object_metadata(ray_start_regular):
    actors = [GPUTestActor.remote() for _ in range(2)]
    create_collective_group(actors, backend="gloo")

    tensor = torch.randn((100, 100))
    ref = actors[0].echo.remote(tensor)
    gpu_obj_id = ref.hex()
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
    assert gpu_obj_id in gpu_object_manager.managed_gpu_object_metadata
    ray.get(actors[1].double.remote(ref))
    del ref

    wait_for_condition(
        lambda: gpu_obj_id not in gpu_object_manager.managed_gpu_object_metadata,
    )


@pytest.mark.parametrize("data_size_bytes", [100])
def test_gc_del_ref_before_recv_finish(ray_start_regular, data_size_bytes):
    """
    This test deletes the ObjectRef of the GPU object before calling
    `ray.get` to ensure the receiver finishes receiving the GPU object.
    """
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    small_tensor = torch.randn((1,))
    cpu_data = b"1" * data_size_bytes
    data = [small_tensor, cpu_data]
    sender = actors[0]
    receiver = actors[1]

    ref1 = sender.echo.remote(data)
    ref2 = receiver.double.remote(ref1)

    del ref1

    result = ray.get(ref2)
    assert result[0] == pytest.approx(small_tensor * 2)
    assert result[1] == cpu_data * 2

    wait_for_condition(
        lambda: ray.get(receiver.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )
    wait_for_condition(
        lambda: ray.get(sender.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


def test_gc_intra_actor_gpu_object(ray_start_regular):
    """
    This test checks that passes a GPU object ref to the same actor multiple times.
    """
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="gloo")

    small_tensor = torch.randn((1,))

    ref = actor.echo.remote(small_tensor)
    result = actor.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)

    result = actor.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)

    del ref

    wait_for_condition(
        lambda: ray.get(actor.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


def test_gc_pass_ref_to_same_and_different_actors(ray_start_regular):
    """
    This test checks that passes a GPU object ref to the same actor and a different actor.
    """
    actor1 = GPUTestActor.remote()
    actor2 = GPUTestActor.remote()
    create_collective_group([actor1, actor2], backend="gloo")

    small_tensor = torch.randn((1,))

    ref = actor1.echo.remote(small_tensor)
    result1 = actor1.double.remote(ref)
    result2 = actor2.double.remote(ref)
    assert ray.get(result1) == pytest.approx(small_tensor * 2)
    assert ray.get(result2) == pytest.approx(small_tensor * 2)

    wait_for_condition(
        lambda: ray.get(actor2.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    del ref

    wait_for_condition(
        lambda: ray.get(actor1.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


def test_p2p(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    small_tensor = torch.randn((1,))
    sender = actors[0]
    receiver = actors[1]

    ref = sender.echo.remote(small_tensor)
    result = receiver.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)

    medium_tensor = torch.randn((500, 500))
    ref = sender.echo.remote(medium_tensor)
    result = receiver.double.remote(ref)
    assert ray.get(result) == pytest.approx(medium_tensor * 2)


def test_p2p_errors_before_group_creation(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]

    small_tensor = torch.randn((1,))
    sender = actors[0]

    with pytest.raises(
        ValueError,
        match="Actor.* does not have tensor transport GLOO available.*",
    ):
        sender.echo.remote(small_tensor)


@pytest.mark.parametrize("has_tensor_transport_method", [True, False])
def test_p2p_blocking(ray_start_regular, has_tensor_transport_method):
    """Test that p2p transfers still work when sender is blocked in another
    task. This should work whether the actor has (a) a tensor transport method
    (a method decorated with @ray.method(tensor_transport=...)) or (b) an actor-level decorator
    @ray.remote(enable_tensor_transport=True)."""

    class _GPUTestActor:
        def double(self, data):
            if isinstance(data, list):
                return [self.double(d) for d in data]
            if support_tensordict and isinstance(data, TensorDict):
                return data.apply(lambda x: x * 2)
            return data * 2

        def infinite_sleep(self, signal):
            signal.send.remote()
            while True:
                time.sleep(0.1)

    if has_tensor_transport_method:
        # Test tensor transport annotation via ray.method.
        @ray.remote
        class GPUTestActor(_GPUTestActor):
            @ray.method(tensor_transport="gloo")
            def echo(self, data):
                return data

    else:
        # Test tensor transport annotation via ray.remote.
        @ray.remote(enable_tensor_transport=True)
        class GPUTestActor(_GPUTestActor):
            def echo(self, data):
                return data

    sender, receiver = GPUTestActor.remote(), GPUTestActor.remote()
    signal = SignalActor.remote()
    create_collective_group([sender, receiver], backend="gloo")
    tensor = torch.randn((500, 500))
    # If the actor does not have a tensor transport method declared, declare it
    # dynamically using .options().
    sender_fn = (
        sender.echo
        if has_tensor_transport_method
        else sender.echo.options(tensor_transport="gloo")
    )
    ref = sender_fn.remote(tensor)

    # Start a blocking task on the sender actor.
    sender.infinite_sleep.remote(signal)
    ray.get(signal.wait.remote(), timeout=10)

    # Ensure that others can still receive the object.
    result = receiver.double.remote(ref)
    result = ray.get(result, timeout=10)
    assert result == pytest.approx(tensor * 2)


def test_p2p_with_cpu_data(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    sender = actors[0]
    receiver = actors[1]

    cpu_data = 123
    ref = sender.echo.remote(cpu_data)
    result = receiver.double.remote(ref)
    assert ray.get(result) == cpu_data * 2


def test_send_same_ref_to_same_actor_task_multiple_times(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    small_tensor = torch.randn((1,))
    sender = actors[0]
    receiver = actors[1]

    ref = sender.echo.remote(small_tensor)
    result = receiver.add.remote(ref, ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)

    wait_for_condition(
        lambda: ray.get(receiver.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


def test_send_same_ref_to_same_actor_multiple_times(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    small_tensor = torch.randn((1,))
    sender = actors[0]
    receiver = actors[1]

    ref = sender.echo.remote(small_tensor)
    result = receiver.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)

    result = receiver.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)


def test_intra_gpu_tensor_transfer(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="gloo")

    small_tensor = torch.randn((1,))

    # Intra-actor communication for pure GPU tensors
    ref = actor.echo.remote(small_tensor)
    result = actor.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)

    # Intra-actor communication for mixed CPU and GPU data
    cpu_data = random.randint(0, 100)
    data = [small_tensor, cpu_data]
    ref = actor.echo.remote(data)
    result = actor.double.remote(ref)
    assert ray.get(result) == pytest.approx([small_tensor * 2, cpu_data * 2])

    # Intra-actor communication for multiple GPU tensors
    tensor1 = torch.randn((1,))
    tensor2 = torch.randn((2,))
    data = [tensor1, tensor2, cpu_data]
    ref = actor.echo.remote(data)
    result = actor.double.remote(ref)
    result = ray.get(result)

    assert result[0] == pytest.approx(tensor1 * 2)
    assert result[1] == pytest.approx(tensor2 * 2)
    assert result[2] == cpu_data * 2


def test_send_same_ref_multiple_times_intra_actor(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="gloo")

    small_tensor = torch.randn((1,))

    ref = actor.echo.remote(small_tensor)
    result = actor.add.remote(ref, ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)


def test_mix_cpu_gpu_data(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    tensor = torch.randn((1,))
    cpu_data = random.randint(0, 100)

    data = [tensor, cpu_data]

    sender, receiver = actors[0], actors[1]
    ref = sender.echo.remote(data)
    ref = receiver.double.remote(ref)
    result = ray.get(ref)

    assert result[0] == pytest.approx(tensor * 2)
    assert result[1] == cpu_data * 2


def test_object_in_plasma(ray_start_regular):
    """
    This test uses a CPU object that is large enough to be stored
    in plasma instead of being inlined in the gRPC message.
    """
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    tensor = torch.randn((1,))
    cpu_data = b"1" * 1000 * 1000
    data = [tensor, cpu_data]

    sender, receiver = actors[0], actors[1]
    ref = sender.echo.remote(data)
    ref = receiver.double.remote(ref)
    result = ray.get(ref)

    assert result[0] == pytest.approx(tensor * 2)
    assert result[1] == cpu_data * 2


def test_multiple_tensors(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    tensor1 = torch.randn((1,))
    tensor2 = torch.randn((2,))
    if support_tensordict:
        td1 = TensorDict(
            {"action1": torch.randn((2,)), "reward1": torch.randn((2,))}, batch_size=[2]
        )
        td2 = TensorDict(
            {"action2": torch.randn((2,)), "reward2": torch.randn((2,))}, batch_size=[2]
        )
    else:
        td1 = 0
        td2 = 0
    cpu_data = random.randint(0, 100)
    data = [tensor1, tensor2, cpu_data, td1, td2]

    sender, receiver = actors[0], actors[1]
    ref = sender.echo.remote(data)
    ref = receiver.double.remote(ref)
    result = ray.get(ref)

    assert result[0] == pytest.approx(tensor1 * 2)
    assert result[1] == pytest.approx(tensor2 * 2)
    assert result[2] == cpu_data * 2
    if support_tensordict:
        assert result[3]["action1"] == pytest.approx(td1["action1"] * 2)
        assert result[3]["reward1"] == pytest.approx(td1["reward1"] * 2)
        assert result[4]["action2"] == pytest.approx(td2["action2"] * 2)
        assert result[4]["reward2"] == pytest.approx(td2["reward2"] * 2)


def test_trigger_out_of_band_tensor_transfer(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    src_actor, dst_actor = actors[0], actors[1]

    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)
    gpu_obj_id = gpu_ref.hex()

    # Check src_actor has the GPU object
    ret_val_src = ray.get(src_actor.get_out_of_band_tensors.remote(gpu_obj_id))
    assert ret_val_src is not None
    assert len(ret_val_src) == 1
    assert torch.equal(ret_val_src[0], tensor)

    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
    gpu_object_manager.add_gpu_object_ref(gpu_ref, src_actor, TensorTransportEnum.GLOO)

    # Trigger out-of-band tensor transfer from src_actor to dst_actor.
    task_args = (gpu_ref,)
    gpu_object_manager.trigger_out_of_band_tensor_transfer(dst_actor, task_args)

    # Check dst_actor has the GPU object
    ret_val_dst = ray.get(
        dst_actor.get_out_of_band_tensors.remote(gpu_obj_id, timeout=10)
    )
    assert ret_val_dst is not None
    assert len(ret_val_dst) == 1
    assert torch.equal(ret_val_dst[0], tensor)


def test_fetch_gpu_object_to_driver(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="gloo")

    tensor1 = torch.tensor([1, 2, 3])
    tensor2 = torch.tensor([4, 5, 6])

    # Case 1: Single tensor
    ref = actor.echo.remote(tensor1)
    assert torch.equal(ray.get(ref, _tensor_transport="object_store"), tensor1)

    # Case 2: Multiple tensors
    ref = actor.echo.remote([tensor1, tensor2])
    result = ray.get(ref, _tensor_transport="object_store")
    assert torch.equal(result[0], tensor1)
    assert torch.equal(result[1], tensor2)

    # Case 3: Mixed CPU and GPU data
    data = [tensor1, tensor2, 7]
    ref = actor.echo.remote(data)
    result = ray.get(ref, _tensor_transport="object_store")
    assert torch.equal(result[0], tensor1)
    assert torch.equal(result[1], tensor2)
    assert result[2] == 7


def test_invalid_tensor_transport(ray_start_regular):
    with pytest.raises(ValueError, match="Invalid tensor transport"):

        @ray.remote
        class InvalidActor:
            @ray.method(tensor_transport="invalid")
            def echo(self, data):
                return data


@pytest.mark.skipif(
    not support_tensordict,
    reason="tensordict is not supported on this platform",
)
def test_tensordict_transfer(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    td = TensorDict(
        {"action": torch.randn((2,)), "reward": torch.randn((2,))}, batch_size=[2]
    )
    sender, receiver = actors[0], actors[1]
    ref = sender.echo.remote(td)
    result = receiver.double.remote(ref)
    td_result = ray.get(result)

    assert td_result["action"] == pytest.approx(td["action"] * 2)
    assert td_result["reward"] == pytest.approx(td["reward"] * 2)


@pytest.mark.skipif(
    not support_tensordict,
    reason="tensordict is not supported on this platform",
)
def test_nested_tensordict(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    inner_td = TensorDict(
        {"action": torch.randn((2,)), "reward": torch.randn((2,))}, batch_size=[2]
    )
    outer_td = TensorDict(
        {"inner_td": inner_td, "test": torch.randn((2,))}, batch_size=[2]
    )
    sender = actors[0]
    receiver = actors[1]
    gpu_ref = sender.echo.remote(outer_td)
    ret_val_src = ray.get(receiver.double.remote(gpu_ref))
    assert ret_val_src is not None
    assert torch.equal(ret_val_src["inner_td"]["action"], inner_td["action"] * 2)
    assert torch.equal(ret_val_src["inner_td"]["reward"], inner_td["reward"] * 2)
    assert torch.equal(ret_val_src["test"], outer_td["test"] * 2)


@pytest.mark.skipif(
    not support_tensordict,
    reason="tensordict is not supported on this platform",
)
def test_tensor_extracted_from_tensordict_in_gpu_object_store(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="gloo")

    td = TensorDict(
        {"action": torch.randn((2,)), "reward": torch.randn((2,))}, batch_size=[2]
    ).to("cpu")
    gpu_ref = actor.echo.remote(td)

    # Since the tensor is extracted from the tensordict, the `ret_val_src` will be a list of tensors
    # instead of a tensordict.
    ret_val_src = ray.get(actor.get_out_of_band_tensors.remote(gpu_ref.hex()))
    assert ret_val_src is not None
    assert len(ret_val_src) == 2
    assert torch.equal(ret_val_src[0], td["action"])
    assert torch.equal(ret_val_src[1], td["reward"])


@pytest.mark.parametrize("enable_tensor_transport", [True, False])
def test_dynamic_tensor_transport_via_options(
    ray_start_regular, enable_tensor_transport
):
    """Test that tensor_transport can be set dynamically via .options() at call
    time, if enable_tensor_transport is set to True in @ray.remote."""

    class TestActor:
        def __init__(self):
            pass

        def normal_method(self):
            return "normal"

        def tensor_method(self):
            return torch.randn(5, 5)

        def double(self, data):
            return data * 2

    if enable_tensor_transport:
        TestActor = ray.remote(enable_tensor_transport=True)(TestActor)
    else:
        TestActor = ray.remote(TestActor)

    # Create actor without any tensor_transport decorators
    sender = TestActor.remote()
    receiver = TestActor.remote()
    create_collective_group([sender, receiver], backend="gloo")

    # Test normal method call
    result = ray.get(sender.normal_method.remote())
    assert result == "normal"

    # Test method call with tensor_transport specified via .options()
    if enable_tensor_transport:
        # If enable_tensor_transport is set to True, then it's okay to use
        # dynamic tensor_transport.
        ref = sender.tensor_method.options(tensor_transport="gloo").remote()
        tensor = ray.get(ref, _tensor_transport="object_store")
        result = ray.get(receiver.double.remote(ref), _tensor_transport="object_store")
        assert result == pytest.approx(tensor * 2)
    else:
        # If enable_tensor_transport is not set, then user cannot use
        # dynamic tensor_transport.
        with pytest.raises(
            ValueError,
            match='Currently, methods with .options\\(tensor_transport="GLOO"\\) are not supported when enable_tensor_transport=False. Please set @ray.remote\\(enable_tensor_transport=True\\) on the actor class definition.',
        ):
            ref = sender.tensor_method.options(tensor_transport="gloo").remote()


def test_app_error_inter_actor(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    src_actor, dst_actor = actors[0], actors[1]

    # Make sure the receiver can receive an exception from the sender.
    ref = src_actor.fail.options(tensor_transport="gloo").remote("test_app_error")
    with pytest.raises(Exception, match="test_app_error"):
        ray.get(dst_actor.double.remote(ref))

    # Make sure the sender and receiver do not hang.
    small_tensor = torch.randn((1,))
    ref = src_actor.echo.remote(small_tensor)
    result = dst_actor.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)


def test_app_error_intra_actor(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="gloo")

    # Make sure the receiver can receive an exception from the sender.
    ref = actor.fail.options(tensor_transport="gloo").remote("test_app_error")
    with pytest.raises(Exception, match="test_app_error"):
        ray.get(actor.double.remote(ref))

    # Make sure the sender and receiver do not hang.
    small_tensor = torch.randn((1,))
    ref = actor.echo.remote(small_tensor)
    result = actor.double.remote(ref)
    assert ray.get(result) == pytest.approx(small_tensor * 2)


def test_app_error_fetch_to_driver(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="gloo")

    ref = actor.fail.options(tensor_transport="gloo").remote("test_app_error")
    with pytest.raises(Exception, match="test_app_error"):
        ray.get(ref, _tensor_transport="object_store")

    # Make sure the driver can receive an exception from the actor.
    small_tensor = torch.tensor([1, 2, 3])
    ref = actor.echo.remote(small_tensor)
    assert torch.equal(ray.get(ref, _tensor_transport="object_store"), small_tensor)


def test_write_after_save(ray_start_regular):
    """Check that an actor can safely write to a tensor after saving it to its
    local state by calling `ray.experimental.wait_tensor_freed`."""

    @ray.remote(enable_tensor_transport=True)
    class GPUTestActor:
        @ray.method(tensor_transport="gloo")
        def save(self, data: torch.Tensor):
            # Save the tensor to the actor's local state.
            self.data = data
            return data

        def receive(self, data: torch.Tensor):
            return data

        def increment_saved(self):
            ray.experimental.wait_tensor_freed(self.data)
            # Write to the saved tensor.
            self.data += 1
            return self.data

    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    medium_tensor = torch.randn((500, 500))
    sender, receiver = actors
    ref = sender.save.remote(medium_tensor)
    # Sender writes to the GPU object while Ray sends the object to a receiver
    # task in the background.
    tensor1 = sender.increment_saved.remote()
    tensor2 = receiver.receive.remote(ref)

    # The sender task should not have returned yet because the ObjectRef is
    # still in scope.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(tensor1, timeout=1)

    del ref
    # Check that Ray completed the transfer of the original tensor before the
    # sender writes to it.
    assert torch.allclose(ray.get(tensor1), medium_tensor + 1)
    assert torch.allclose(ray.get(tensor2), medium_tensor)


def test_wait_tensor_freed(ray_start_regular):
    """Unit test for ray.experimental.wait_tensor_freed. Check that the call
    returns when the tensor has been freed from the GPU object store."""
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store
    obj_id = "random_id"
    tensor = torch.randn((1,))
    gpu_object_store.add_object(obj_id, [tensor], is_primary=True)

    assert gpu_object_store.has_object(obj_id)
    with pytest.raises(TimeoutError):
        ray.experimental.wait_tensor_freed(tensor, timeout=1)
    assert gpu_object_store.has_object(obj_id)

    # Simulate garbage collection in a background thread.
    def gc():
        time.sleep(0.1)
        gpu_object_store.pop_object(obj_id)

    gc_thread = threading.Thread(target=gc)
    gc_thread.start()
    # Now the wait_tensor_freed call should be able to return.
    ray.experimental.wait_tensor_freed(tensor)
    gc_thread.join()
    assert not gpu_object_store.has_object(obj_id)


def test_wait_tensor_freed_double_tensor(ray_start_regular):
    """Unit test for ray.experimental.wait_tensor_freed when multiple objects
    contain the same tensor."""
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store
    obj_id1 = "random_id1"
    obj_id2 = "random_id2"
    tensor = torch.randn((1,))
    gpu_object_store.add_object(obj_id1, [tensor], is_primary=True)
    gpu_object_store.add_object(obj_id2, [tensor], is_primary=True)

    assert gpu_object_store.has_object(obj_id1)
    assert gpu_object_store.has_object(obj_id2)
    with pytest.raises(TimeoutError):
        ray.experimental.wait_tensor_freed(tensor, timeout=1)
    assert gpu_object_store.has_object(obj_id1)
    assert gpu_object_store.has_object(obj_id2)

    # Simulate garbage collection in a background thread.
    def gc(obj_id):
        time.sleep(0.1)
        gpu_object_store.pop_object(obj_id)

    # Free one object. Tensor should still be stored.
    gc_thread = threading.Thread(target=gc, args=(obj_id1,))
    gc_thread.start()
    with pytest.raises(TimeoutError):
        ray.experimental.wait_tensor_freed(tensor, timeout=1)
    gc_thread.join()
    assert not gpu_object_store.has_object(obj_id1)

    # Free the other object. Now the wait_tensor_freed call should be able to
    # return.
    gc_thread = threading.Thread(target=gc, args=(obj_id2,))
    gc_thread.start()
    ray.experimental.wait_tensor_freed(tensor)
    gc_thread.join()
    assert not gpu_object_store.has_object(obj_id2)


def test_send_back_and_dst_warning(ray_start_regular):
    # Test warning when object is sent back to the src actor and to dst actors
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")

    src_actor, dst_actor = actors[0], actors[1]

    tensor = torch.tensor([1, 2, 3])

    warning_message = r"GPU ObjectRef\(.+\)"

    with pytest.warns(UserWarning, match=warning_message):
        t = src_actor.echo.remote(tensor)
        t1 = src_actor.echo.remote(t)  # Sent back to the source actor
        t2 = dst_actor.echo.remote(t)  # Also sent to another actor
        ray.get([t1, t2], _tensor_transport="object_store")

    # Second transmission of ObjectRef `t` to `dst_actor` should not trigger a warning
    # Verify no `pytest.warns` context is used here because no warning should be raised
    t3 = dst_actor.echo.remote(t)
    ray.get(t3, _tensor_transport="object_store")


def test_duplicate_objectref_transfer(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="gloo")
    actor0, actor1 = actors[0], actors[1]

    small_tensor = torch.randn((1,))

    # Store the original value for comparison
    original_value = small_tensor

    ref = actor0.echo.remote(small_tensor)

    # Pass the same ref to actor1 twice
    result1 = actor1.increment.remote(ref)
    result2 = actor1.increment.remote(ref)

    # Both should return original_value + 1 because each increment task should receive the same object value.
    val1 = ray.get(result1)
    val2 = ray.get(result2)

    # Check for correctness
    assert val1 == pytest.approx(
        original_value + 1
    ), f"Result1 incorrect: got {val1}, expected {original_value + 1}"
    assert val2 == pytest.approx(
        original_value + 1
    ), f"Result2 incorrect: got {val2}, expected {original_value + 1}"

    # Additional check: results should be equal (both got clean copies)
    assert val1 == pytest.approx(
        val2
    ), f"Results differ: result1={val1}, result2={val2}"


def test_transfer_from_not_actor_creator(ray_start_regular):
    @ray.remote
    class Actor:
        @ray.method(tensor_transport="gloo")
        def create(self):
            return torch.tensor([1, 2, 3])

        def consume(self, obj):
            return obj

        def do_transfer(self, a1, a2):
            create_collective_group([a1, a2], backend="torch_gloo")
            return ray.get(a1.consume.remote(a2.create.remote()))

    actor = [Actor.remote() for _ in range(3)]
    assert ray.get(actor[2].do_transfer.remote(actor[0], actor[1])) == pytest.approx(
        torch.tensor([1, 2, 3])
    )


def test_send_fails(ray_start_regular):
    actors = [ErrorActor.remote() for _ in range(2)]
    create_collective_group(actors, backend="torch_gloo")

    # The gpu object will be gone when we trigger the transfer
    # so the send will error out
    gpu_obj_ref = actors[0].send.remote(torch.randn((100, 100)))
    ray.get(actors[0].clear_gpu_object_store.remote())
    result_ref = actors[1].recv.remote(gpu_obj_ref)

    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(result_ref)


def test_send_actor_dies(ray_start_regular):
    actors = [ErrorActor.remote() for _ in range(2)]
    create_collective_group(actors, backend="torch_gloo")

    # Try a transfer with the sender's background thread blocked,
    # so the send doesn't happen before the actor is killed
    gpu_obj_ref = actors[0].send.remote(torch.randn((100, 100)))
    actors[0].block_background_thread.remote()
    result_ref = actors[1].recv.remote(gpu_obj_ref)
    ray.kill(actors[0])

    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(result_ref)


def test_recv_actor_dies(ray_start_regular, caplog, propagate_logs):
    actors = [ErrorActor.remote() for _ in range(2)]
    create_collective_group(actors, backend="torch_gloo")

    # Do a transfer with the receiver's background thread blocked,
    # so the recv doesn't happen before the actor is killed
    gpu_obj_ref = actors[0].send.remote(torch.randn((100, 100)))
    actors[1].block_background_thread.remote()
    result_ref = actors[1].recv.remote(gpu_obj_ref)
    ray.kill(actors[1])

    def check_logs():
        records = caplog.records
        return any(
            record.levelno == logging.ERROR
            and re.search(r"RDT transfer with.*failed", record.message)
            for record in records
        ) and any(
            record.levelno == logging.ERROR
            and "Destroyed collective group" in record.message
            for record in records
        )

    wait_for_condition(check_logs)

    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(result_ref)
    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(actors[0].recv.remote(1))


@pytest.mark.skip(
    "Lineage Reconstruction currently results in a check failure with RDT"
)
def test_rdt_lineage_reconstruction(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1)
    worker_to_kill = cluster.add_node(num_cpus=1, resources={"to_restart": 1})

    @ray.remote(max_restarts=1, max_task_retries=1, resources={"to_restart": 1})
    class RecvRestartableActor:
        def recv(self, obj):
            return obj

    send_actor = GPUTestActor.remote()
    recv_actor = RecvRestartableActor.remote()
    create_collective_group([send_actor, recv_actor], backend="gloo")

    one_mb_tensor = torch.randn((1024 * 1024,))
    ref = recv_actor.recv.remote(send_actor.echo.remote(one_mb_tensor))
    ray.wait([ref], fetch_local=False)
    cluster.remove_node(worker_to_kill, allow_graceful=False)
    cluster.add_node(num_cpus=1, resources={"to_restart": 1})
    assert ray.get(ref).nbytes >= (1024 * 1024)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
