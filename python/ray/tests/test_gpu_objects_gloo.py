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

    def double(self, data):
        if isinstance(data, list):
            return [self.double(d) for d in data]
        if support_tensordict and isinstance(data, TensorDict):
            return data.apply(lambda x: x * 2)
        return data * 2

    def get_gpu_object(self, obj_id: str, timeout=None):
        gpu_object_store = (
            ray._private.worker.global_worker.gpu_object_manager.gpu_object_store
        )
        if timeout is None:
            timeout = 0
        return gpu_object_store.wait_and_get_object(obj_id, timeout)

    def get_num_gpu_objects(self):
        gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
        return gpu_object_manager.gpu_object_store.get_num_objects()

    def infinite_sleep(self, signal):
        signal.send.remote()
        while True:
            time.sleep(1)


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
    create_collective_group(actors, backend="torch_gloo")

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


@pytest.mark.parametrize("data_size_bytes", [100])
def test_gc_del_ref_before_recv_finish(ray_start_regular, data_size_bytes):
    """
    This test deletes the ObjectRef of the GPU object before calling
    `ray.get` to ensure the receiver finishes receiving the GPU object.
    """
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="torch_gloo")

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
    create_collective_group([actor], backend="torch_gloo")

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
    create_collective_group([actor1, actor2], backend="torch_gloo")

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
    create_collective_group(actors, backend="torch_gloo")

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
    receiver = actors[1]

    with pytest.raises(
        ValueError,
        match="Actor.* does not have tensor transport GLOO available. Please create a communicator with `ray.experimental.collective.create_collective_group` before calling actor tasks with non-default tensor_transport.",
    ):
        ref = sender.echo.remote(small_tensor)


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
                time.sleep(1)


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

    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="torch_gloo")

    sender = actors[0]
    receiver = actors[1]

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
    signal = SignalActor.remote()
    sender.infinite_sleep.remote(signal)
    ray.get(signal.wait.remote(), timeout=10)

    # Ensure that others can still receive the object.
    result = receiver.double.remote(ref)
    result = ray.get(result, timeout=10)
    assert result == pytest.approx(tensor * 2)


def test_intra_gpu_tensor_transfer(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="torch_gloo")

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


def test_mix_cpu_gpu_data(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="torch_gloo")

    tensor = torch.randn((1,))
    cpu_data = random.randint(0, 100)
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
    create_collective_group(actors, backend="torch_gloo")

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
    create_collective_group(actors, backend="torch_gloo")

    src_actor, dst_actor = actors[0], actors[1]

    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)
    gpu_obj_id = gpu_ref.hex()

    # Check src_actor has the GPU object
    ret_val_src = ray.get(src_actor.get_gpu_object.remote(gpu_obj_id))
    assert ret_val_src is not None
    assert len(ret_val_src) == 1
    assert torch.equal(ret_val_src[0], tensor)

    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
    gpu_object_manager.add_gpu_object_ref(gpu_ref, src_actor, TensorTransportEnum.GLOO)

    # Trigger out-of-band tensor transfer from src_actor to dst_actor.
    task_args = (gpu_ref,)
    gpu_object_manager.trigger_out_of_band_tensor_transfer(dst_actor, task_args)

    # Check dst_actor has the GPU object
    ret_val_dst = ray.get(dst_actor.get_gpu_object.remote(gpu_obj_id, timeout=10))
    assert ret_val_dst is not None
    assert len(ret_val_dst) == 1
    assert torch.equal(ret_val_dst[0], tensor)


def test_fetch_gpu_object_to_driver(ray_start_regular):
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="torch_gloo")

    tensor1 = torch.tensor([1, 2, 3])
    tensor2 = torch.tensor([4, 5, 6])

    # Case 1: Single tensor
    ref = actor.echo.remote(tensor1)
    assert torch.equal(ray.get(ref), tensor1)

    # Case 2: Multiple tensors
    ref = actor.echo.remote([tensor1, tensor2])
    result = ray.get(ref)
    assert torch.equal(result[0], tensor1)
    assert torch.equal(result[1], tensor2)

    # Case 3: Mixed CPU and GPU data
    data = [tensor1, tensor2, 7]
    ref = actor.echo.remote(data)
    result = ray.get(ref)
    assert torch.equal(result[0], tensor1)
    assert torch.equal(result[1], tensor2)
    assert result[2] == 7


@pytest.mark.skipif(
    not support_tensordict,
    reason="tensordict is not supported on this platform",
)
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
    create_collective_group(actors, backend="torch_gloo")

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
    create_collective_group(actors, backend="torch_gloo")

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
    create_collective_group([actor], backend="torch_gloo")

    td = TensorDict(
        {"action": torch.randn((2,)), "reward": torch.randn((2,))}, batch_size=[2]
    ).to("cpu")
    gpu_ref = actor.echo.remote(td)

    # Since the tensor is extracted from the tensordict, the `ret_val_src` will be a list of tensors
    # instead of a tensordict.
    ret_val_src = ray.get(actor.get_gpu_object.remote(gpu_ref.hex()))
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
    create_collective_group([sender, receiver], backend="torch_gloo")

    # Test normal method call
    result = ray.get(sender.normal_method.remote())
    assert result == "normal"

    # Test method call with tensor_transport specified via .options()
    if enable_tensor_transport:
        # If enable_tensor_transport is set to True, then it's okay to use
        # dynamic tensor_transport.
        ref = sender.tensor_method.options(tensor_transport="gloo").remote()
        tensor = ray.get(ref)
        result = ray.get(receiver.double.remote(ref))
        assert result == pytest.approx(tensor * 2)
    else:
        # If enable_tensor_transport is not set, then user cannot use
        # dynamic tensor_transport.
        with pytest.raises(
            ValueError,
            match='Currently, methods with .options\\(tensor_transport="GLOO"\\) are not supported when enable_tensor_transport=False. Please set @ray.remote\\(enable_tensor_transport=True\\) on the actor class definition.',
        ):
            ref = sender.tensor_method.options(tensor_transport="gloo").remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
