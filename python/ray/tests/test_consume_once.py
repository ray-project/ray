import dataclasses
import pickle
import sys
import threading

import pytest

import ray


@ray.remote
class Producer:
    def produce(self, value):
        return value

    def produce_pair(self):
        return 1, 2

    def generate(self):
        yield 1

    @ray.method(num_returns="dynamic")
    def generate_dynamic(self):
        yield 1


@ray.remote
class Consumer:
    def __init__(self):
        self.num_calls = 0

    def consume(self, value):
        self.num_calls += 1
        return value

    def get_num_calls(self):
        return self.num_calls

    def consume_all(self, *values, extra=None):
        return list(values) + [extra]


@ray.remote
class Holder:
    def __init__(self, *values, extra=None):
        self.values = values


@ray.remote
class Owner:
    """Owns a consume-once ref and tries to hand it back to its caller."""

    def __init__(self):
        self.producer = Producer.remote()

    def return_ref(self):
        return self.producer.produce.options(_consume_once=True).remote(1)

    def return_nested_ref(self):
        return [self.producer.produce.options(_consume_once=True).remote(1)]

    def keep_ref(self):
        self.kept_ref = self.producer.produce.options(_consume_once=True).remote(1)

    def return_kept_ref(self):
        return self.kept_ref

    def raise_with_ref(self):
        ref = self.producer.produce.options(_consume_once=True).remote(1)
        raise RuntimeError("boom", ref)

    def yield_ref(self):
        yield 1
        yield self.producer.produce.options(_consume_once=True).remote(1)

    def ping(self):
        return "alive"


@ray.remote
class AsyncOwner:
    def __init__(self):
        self.producer = Producer.remote()

    async def raise_with_ref(self):
        ref = self.producer.produce.options(_consume_once=True).remote(1)
        raise RuntimeError("boom", ref)

    async def ping(self):
        return "alive"


@ray.remote
def raise_with_ref_task():
    ref = Producer.remote().produce.options(_consume_once=True).remote(1)
    raise RuntimeError("boom", ref)


@ray.remote
def normal_task(*values, extra=None):
    return values[0]


def test_consume_once_ref_can_be_read_and_passed_to_actor_task(
    ray_start_regular_shared,
):
    producer = Producer.remote()
    consumer = Consumer.remote()

    ref = producer.produce.options(_consume_once=True).remote(1)
    assert ray.get(ref) == 1
    assert ray.get(consumer.consume.remote(ref)) == 1


def test_actor_task_accepts_mixed_args(ray_start_regular_shared):
    producer = Producer.remote()
    consumer = Consumer.remote()

    movable_1 = producer.produce.options(_consume_once=True).remote(1)
    movable_2 = producer.produce.options(_consume_once=True).remote(2)
    plain = producer.produce.remote(3)
    result = consumer.consume_all.remote(movable_1, plain, 4, extra=movable_2)
    assert ray.get(result) == [1, 3, 4, 2]


def test_normal_task_rejects_mixed_args(ray_start_regular_shared):
    producer = Producer.remote()

    movable = producer.produce.options(_consume_once=True).remote(1)
    plain = producer.produce.remote(2)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(plain, movable)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(movable, plain)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(plain, extra=movable)


def test_actor_constructor_rejects_mixed_args(ray_start_regular_shared):
    producer = Producer.remote()

    movable = producer.produce.options(_consume_once=True).remote(1)
    plain = producer.produce.remote(2)
    with pytest.raises(ValueError, match="argument to an actor constructor"):
        Holder.remote(plain, movable)
    with pytest.raises(ValueError, match="argument to an actor constructor"):
        Holder.remote(plain, extra=movable)


def test_normal_task_rejects_consume_once_arg(ray_start_regular_shared):
    producer = Producer.remote()

    ref = producer.produce.options(_consume_once=True).remote(1)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(ref)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(b"x" * (1024 * 1024), ref)

    assert ray.get(normal_task.remote(producer.produce.remote(2))) == 2


def test_actor_constructor_rejects_consume_once_arg(ray_start_regular_shared):
    producer = Producer.remote()

    ref = producer.produce.options(_consume_once=True).remote(1)
    with pytest.raises(ValueError, match="argument to an actor constructor"):
        Holder.remote(ref)


def test_all_returns_are_consume_once(ray_start_regular_shared):
    producer = Producer.remote()

    refs = producer.produce_pair.options(num_returns=2, _consume_once=True).remote()
    assert ray.get(refs) == [1, 2]
    for ref in refs:
        with pytest.raises(ValueError, match="argument to a task"):
            normal_task.remote(ref)


@pytest.mark.parametrize("num_returns", [None, "streaming"])
def test_consume_once_rejected_for_streaming_generator_methods(
    ray_start_regular_shared, num_returns
):
    producer = Producer.remote()
    # A generator method without num_returns defaults to "streaming".
    options = {} if num_returns is None else {"num_returns": num_returns}

    with pytest.raises(ValueError, match="streaming generator"):
        producer.generate.options(_consume_once=True, **options).remote()


def test_consume_once_rejected_for_dynamic_generator_methods(
    ray_start_regular_shared,
):
    producer = Producer.remote()

    with pytest.raises(ValueError, match="num_returns='dynamic'"):
        producer.generate.options(num_returns="dynamic", _consume_once=True).remote()
    with pytest.raises(ValueError, match="num_returns='dynamic'"):
        producer.generate_dynamic.options(_consume_once=True).remote()


def test_consume_once_rejected_with_tensor_transport(ray_start_regular_shared):
    producer = Producer.remote()

    with pytest.raises(ValueError, match="tensor_transport"):
        producer.produce._remote(args=[1], _consume_once=True, tensor_transport="NCCL")


def test_consume_once_must_be_bool(ray_start_regular_shared):
    producer = Producer.remote()

    with pytest.raises(TypeError, match="_consume_once must be a bool"):
        producer.produce.options(_consume_once=1).remote(1)


def test_consume_once_only_accepted_on_actor_method_options(
    ray_start_regular_shared,
):
    with pytest.raises(ValueError, match="Invalid option keyword _consume_once"):
        normal_task.options(_consume_once=True)

    with pytest.raises(ValueError, match="Invalid option keyword _consume_once"):

        @ray.remote(_consume_once=True)
        def f():
            pass

    with pytest.raises(ValueError, match="Invalid option keyword _consume_once"):
        Producer.options(_consume_once=True)

    with pytest.raises(AssertionError, match="_consume_once"):

        @ray.remote
        class A:
            @ray.method(_consume_once=True)
            def m(self):
                pass


def move_state(ref):
    return ray._private.worker.global_worker.core_worker.get_move_state(ref)


def test_move_state_transitions(ray_start_regular_shared):
    producer = Producer.remote()
    consumer = Consumer.remote()

    ref = producer.produce.options(_consume_once=True).remote(1)
    plain = producer.produce.remote(2)
    assert move_state(ref) == "MOVABLE"
    assert move_state(plain) == "NOT_MOVABLE"
    assert move_state(ray.ObjectRef.nil()) is None

    # Reads don't consume the ref.
    assert ray.get(ref) == 1
    assert move_state(ref) == "MOVABLE"

    assert ray.get(consumer.consume.remote(ref)) == 1
    assert move_state(ref) == "MOVED"
    assert ray.get(ref) == 1

    ray.get(consumer.consume.remote(plain))
    assert move_state(plain) == "NOT_MOVABLE"


def test_second_consumer_rejected(ray_start_regular_shared):
    producer = Producer.remote()
    consumer = Consumer.remote()
    other_consumer = Consumer.remote()

    ref = producer.produce.options(_consume_once=True).remote(1)
    consumer.consume.remote(ref)
    with pytest.raises(ValueError, match="already passed to an actor task"):
        consumer.consume.remote(ref)
    with pytest.raises(ValueError, match="already passed to an actor task"):
        other_consumer.consume_all.remote(1, extra=ref)
    # A moved ref still can't go to a normal task or an actor constructor.
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(ref)
    with pytest.raises(ValueError, match="argument to an actor constructor"):
        Holder.remote(ref)


def test_rejected_submit_moves_no_args(ray_start_regular_shared):
    producer = Producer.remote()
    consumer = Consumer.remote()

    movable = producer.produce.options(_consume_once=True).remote(1)
    moved = producer.produce.options(_consume_once=True).remote(2)
    consumer.consume.remote(moved)

    with pytest.raises(ValueError, match="already passed to an actor task"):
        consumer.consume_all.remote(movable, moved)
    assert move_state(movable) == "MOVABLE"
    assert ray.get(consumer.consume.remote(movable)) == 1


def test_repeated_arg_is_one_consumption(ray_start_regular_shared):
    producer = Producer.remote()
    consumer = Consumer.remote()

    ref = producer.produce.options(_consume_once=True).remote(1)
    assert ray.get(consumer.consume_all.remote(ref, ref, extra=ref)) == [1, 1, 1]
    assert move_state(ref) == "MOVED"
    with pytest.raises(ValueError, match="already passed to an actor task"):
        consumer.consume.remote(ref)


def test_concurrent_consumers_only_one_wins(ray_start_regular_shared):
    producer = Producer.remote()
    consumer = Consumer.remote()
    ref = producer.produce.options(_consume_once=True).remote(1)

    num_threads = 16
    barrier = threading.Barrier(num_threads)
    results = []

    def consume():
        barrier.wait()
        try:
            results.append(consumer.consume.remote(ref))
        except ValueError as e:
            results.append(e)

    threads = [threading.Thread(target=consume) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if isinstance(r, ray.ObjectRef)]
    assert len(accepted) == 1
    assert ray.get(accepted[0]) == 1
    for r in results:
        if not isinstance(r, ray.ObjectRef):
            assert "already passed to an actor task" in str(r)


@pytest.mark.parametrize("consumed", [False, True])
def test_borrowing_rejected(ray_start_regular_shared, consumed):
    producer = Producer.remote()
    consumer = Consumer.remote()
    ref = producer.produce.options(_consume_once=True).remote(1)
    if consumed:
        ray.get(consumer.consume.remote(ref))

    match = "only be passed directly as an argument to one actor task"
    with pytest.raises(pickle.PicklingError, match=match):
        consumer.consume.remote([ref])
    with pytest.raises(pickle.PicklingError, match=match):
        consumer.consume.remote({"ref": ref})
    with pytest.raises(pickle.PicklingError, match=match):
        consumer.consume.remote([ref, ref])
    with pytest.raises(pickle.PicklingError, match=match):
        ray.put([ref])
    assert move_state(ref) == ("MOVED" if consumed else "MOVABLE")


@pytest.mark.parametrize("method", ["return_ref", "return_nested_ref", "kept_ref"])
def test_returning_consume_once_ref_rejected(ray_start_regular_shared, method):
    owner = Owner.remote()
    if method == "kept_ref":
        ray.get(owner.keep_ref.remote())
        method = "return_kept_ref"

    with pytest.raises(ray.exceptions.RayTaskError, match="_consume_once=True") as e:
        ray.get(getattr(owner, method).remote())
    assert isinstance(e.value, pickle.PicklingError)
    assert ray.get(owner.ping.remote()) == "alive"


def test_rejected_nested_arg_submits_nothing_and_leaks_nothing(
    ray_start_regular_shared,
):
    producer = Producer.remote()
    consumer = Consumer.remote()
    ref = producer.produce.options(_consume_once=True).remote(1)
    core_worker = ray._private.worker.global_worker.core_worker
    refs_before = set(core_worker.get_all_reference_counts())

    # The large by-value arg is put into plasma before the nested ref is reached.
    with pytest.raises(pickle.PicklingError, match="_consume_once=True"):
        consumer.consume_all.remote(b"x" * (1024 * 1024), [ref])
    assert set(core_worker.get_all_reference_counts()) == refs_before
    assert ray.get(consumer.get_num_calls.remote()) == 0


def test_consume_once_ref_in_dataclass_arg_rejected(ray_start_regular_shared):
    @dataclasses.dataclass
    class Box:
        ref: ray.ObjectRef

    producer = Producer.remote()
    consumer = Consumer.remote()
    ref = producer.produce.options(_consume_once=True).remote(1)

    with pytest.raises(pickle.PicklingError, match="_consume_once=True"):
        consumer.consume.remote(Box(ref))


def assert_unserializable_cause(error):
    # The original exception can't be sent, so the cause becomes a RayError that
    # explains why, and the caller can't catch it as a RuntimeError.
    assert not isinstance(error, RuntimeError)
    assert "RuntimeError isn't serializable" in str(error.cause)
    assert "_consume_once=True" in str(error.cause)


@pytest.mark.parametrize("owner_kind", ["sync", "threaded", "async"])
def test_exception_carrying_consume_once_ref_keeps_actor_alive(
    ray_start_regular_shared, owner_kind
):
    if owner_kind == "sync":
        owner = Owner.remote()
    elif owner_kind == "threaded":
        owner = Owner.options(max_concurrency=4).remote()
    else:
        owner = AsyncOwner.remote()

    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get(owner.raise_with_ref.remote())
    assert_unserializable_cause(exc_info.value)
    assert ray.get(owner.ping.remote()) == "alive"


def test_exception_carrying_consume_once_ref_in_normal_task(
    ray_start_regular_shared,
):
    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get(raise_with_ref_task.remote())
    assert_unserializable_cause(exc_info.value)
    assert ray.get(normal_task.remote(1)) == 1


def test_exception_carrying_consume_once_ref_in_actor_init(ray_start_regular_shared):
    @ray.remote
    class RaisesInInit:
        def __init__(self):
            ref = Producer.remote().produce.options(_consume_once=True).remote(1)
            raise RuntimeError("boom", ref)

        def ping(self):
            return "alive"

    # Same as for any exception in __init__: actor creation fails.
    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(RaisesInInit.remote().ping.remote())


def test_capturing_consume_once_ref_rejected(ray_start_regular_shared):
    ref = Producer.remote().produce.options(_consume_once=True).remote(1)

    @ray.remote
    def captures_ref():
        return ray.get(ref)

    @ray.remote
    class CapturesRef:
        def get(self):
            return ray.get(ref)

    with pytest.raises(pickle.PicklingError, match="_consume_once=True"):
        captures_ref.remote()
    with pytest.raises(pickle.PicklingError, match="_consume_once=True"):
        CapturesRef.remote()
    with pytest.raises(pickle.PicklingError, match="_consume_once=True"):
        ray.cloudpickle.dumps([ref])
    assert move_state(ref) == "MOVABLE"
    assert ray.get(normal_task.remote(1)) == 1


def test_streaming_generator_yielding_consume_once_ref_rejected(
    ray_start_regular_shared,
):
    owner = Owner.remote()

    gen = owner.yield_ref.remote()
    assert ray.get(next(gen)) == 1
    with pytest.raises(ray.exceptions.RayTaskError, match="_consume_once=True"):
        ray.get(next(gen))
    assert ray.get(owner.ping.remote()) == "alive"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
