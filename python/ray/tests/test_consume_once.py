import sys

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
    def consume(self, value):
        return value

    def consume_all(self, *values, extra=None):
        return list(values) + [extra]


@ray.remote
class Holder:
    def __init__(self, *values, extra=None):
        self.values = values


@ray.remote
def normal_task(*values, extra=None):
    return values[0]


def test_consume_once_ref_can_be_read_and_passed_to_actor_task(
    ray_start_regular_shared,
):
    producer = Producer.remote()
    consumer = Consumer.remote()

    ref = producer.produce.options(consume_once=True).remote(1)
    assert ray.get(ref) == 1
    assert ray.get(consumer.consume.remote(ref)) == 1


def test_actor_task_accepts_mixed_args(ray_start_regular_shared):
    producer = Producer.remote()
    consumer = Consumer.remote()

    movable_1 = producer.produce.options(consume_once=True).remote(1)
    movable_2 = producer.produce.options(consume_once=True).remote(2)
    plain = producer.produce.remote(3)
    result = consumer.consume_all.remote(movable_1, plain, 4, extra=movable_2)
    assert ray.get(result) == [1, 3, 4, 2]


def test_normal_task_rejects_mixed_args(ray_start_regular_shared):
    producer = Producer.remote()

    movable = producer.produce.options(consume_once=True).remote(1)
    plain = producer.produce.remote(2)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(plain, movable)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(movable, plain)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(plain, extra=movable)


def test_actor_constructor_rejects_mixed_args(ray_start_regular_shared):
    producer = Producer.remote()

    movable = producer.produce.options(consume_once=True).remote(1)
    plain = producer.produce.remote(2)
    with pytest.raises(ValueError, match="argument to an actor constructor"):
        Holder.remote(plain, movable)
    with pytest.raises(ValueError, match="argument to an actor constructor"):
        Holder.remote(plain, extra=movable)


def test_normal_task_rejects_consume_once_arg(ray_start_regular_shared):
    producer = Producer.remote()

    ref = producer.produce.options(consume_once=True).remote(1)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(ref)
    with pytest.raises(ValueError, match="argument to a task"):
        normal_task.remote(b"x" * (1024 * 1024), ref)

    assert ray.get(normal_task.remote(producer.produce.remote(2))) == 2


def test_actor_constructor_rejects_consume_once_arg(ray_start_regular_shared):
    producer = Producer.remote()

    ref = producer.produce.options(consume_once=True).remote(1)
    with pytest.raises(ValueError, match="argument to an actor constructor"):
        Holder.remote(ref)


def test_all_returns_are_consume_once(ray_start_regular_shared):
    producer = Producer.remote()

    refs = producer.produce_pair.options(num_returns=2, consume_once=True).remote()
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
        producer.generate.options(consume_once=True, **options).remote()


def test_consume_once_rejected_for_dynamic_generator_methods(
    ray_start_regular_shared,
):
    producer = Producer.remote()

    with pytest.raises(ValueError, match="num_returns='dynamic'"):
        producer.generate.options(num_returns="dynamic", consume_once=True).remote()
    with pytest.raises(ValueError, match="num_returns='dynamic'"):
        producer.generate_dynamic.options(consume_once=True).remote()


def test_consume_once_rejected_with_tensor_transport(ray_start_regular_shared):
    producer = Producer.remote()

    with pytest.raises(ValueError, match="tensor_transport"):
        producer.produce._remote(args=[1], consume_once=True, tensor_transport="NCCL")


def test_consume_once_must_be_bool(ray_start_regular_shared):
    producer = Producer.remote()

    with pytest.raises(TypeError, match="consume_once must be a bool"):
        producer.produce.options(consume_once=1).remote(1)


def test_consume_once_only_accepted_on_actor_method_options(
    ray_start_regular_shared,
):
    with pytest.raises(ValueError, match="Invalid option keyword consume_once"):
        normal_task.options(consume_once=True)

    with pytest.raises(ValueError, match="Invalid option keyword consume_once"):

        @ray.remote(consume_once=True)
        def f():
            pass

    with pytest.raises(ValueError, match="Invalid option keyword consume_once"):
        Producer.options(consume_once=True)

    with pytest.raises(AssertionError, match="consume_once"):

        @ray.remote
        class A:
            @ray.method(consume_once=True)
            def m(self):
                pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
