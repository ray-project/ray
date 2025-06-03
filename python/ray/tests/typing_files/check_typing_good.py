import ray
from typing import Generator
from ray import ObjectRef
from ray.util.queue import Queue

ray.init()


@ray.remote
def int_task() -> int:
    return 1


@ray.remote
def f(a: int) -> str:
    return "a = {}".format(a + 1)


@ray.remote
def g(s: str) -> str:
    return s + " world"


@ray.remote
def h(a: str, b: int) -> str:
    return a


def func(a: "ObjectRef[str]"):
    pass


# Make sure the function arg is check
print(f.remote(1))
object_ref_str = f.remote(1)
object_ref_int = int_task.remote()

# Make sure the ObjectRef[T] variant of function arg is checked
print(g.remote(object_ref_str))

# Make sure it is backward compatible after
# introducing generator types.
func(object_ref_str)

# Make sure there can be mixed T0 and ObjectRef[T1] for args
print(h.remote(object_ref_str, 100))

ready, unready = ray.wait([object_ref_str, object_ref_int])

# Make sure the return type is checked.
xy = ray.get(object_ref_str) + "y"


# Right now, we only check if it doesn't raise errors.
@ray.remote
def generator_1() -> Generator[int, None, None]:
    yield 1


gen = generator_1.remote()

"""
TODO(sang): Enable it.
Test generator

Generator can have 4 different output
per generator and async generator. See
https://docs.python.org/3/library/typing.html#typing.Generator
for more details.
"""

# @ray.remote
# def generator_1() -> Generator[int, None, None]:
#     yield 1


# @ray.remote
# def generator_2() -> Iterator[int]:
#     yield 1


# @ray.remote
# def generator_3() -> Iterable[int]:
#     yield 1


# gen: ObjectRefGenerator[int] = generator_1.remote()
# gen2: ObjectRefGenerator[int] = generator_2.remote()
# gen3: ObjectRefGenerator[int] = generator_3.remote()


# next_item: ObjectRef[int] = gen.__next__()


# @ray.remote
# async def async_generator_1() -> AsyncGenerator[int, None]:
#     yield 1


# @ray.remote
# async def async_generator_2() -> AsyncIterator[int]:
#     yield 1


# @ray.remote
# async def async_generator_3() -> AsyncIterable[int]:
#     yield 1


# gen4: ObjectRefGenerator[int] = async_generator_1.remote()
# gen5: ObjectRefGenerator[int] = async_generator_2.remote()
# gen6: ObjectRefGenerator[int] = async_generator_3.remote()


def int_queue():
    """Test the queue with int."""
    items = [1, 2, 3]
    queue = Queue[int]()
    for i in items:
        queue.put(i)
        item: int = queue.get()


def float_queue():
    """Test the queue with float."""
    items = [1.0, 2.0, 3.0]
    queue = Queue[float]()
    for i in items:
        queue.put(i)
        item: float = queue.get()


def random_object_queue():
    """Test the queue with random object."""

    class RandomObject:
        pass

    items = [RandomObject(), RandomObject(), RandomObject()]
    queue = Queue[RandomObject]()
    for i in items:
        queue.put(i)
        item: RandomObject = queue.get()


async def async_queue():
    items = ["1", "2", "3"]
    queue = Queue[str]()
    for i in items:
        await queue.put_async(i)
        item: str = await queue.get_async()
