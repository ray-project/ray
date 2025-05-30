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


int_list = [1, 2, 3]
int_queue = Queue[int]()
int_item: int
for i in int_list:
    int_queue.put(i)
    int_item = int_queue.get()


float_list = [1.0, 2.0, 3.0]
float_queue = Queue[float]()
float_item: float
for i in float_list:
    float_queue.put(i)
    float_item = float_queue.get()


str_list = ["1", "2", "3"]
str_queue = Queue[str]()
str_item: str
for i in str_list:
    str_queue.put(i)
    std_item = str_queue.get()

class RandomObject:
    pass

random_object_list = [RandomObject(), RandomObject(), RandomObject()]
random_object_queue = Queue[RandomObject]()
random_object_item: RandomObject
for i in random_object_list:
    random_object_queue.put(i)
    random_object_item = random_object_queue.get()


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
