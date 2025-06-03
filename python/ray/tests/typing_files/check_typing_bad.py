import ray
from ray.util.queue import Queue

ray.init()


@ray.remote
def f(a: int) -> str:
    return "a = {}".format(a + 1)


@ray.remote
def g(s: str) -> str:
    return s + " world"


@ray.remote
def h(a: str, b: int) -> str:
    return a


# Does not typecheck due to incorrect input type:
a = h.remote(1, 1)
b = f.remote("hello")
c = f.remote(1, 1)
d = f.remote(1) + 1

# Check return type
ref_to_str = f.remote(1)
unwrapped_str = ray.get(ref_to_str)
unwrapped_str + 100  # Fail

# Check ObjectRef[T] as args
f.remote(ref_to_str)  # Fail


# Does not type check due to incorrect input type
def put_float_in_int_queue():
    queue = Queue[int]()
    queue.put(1.0)  # Fail, float cannot be put into int queue


def get_str_from_float_queue():
    queue = Queue[float]()
    item: str = queue.get()  # Fail, str cannot be assigned to int
    return item


async def put_async():
    queue = Queue[int]()
    await queue.put_async(1.0)  # Fail, float cannot be put into int queue


async def get_async():
    queue = Queue[int]()
    item: str = await queue.get_async()  # Fail, str cannot be assigned to int
    return item
