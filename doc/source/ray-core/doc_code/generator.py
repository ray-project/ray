# __program_start__
import ray
from ray import DynamicObjectRefGenerator

# fmt: off
# __dynamic_generator_start__
import numpy as np


@ray.remote(num_returns="dynamic")
def split(array, chunk_size):
    while len(array) > 0:
        yield array[:chunk_size]
        array = array[chunk_size:]


array_ref = ray.put(np.zeros(np.random.randint(1000_000)))
block_size = 1000

# Returns an ObjectRef[DynamicObjectRefGenerator].
dynamic_ref = split.remote(array_ref, block_size)
print(dynamic_ref)
# ObjectRef(c8ef45ccd0112571ffffffffffffffffffffffff0100000001000000)

i = -1
ref_generator = ray.get(dynamic_ref)
print(ref_generator)
# <ray._raylet.DynamicObjectRefGenerator object at 0x7f7e2116b290>
for i, ref in enumerate(ref_generator):
    # Each DynamicObjectRefGenerator iteration returns an ObjectRef.
    assert len(ray.get(ref)) <= block_size
num_blocks_generated = i + 1
array_size = len(ray.get(array_ref))
assert array_size <= num_blocks_generated * block_size
print(f"Split array of size {array_size} into {num_blocks_generated} blocks of "
      f"size {block_size} each.")
# Split array of size 63153 into 64 blocks of size 1000 each.

# NOTE: The dynamic_ref points to the generated ObjectRefs. Make sure that this
# ObjectRef goes out of scope so that Ray can garbage-collect the internal
# ObjectRefs.
del dynamic_ref
# __dynamic_generator_end__
# fmt: on


# fmt: off
# __dynamic_generator_pass_start__
@ray.remote
def get_size(ref_generator : DynamicObjectRefGenerator):
    print(ref_generator)
    num_elements = 0
    for ref in ref_generator:
        array = ray.get(ref)
        assert len(array) <= block_size
        num_elements += len(array)
    return num_elements


# Returns an ObjectRef[DynamicObjectRefGenerator].
dynamic_ref = split.remote(array_ref, block_size)
assert array_size == ray.get(get_size.remote(dynamic_ref))
# (get_size pid=1504184)
# <ray._raylet.DynamicObjectRefGenerator object at 0x7f81c4250ad0>

# This also works, but should be avoided because you have to call an additional
# `ray.get`, which blocks the driver.
ref_generator = ray.get(dynamic_ref)
assert array_size == ray.get(get_size.remote(ref_generator))
# (get_size pid=1504184)
# <ray._raylet.DynamicObjectRefGenerator object at 0x7f81c4251b50>
# __dynamic_generator_pass_end__
# fmt: on


# fmt: off
# __generator_errors_start__
@ray.remote
def generator():
    for i in range(2):
        yield i
    raise Exception("error")


ref1, ref2, ref3, ref4 = generator.options(num_returns=4).remote()
assert ray.get([ref1, ref2]) == [0, 1]
# All remaining ObjectRefs will contain the error.
try:
    ray.get([ref3, ref4])
except Exception as error:
    print(error)

dynamic_ref = generator.options(num_returns="dynamic").remote()
ref_generator = ray.get(dynamic_ref)
ref1, ref2, ref3 = ref_generator
assert ray.get([ref1, ref2]) == [0, 1]
# Generators with num_returns="dynamic" will store the exception in the final
# ObjectRef.
try:
    ray.get(ref3)
except Exception as error:
    print(error)
# __generator_errors_end__
# fmt: on

# fmt: off
# __generator_errors_unsupported_start__
# Generators that yield more values than expected currently do not throw an
# exception (the error is only logged).
# See https://github.com/ray-project/ray/issues/28689.
ref1, ref2 = generator.options(num_returns=2).remote()
assert ray.get([ref1, ref2]) == [0, 1]
"""
(generator pid=2375938) 2022-09-28 11:08:51,386 ERROR worker.py:755 --
    Unhandled error: Task threw exception, but all return values already
    created.  This should only occur when using generator tasks.
...
"""
# __generator_errors_unsupported_end__
# fmt: on
