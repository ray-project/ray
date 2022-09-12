# __program_start__
import ray
from ray import ObjectRefGenerator

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

# Returns an ObjectRef[ObjectRefGenerator].
dynamic_ref = split.remote(array_ref, block_size)
print(dynamic_ref)

i = -1
ref_generator = ray.get(dynamic_ref)
print(ref_generator)
for i, ref in enumerate(ref_generator):
    # Each ObjectRefGenerator iteration returns an ObjectRef.
    assert len(ray.get(ref)) <= block_size
num_blocks_generated = i + 1
array_size = len(ray.get(array_ref))
assert array_size <= num_blocks_generated * block_size
print(f"Split array of size {array_size} into {num_blocks_generated} blocks of "
      f"size {block_size} each.")

# NOTE: The dynamic_ref points to the generated ObjectRefs. Make sure that this
# ObjectRef goes out of scope so that Ray can garbage-collect the internal
# ObjectRefs.
del dynamic_ref
# __dynamic_generator_end__
# fmt: on


# fmt: off
# __dynamic_generator_pass_start__
@ray.remote
def get_size(ref_generator : ObjectRefGenerator):
    print(ref_generator)
    num_elements = 0
    for ref in ref_generator:
        array = ray.get(ref)
        assert len(array) <= block_size
        num_elements += len(array)
    return num_elements


# Returns an ObjectRef[ObjectRefGenerator].
dynamic_ref = split.remote(array_ref, block_size)
assert array_size == ray.get(get_size.remote(dynamic_ref))

# This also works, but should be avoided because you have to call an additional
# `ray.get`, which blocks the driver.
ref_generator = ray.get(dynamic_ref)
assert array_size == ray.get(get_size.remote(ref_generator))
# __dynamic_generator_pass_end__
# fmt: on
