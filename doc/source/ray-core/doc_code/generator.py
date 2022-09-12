# __program_start__
import ray

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

ref_generator = split.remote(array_ref, block_size)  # Returns an ObjectRefGenerator.
print(ref_generator)

i = -1
# NOTE: When the generator is iterated for the first time, this will block
# until the task is complete and the number of ObjectRefs returned by the task
# is known. This is unlike remote functions with a static num_returns, where
# ObjectRefs can be passed to another function before the task is complete.
for i, ref in enumerate(ref_generator):
    # Each ObjectRefGenerator iteration returns an ObjectRef.
    assert len(ray.get(ref)) <= block_size
num_blocks_generated = i + 1
array_size = len(ray.get(array_ref))
assert array_size <= num_blocks_generated * block_size
print(f"Split array of size {array_size} into {num_blocks_generated} blocks of "
      f"size {block_size} each.")
# __dynamic_generator_end__
# fmt: on


# fmt: off
# __dynamic_generator_ray_get_start__
ref_generator = split.remote(array_ref, block_size)
value_generator = ray.get(ref_generator)
print(value_generator)
for array in value_generator:
    assert len(array) <= block_size
# __dynamic_generator_ray_get_end__
# fmt: on


# fmt: off
# __dynamic_generator_pass_start__
@ray.remote
def get_size(ref_generator):
    value_generator = ray.get(ref_generator)
    print(value_generator)
    num_elements = 0
    for array in value_generator:
        assert len(array) <= block_size
        num_elements += len(array)
    return num_elements


ref_generator = split.remote(array_ref, block_size)
assert array_size == ray.get(get_size.remote(ref_generator))
# __dynamic_generator_pass_end__
# fmt: on
