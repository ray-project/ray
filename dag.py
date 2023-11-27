import ctypes
import numpy as np


import ray

# TODO:
# - Compare against normal actor
# - Support normal actor calls on the streaming actor
# - Support other DAG shapes
# - Implement version that uses plasma unseal and ray.put/ray.wait for comparison
#   - Implement ray.wait that only calls Wait on the plasma store (current .Wait() also waits on the core worker's in-memory store)


ARR_SIZE = 10


# TODO: This requires the reader to have already finished reading.
def put(ptr, seq_num, val):
    assert ptr[0] < seq_num

    data_ptr = ctypes.c_char_p(ptr[1:].ctypes.data)
    ctypes.memmove(data_ptr, val.ctypes.data, len(val))

    seq_num_ptr = ctypes.c_char_p(ptr.ctypes.data)
    ctypes.memset(seq_num_ptr, seq_num, 1)


def get(ptr, seq_num):
    while ptr[0] != seq_num:
        assert ptr[0] <= seq_num, f"seq_num {ptr[0]} already passed {seq_num}"
        pass

    assert ptr[0] == seq_num, f"seq_num {ptr[0]} already passed {seq_num}"
    return ptr[1:]


@ray.remote
class StreamActor:
    def __init__(self):
        pass

    def execute(self, input_refs, output_refs, fn):
        input_ptr = ray.get(input_refs[0])
        output_ptr = ray.get(output_refs[0])

        # Continuous loop to get inputs and put outputs.
        seq_num = 1
        while True:
            # Poll input for the next sequence counter.
            val = get(input_ptr, seq_num)
            return_val = fn(val)
            #print("XXX", input_ptr, val, return_val)

            # Copy fn(input) to output_ptr, update the sequence counter.
            put(output_ptr, seq_num, return_val)
            #print("YYY", output_ptr)
            seq_num += 1


# TODO: Create and submit DAGs.
def driver():
    # Allocate all refs.
    # The first index will be a sequence counter.
    val = np.zeros(ARR_SIZE + 1, dtype=np.uint8)

    input_refs = [ray.put(val)]
    output_refs = [ray.put(val)]

    worker = StreamActor.remote()

    def times_two(array):
        #print(array)
        return array * 2

    # send the RPCs to actors to initialize.
    worker.execute.remote(input_refs, output_refs, times_two)

    input_ptr = ray.get(input_refs[0])
    output_ptr = ray.get(output_refs[0])
    for i in range(10):
        # "Submit" task.
        val = np.ones(ARR_SIZE, dtype=np.uint8) * i
        put(input_ptr, i + 1, val)
        #print("XXX", input_ptr)

        # Get return value.
        val = get(output_ptr, i + 1)
        #print("YYY", output_ptr)
        assert all(val == i * 2), (val, i)


if __name__ == "__main__":
    # Test shared-mem put and get.
    ref = ray.put(np.zeros(ARR_SIZE + 1, dtype=np.uint8))
    x = ray.get(ref)
    for i in range(3):
        put(x, i + 1, np.ones(ARR_SIZE, dtype=np.uint8) * i)
        assert x[0] == i + 1
        assert all(x[1:] == i)
        assert all(get(ray.get(ref), seq_num=i + 1) == i)


    driver()
