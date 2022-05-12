import ray


@ray.remote
def large_object_function():
    # Large object is stored in the local object store
    # and available in the distributed memory,
    # instead of returning inline directly to the caller.
    return [1] * (1024 * 1024)


@ray.remote
def small_object_function():
    # Small object is returned inline directly to the caller,
    # instead of storing in the distributed memory.
    return [1]


@ray.remote
def consume_function(data):
    return len(data)


large_object = large_object_function.remote()
small_object = small_object_function.remote()

# Ray will try to run consume_function on the same node
# where large_object_function runs.
consume_function.remote(large_object)

# Ray will try to spread consume_function across the entire cluster
# instead of only running on the node where large_object_function runs.
[
    consume_function.options(scheduling_strategy="SPREAD").remote(large_object)
    for i in range(10)
]

# Ray won't consider locality for scheduling consume_function
# since the argument is small and will be sent to the worker node inline directly.
consume_function.remote(small_object)
