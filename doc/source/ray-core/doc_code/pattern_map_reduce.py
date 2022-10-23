import ray

# __single_threaded_map_reduce_start__
items = list(range(100))
map_outputs = [i * 2 for i in items]
reduce_output = sum(map_outputs)
assert reduce_output == 9900
# __single_threaded_map_reduce_end__


# __parallel_map_reduce_start__
@ray.remote
def map(func, obj):
    return func(obj)


@ray.remote
def reduce(func, *objs):
    return func(objs)


map_outputs = [map.remote(lambda i: i * 2, i) for i in items]

# Simple reduce
# Here we use the * syntax to unpack map_outputs
# and pass each map output ObjectRef to reduce individually instead of passing
# the entire list as a single argument.
# When an argument is ObjectRef, Ray will resolve it
# and replace it with the actual object when calling the remote function.
reduce_output = ray.get(reduce.remote(sum, *map_outputs))
assert reduce_output == 9900

# Tree reduce
intermediate_reduce_outputs = [
    reduce.remote(sum, *map_outputs[i * 20 : (i + 1) * 20]) for i in range(5)
]
reduce_output = ray.get(reduce.remote(sum, *intermediate_reduce_outputs))
assert reduce_output == 9900
# __parallel_map_reduce_end__
