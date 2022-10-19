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

# simple reduce
reduce_output = ray.get(reduce.remote(sum, *map_outputs))
assert reduce_output == 9900

# tree reduce
intermediate_reduce_outputs = [
    reduce.remote(sum, *map_outputs[i * 20 : (i + 1) * 20]) for i in range(5)
]
reduce_output = ray.get(reduce.remote(sum, *intermediate_reduce_outputs))
assert reduce_output == 9900
# __parallel_map_reduce_end__
