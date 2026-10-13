import ray


@ray.remote
def echo_and_get(x_list):  # List[ObjectRef]
    """This function prints its input values to stdout."""
    print("args:", x_list)
    print("values:", ray.get(x_list))


# Put the values (1, 2, 3) into Ray's object store.
a, b, c = ray.put(1), ray.put(2), ray.put(3)

# Passing an object as a nested argument to `echo_and_get`. Ray does not
# de-reference nested args, so `echo_and_get` sees the references.
echo_and_get.remote([a, b, c])
# -> prints args: [ObjectRef(...), ObjectRef(...), ObjectRef(...)]
#           values: [1, 2, 3]
