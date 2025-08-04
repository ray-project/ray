import ray

# Put the values (1, 2, 3) into Ray's object store.
a, b, c = ray.put(1), ray.put(2), ray.put(3)


@ray.remote
def print_via_capture():
    """This function prints the values of (a, b, c) to stdout."""
    print(ray.get([a, b, c]))


# Passing object references via closure-capture. Inside the `print_via_capture`
# function, the global object refs (a, b, c) can be retrieved and printed.
print_via_capture.remote()
# -> prints [1, 2, 3]
