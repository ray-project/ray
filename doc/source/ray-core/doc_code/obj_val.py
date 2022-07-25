import ray


@ray.remote
def echo(a: int, b: int, c: int):
    """This function prints its input values to stdout."""
    print(a, b, c)


# Passing the literal values (1, 2, 3) to `echo`.
echo.remote(1, 2, 3)
# -> prints "1 2 3"

# Put the values (1, 2, 3) into Ray's object store.
a, b, c = ray.put(1), ray.put(2), ray.put(3)

# Passing an object as a top-level argument to `echo`. Ray will de-reference top-level
# arguments, so `echo` will see the literal values (1, 2, 3) in this case as well.
echo.remote(a, b, c)
# -> prints "1 2 3"
