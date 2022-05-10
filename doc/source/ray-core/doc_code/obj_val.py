import ray

@ray.remote
def echo(x: int):
    """This function prints its input value to stdout."""
    print(x)

# Passing the literal value `1` to `echo`.
echo.remote(1)
# -> prints "1"

# Put the value `1` into Ray's object store.
object_ref = ray.put(1)

# Passing an object as a top-level argument to `echo`. Ray will de-reference top-level
# arguments, so `echo` will see the literal value `1` in this case as well.
echo.remote(object_ref)
# -> prints "1"
