import ray

ray.init()


@ray.remote
def f(a: int) -> str:
    return "a = {}".format(a + 1)


@ray.remote
def g(s: str) -> str:
    return s + " world"


@ray.remote
def h(a: str, b: int) -> str:
    return a


# Make sure the function arg is check
print(f.remote(1))
object_ref_str = f.remote(1)

# Make sure the ObjectRef[T] variant of function arg is checked
print(g.remote(object_ref_str))

# Make sure there can be mixed T0 and ObjectRef[T1] for args
print(h.remote(object_ref_str, 100))

# Make sure the return type is checked.
xy = ray.get(object_ref_str) + "y"
