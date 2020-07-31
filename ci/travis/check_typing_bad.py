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


# Does not typecheck:
a = h.remote(1, 1)
b = f.remote("hello")
c = f.remote(1, 1)
d = f.remote(1) + 1

# Check return type
ref_to_str = f.remote(1)
unwrapped_str = ray.get(ref_to_str)
unwrapped_str + 100  # Should fail type check
