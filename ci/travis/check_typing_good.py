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


print(f.remote(1))
x = f.remote(1)
print(g.remote(x))

# typechecks but doesn't run
print(ray.get(f.remote(x)))
