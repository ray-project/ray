import ray


def f(row):
    return {"result": [[], [1, 2]]}


ray.data.range(1).map(f).materialize()
