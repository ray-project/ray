import ray

ray.init()

@ray.remote
def f():
    for i in range(3):
        yield i

gen = f.options(num_returns="streaming").remote()

r, ur = ray.wait([gen], num_returns=1)
assert len(r) == 1
assert ray.get(r) == [0], ray.get(r)
assert len(ur) == 1
r, ur = ray.wait([gen], num_returns=1)
assert len(r) == 1
assert ray.get(r) == [1]
assert len(ur) == 1
r, ur = ray.wait([gen], num_returns=1)
assert len(r) == 1
assert ray.get(r) == [2]
assert len(ur) == 1
r, ur = ray.wait([gen], num_returns=1)
assert len(r) == 1
assert isinstance(r[0], ray._raylet.StreamingObjectRefGenerator)
assert len(ur) == 0
