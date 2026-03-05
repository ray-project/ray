import ray

ray.init()

@ray.remote
def add(a, b):
    return a + b

ref = add.remote(2, 3)

res = ray.get(ref)

print(res)

ray.shutdown()


