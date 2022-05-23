import ray

ray.init()


@ray.remote(num_cpus=1)
def f():
    pass


print("Hanging...")
ray.get(f.remote())
print("Success!")
