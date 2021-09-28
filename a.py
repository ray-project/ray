import ray
ray.init()

@ray.remote 
def some_function(x: int, y: int, z, *arg) -> float:
    return x / y
a = ray.put([1])
ray.get(some_function.remote(1, 0, a, 5))
