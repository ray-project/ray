from ray.experimental.client import ray
import time

ray.connect("localhost:50050")

@ray.remote
def plus2(x):
    return x + 2

print(ray.get(plus2.remote(24)))

@ray.remote
def fact(x):
    print("Executing", x)
    if x <= 0:
        return 1
    return x * ray.get(fact.remote(x - 1))


obj = fact.remote(20)
print(ray.get(obj))
