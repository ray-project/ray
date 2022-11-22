import ray
from time import sleep

@ray.remote
def f(i):
  sleep(i)
  if i == 3:
    raise ValueError

@ray.remote
def g(*args):
  pass

xs = [f.bind(i) for i in range(10)]
y = g.bind(*xs)
ray.get(y.execute())
