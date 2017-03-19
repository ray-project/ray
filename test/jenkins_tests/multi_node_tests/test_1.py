import ray
import testenv

@ray.remote
def f(x):
  return x + "abc"

if __name__ == "__main__":
  ray.init(redis_address=os.environ["RAY_REDIS_ADDRESS"])
  ray.get([f.remote(i) for i in range(5)])
