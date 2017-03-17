import ray
import testenv

@ray.remote
def f(x):
    return x

if __name__ == "__main__":
    testenv.Env().ray_init()
    ray.get([f.remote(i) for i in range(100)])