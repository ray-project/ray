import ray

ray.init()

# Test a simple function.


@ray.remote
def f_simple():
    return 1


for i in range(100):
    print(i)
    assert ray.get(f_simple.remote()) == 1
