import time
import ray
ray.init()

@ray.remote
def f():
    pass

@ray.remote
class A:
    def f(self):
        pass

start = time.time()
bad_env = {"conda": {"dependencies": ["this_doesnt_exist"]}}
# with pytest.raises(
#     RuntimeEnvSetupError,
#     # The actual error message should be included in the exception.
#     match="No such file or directory",
# ):
ray.get(f.options(runtime_env=bad_env).remote())