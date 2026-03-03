import ray
import sys

# Add the RAY_DEBUG_POST_MORTEM=1 environment variable
# if you want to activate post-mortem debugging
ray.init(
    runtime_env={
        "env_vars": {"RAY_DEBUG_POST_MORTEM": "1"},
    }
)


@ray.remote
def my_task(x):
    y = x * x
    breakpoint()  # Add a breakpoint in the Ray task.
    return y


@ray.remote
def post_mortem(x):
    x += 1
    raise Exception("An exception is raised.")
    return x


if len(sys.argv) == 1:
    ray.get(my_task.remote(10))
else:
    ray.get(post_mortem.remote(10))
