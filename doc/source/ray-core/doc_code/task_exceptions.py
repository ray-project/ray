# flake8: noqa

# fmt: off
# __task_exceptions_begin__

import ray

@ray.remote
def f():
    raise Exception("the real error")

@ray.remote
def g(x):
    return


try:
    ray.get(f.remote())
except ray.exceptions.RayTaskError as e:
    print(e)
    # ray::f() (pid=71867, ip=XXX.XX.XXX.XX)
    #   File "errors.py", line 5, in f
    #     raise Exception("the real error")
    # Exception: the real error

try:
    ray.get(g.remote(f.remote()))
except ray.exceptions.RayTaskError as e:
    print(e)
    # ray::g() (pid=73085, ip=128.32.132.47)
    #   At least one of the input arguments for this task could not be computed:
    # ray.exceptions.RayTaskError: ray::f() (pid=73085, ip=XXX.XX.XXX.XX)
    #   File "errors.py", line 5, in f
    #     raise Exception("the real error")
    # Exception: the real error

# __task_exceptions_end__
# fmt: on
