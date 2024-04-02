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
# __catch_user_exceptions_begin__

class MyException(Exception):
    ...

@ray.remote
def raises_my_exc():
    raise MyException("a user exception")
try:
    ray.get(raises_my_exc.remote())
except MyException as e:
    print(e)
    # ray::raises_my_exc() (pid=15329, ip=127.0.0.1)
    #   File "/Users/ruiyangwang/gits/ray/doc/source/ray-core/doc_code/task_exceptions.py", line 45, in raises_my_exc
    #     raise MyException("a user exception")
    # MyException: a user exception

# __catch_user_exceptions_end__
# __catch_user_final_exceptions_begin__

from typing import final

@final
class MyFinalException(Exception):
    ...

@ray.remote
def raises_my_final_exc():
    raise MyFinalException("a *final* user exception")
try:
    ray.get(raises_my_final_exc.remote())
except ray.exceptions.RayTaskError as e:
    assert isinstance(e.cause, MyFinalException)
    print(e)
    # ray::raises_my_final_exc() (pid=26737, ip=127.0.0.1)
    #   File "/Users/ruiyangwang/gits/ray/doc/source/ray-core/doc_code/task_exceptions.py", line 66, in raises_my_final_exc
    #     raise MyFinalException("a *final* user exception")
    # MyFinalException: a *final* user exception
    print(type(e.cause))
    # <class '__main__.MyFinalException'>
    print(e.cause)
    # a *final* user exception
# __catch_user_final_exceptions_end__
# fmt: on
