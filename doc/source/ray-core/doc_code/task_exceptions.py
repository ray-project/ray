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
# __unserializable_exceptions_begin__

import threading

class UnserializableException(Exception):
    def __init__(self):
        self.lock = threading.Lock()

@ray.remote
def raise_unserializable_error():
    raise UnserializableException

try:
    ray.get(raise_unserializable_error.remote())
except ray.exceptions.RayTaskError as e:
    print(e)
    # ray::raise_unserializable_error() (pid=328577, ip=172.31.5.154)
    #   File "/home/ubuntu/ray/tmp~/main.py", line 25, in raise_unserializable_error
    #     raise UnserializableException
    # UnserializableException
    print(type(e.cause))
    # <class 'ray.exceptions.RayError'>
    print(e.cause)
    # The original cause of the RayTaskError (<class '__main__.UnserializableException'>) isn't serializable: cannot pickle '_thread.lock' object. Overwriting the cause to a RayError.

# __unserializable_exceptions_end__
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
    #   File "<$PWD>/task_exceptions.py", line 45, in raises_my_exc
    #     raise MyException("a user exception")
    # MyException: a user exception

# __catch_user_exceptions_end__
# __catch_user_final_exceptions_begin__
class MyFinalException(Exception):
    def __init_subclass__(cls, /, *args, **kwargs):
        raise TypeError("Cannot subclass this little exception class.")

@ray.remote
def raises_my_final_exc():
    raise MyFinalException("a *final* user exception")
try:
    ray.get(raises_my_final_exc.remote())
except ray.exceptions.RayTaskError as e:
    assert isinstance(e.cause, MyFinalException)
    print(e)
    # 2024-04-08 21:11:47,417 WARNING exceptions.py:177 -- User exception type <class '__main__.MyFinalException'> in RayTaskError can not be subclassed! This exception will be raised as RayTaskError only. You can use `ray_task_error.cause` to access the user exception. Failure in subclassing: Cannot subclass this little exception class.
    # ray::raises_my_final_exc() (pid=88226, ip=127.0.0.1)
    # File "<$PWD>/task_exceptions.py", line 66, in raises_my_final_exc
    #     raise MyFinalException("a *final* user exception")
    # MyFinalException: a *final* user exception
    print(type(e.cause))
    # <class '__main__.MyFinalException'>
    print(e.cause)
    # a *final* user exception
# __catch_user_final_exceptions_end__
# fmt: on
