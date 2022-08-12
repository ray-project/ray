# This file is used by CrossLanguageInvocationTest.java to test cross-language
# invocation.

import ray


@ray.remote
def py_return_input(v):
    return v


@ray.remote
def py_func_python_raise_exception():
    1 / 0


@ray.remote
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value).encode("utf-8")
