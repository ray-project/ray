# This file is used by CrossLanguageInvocationTest.java to test cross-language
# invocation.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

import ray


@ray.remote
def py_func(value):
    assert isinstance(value, bytes)
    return b"Response from Python: " + value


@ray.remote
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value).encode("utf-8") if six.PY3 else str(self.value)
