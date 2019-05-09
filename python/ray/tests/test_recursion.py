# This test is not inside of test_basic.py because when a recursive remote
# function is defined inside of another function, we currently can't handle
# that.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


@ray.remote
def factorial(n):
    if n == 0:
        return 1
    return n * ray.get(factorial.remote(n - 1))


def test_recursion(ray_start_regular):
    assert ray.get(factorial.remote(0)) == 1
    assert ray.get(factorial.remote(1)) == 1
    assert ray.get(factorial.remote(2)) == 2
    assert ray.get(factorial.remote(3)) == 6
    assert ray.get(factorial.remote(4)) == 24
    assert ray.get(factorial.remote(5)) == 120
