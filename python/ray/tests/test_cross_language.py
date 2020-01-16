import pytest

import ray
import ray.cluster_utils
import ray.test_utils


@ray.remote
def foo(x):
    return x


@ray.remote
class Actor(object):
    def __init__(self, x):
        self.x = x

    def bar(self, y):
        return self.x + y


def test_cross_language_python(shutdown_only):
    ray.init(load_code_from_local=True, include_java=True)

    arg = "foo"
    r = ray.python_function("ray.tests.test_cross_language", "foo").remote(arg)
    assert ray.get(r) == arg

    x = "x"
    y = "y"
    a = ray.python_actor_class("ray.tests.test_cross_language",
                               "Actor").remote(x)
    r = a.bar.remote(y)
    assert ray.get(r) == x + y

    # test for kwargs
    arg = "foo"
    r = ray.python_function("ray.tests.test_cross_language",
                            "foo").remote(x=arg)
    assert ray.get(r) == arg

    # test for kwargs
    x = "x"
    y = "y"
    a = ray.python_actor_class("ray.tests.test_cross_language",
                               "Actor").remote(x=x)
    r = a.bar.remote(y=y)
    assert ray.get(r) == x + y


def test_cross_language_check_load_code_from_local(shutdown_only):
    ray.init()

    with pytest.raises(Exception, match="--load-code-from-local"):
        ray.python_function("ray.tests.test_cross_language",
                            "foo").remote("arg1")

    with pytest.raises(Exception, match="--load-code-from-local"):
        ray.python_actor_class("ray.tests.test_cross_language",
                               "Actor").remote("arg1")


def test_cross_language_raise_kwargs(shutdown_only):
    ray.init(load_code_from_local=True, include_java=True)

    with pytest.raises(Exception, match="kwargs"):
        ray.java_function("a", "b").remote(x="arg1")

    with pytest.raises(Exception, match="kwargs"):
        ray.java_actor_class("a").remote(x="arg1")
