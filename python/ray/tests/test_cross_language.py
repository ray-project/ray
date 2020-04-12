import pytest

import ray
import ray.cluster_utils
import ray.test_utils


def test_cross_language_raise_kwargs(shutdown_only):
    ray.init(load_code_from_local=True, include_java=True)

    with pytest.raises(Exception, match="kwargs"):
        ray.java_function("a", "b").remote(x="arg1")

    with pytest.raises(Exception, match="kwargs"):
        ray.java_actor_class("a").remote(x="arg1")


def test_cross_language_raise_exception(shutdown_only):
    ray.init(load_code_from_local=True, include_java=True)

    class PythonObject(object):
        pass

    with pytest.raises(Exception, match="transfer"):
        ray.java_function("a", "b").remote(PythonObject())
