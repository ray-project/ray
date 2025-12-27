import sys

import pytest

import ray
import ray.cluster_utils


def test_cross_language_raise_kwargs(shutdown_only):
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))

    with pytest.raises(Exception, match="kwargs"):
        ray.cross_language.java_function("a", "b").remote(x="arg1")

    with pytest.raises(Exception, match="kwargs"):
        ray.cross_language.java_actor_class("a").remote(x="arg1")

    with pytest.raises(Exception, match="kwargs"):
        ray.cross_language.cpp_function("a").remote(x="arg1")

    with pytest.raises(Exception, match="kwargs"):
        ray.cross_language.cpp_actor_class("a", "b").remote(x="arg1")


def test_cross_language_raise_exception(shutdown_only):
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))

    class PythonObject(object):
        pass

    with pytest.raises(Exception, match="transfer"):
        ray.cross_language.java_function("a", "b").remote(PythonObject())

    with pytest.raises(Exception, match="transfer"):
        ray.cross_language.cpp_function("a").remote(PythonObject())


def test_function_id(shutdown_only):
    cpp_func = ray.cross_language.cpp_function("TestFunctionID")
    func_id = cpp_func._function_descriptor.function_id
    print(func_id)
    assert func_id is not None


def test_actor_function_id(shutdown_only):
    cpp_actor_class = ray.cross_language.cpp_actor_class(
        "TestActorFunction", "TestActor"
    )
    func_id = (
        cpp_actor_class.__ray_metadata__.actor_creation_function_descriptor.function_id
    )
    print(func_id)
    assert func_id is not None


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
