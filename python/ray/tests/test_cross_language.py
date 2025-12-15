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


def test_cross_language_raise_exception(shutdown_only):
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))

    class PythonObject(object):
        pass

    with pytest.raises(Exception, match="transfer"):
        ray.cross_language.java_function("a", "b").remote(PythonObject())


def test_cpp_descriptor_resolves_python_actor_method(shutdown_only):
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))

    @ray.remote
    class SampleActor:
        def check_descriptor(self):
            worker = ray._private.worker.global_worker
            fm = worker.function_actor_manager
            fd = ray._raylet.CppFunctionDescriptor(
                "check_descriptor", "PYTHON", self.__class__.__name__
            )
            info = fm.get_execution_info(worker.current_job_id, fd)
            return info.function_name

    actor = SampleActor.remote()
    assert ray.get(actor.check_descriptor.remote()) == "check_descriptor"


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
