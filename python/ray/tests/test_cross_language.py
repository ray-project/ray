import pytest
import sys

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


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
