import ray
import ray.cluster_utils


def test_cross_language_cpp():
    ray.init(
        job_config=ray.job_config.JobConfig(code_search_path=["bazel-bin/cpp/plus.so"])
    )
    obj = ray.cpp_function("Plus1").remote(1)
    assert 2 == ray.get(obj)
