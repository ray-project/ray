import ray
import ray.cluster_utils
from ray.exceptions import CrossLanguageError
import pytest


def test_cross_language_cpp():
    ray.init(
        job_config=ray.job_config.JobConfig(code_search_path=["bazel-bin/cpp/plus.so"])
    )
    obj = ray.cross_language.cpp_function("Plus1").remote(1)
    assert 2 == ray.get(obj)

    obj1 = ray.cross_language.cpp_function("ThrowTask").remote()
    with pytest.raises(CrossLanguageError):
        ray.get(obj1)

    obj = ray.cross_language.cpp_function("Plus1").remote("invalid arg")
    with pytest.raises(CrossLanguageError):
        ray.get(obj)

    obj = ray.cross_language.cpp_function("Plus1").remote(1, 2)
    with pytest.raises(CrossLanguageError):
        ray.get(obj)

    obj = ray.cross_language.cpp_function("Plus1").remote()
    with pytest.raises(CrossLanguageError):
        ray.get(obj)

    obj2 = ray.cross_language.cpp_function("NotExsitTask").remote()
    with pytest.raises(CrossLanguageError):
        ray.get(obj2)

    obj3 = ray.cross_language.cpp_function("Echo").remote("hello")
    assert "hello" == ray.get(obj3)

    list = [0] * 100000
    obj4 = ray.cross_language.cpp_function("ReturnLargeArray").remote(list)
    assert list == ray.get(obj4)

    map = {0: "hello"}
    obj5 = ray.cross_language.cpp_function("GetMap").remote(map)
    assert {0: "hello", 1: "world"} == ray.get(obj5)

    v = ["hello", "world"]
    obj6 = ray.cross_language.cpp_function("GetList").remote(v)
    assert v == ray.get(obj6)

    obj6 = ray.cross_language.cpp_function("GetArray").remote(v)
    assert v == ray.get(obj6)

    tuple = [1, "hello"]
    obj7 = ray.cross_language.cpp_function("GetTuple").remote(tuple)
    assert tuple == ray.get(obj7)

    student = ["tom", 20]
    obj8 = ray.cross_language.cpp_function("GetStudent").remote(student)
    assert student == ray.get(obj8)

    students = {0: ["tom", 20], 1: ["jerry", 10]}
    obj9 = ray.cross_language.cpp_function("GetStudents").remote(students)
    assert students == ray.get(obj9)
