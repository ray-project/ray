import ray
import ray.cluster_utils
from ray.exceptions import CrossLanguageError
from ray.exceptions import RayActorError
import pytest


def test_cross_language_cpp():
    ray.init(
        job_config=ray.job_config.JobConfig(
            code_search_path=["../../plus.so:../../counter.so"]
        )
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


def test_cross_language_cpp_actor():
    actor = ray.cross_language.cpp_actor_class("CreateCounter", "Counter").remote()
    obj = actor.Plus1.remote()
    assert 1 == ray.get(obj)

    actor1 = ray.cross_language.cpp_actor_class(
        "RAY_FUNC(Counter::FactoryCreate)", "Counter"
    ).remote("invalid arg")
    obj = actor1.Plus1.remote()
    with pytest.raises(RayActorError):
        ray.get(obj)

    actor1 = ray.cross_language.cpp_actor_class(
        "RAY_FUNC(Counter::FactoryCreate)", "Counter"
    ).remote()

    obj = actor1.Plus1.remote()
    assert 1 == ray.get(obj)

    obj = actor1.Add.remote(2)
    assert 3 == ray.get(obj)

    obj2 = actor1.ExceptionFunc.remote()
    with pytest.raises(CrossLanguageError):
        ray.get(obj2)

    obj3 = actor1.NotExistFunc.remote()
    with pytest.raises(CrossLanguageError):
        ray.get(obj3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
