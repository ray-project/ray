import pytest
import ray
import ray.cluster_utils
from ray.exceptions import CrossLanguageError, RayActorError
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Ray
ray.init(
    job_config=ray.job_config.JobConfig(
        code_search_path=["../../plus.so", "../../counter.so"]
    )
)

def validate_result(expected, actual):
    """Simple AI-based validation function."""
    return expected == actual

def test_cross_language_cpp():
    logger.info("Testing cross-language CPP functions...")

    # Test basic function call
    obj = ray.cross_language.cpp_function("Plus1").remote(1)
    assert validate_result(2, ray.get(obj)), "Plus1 function failed."

    # Test error handling for incorrect arguments
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

    # Test function calls with different data types
    obj3 = ray.cross_language.cpp_function("Echo").remote("hello")
    assert validate_result("hello", ray.get(obj3)), "Echo function failed."

    list_data = [0] * 100000
    obj4 = ray.cross_language.cpp_function("ReturnLargeArray").remote(list_data)
    assert validate_result(list_data, ray.get(obj4)), "ReturnLargeArray function failed."

    map_data = {0: "hello"}
    obj5 = ray.cross_language.cpp_function("GetMap").remote(map_data)
    assert validate_result({0: "hello", 1: "world"}, ray.get(obj5)), "GetMap function failed."

    v = ["hello", "world"]
    obj6 = ray.cross_language.cpp_function("GetList").remote(v)
    assert validate_result(v, ray.get(obj6)), "GetList function failed."

    obj6 = ray.cross_language.cpp_function("GetArray").remote(v)
    assert validate_result(v, ray.get(obj6)), "GetArray function failed."

    tuple_data = [1, "hello"]
    obj7 = ray.cross_language.cpp_function("GetTuple").remote(tuple_data)
    assert validate_result(tuple_data, ray.get(obj7)), "GetTuple function failed."

    student = ["tom", 20]
    obj8 = ray.cross_language.cpp_function("GetStudent").remote(student)
    assert validate_result(student, ray.get(obj8)), "GetStudent function failed."

    students = {0: ["tom", 20], 1: ["jerry", 10]}
    obj9 = ray.cross_language.cpp_function("GetStudents").remote(students)
    assert validate_result(students, ray.get(obj9)), "GetStudents function failed."

def test_cross_language_cpp_actor():
    logger.info("Testing cross-language CPP actors...")

    actor = ray.cross_language.cpp_actor_class(
        "RAY_FUNC(Counter::FactoryCreate)", "Counter"
    ).remote()
    obj = actor.Plus1.remote()
    assert validate_result(1, ray.get(obj)), "Counter Plus1 actor function failed."

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
    assert validate_result(1, ray.get(obj)), "Counter Plus1 actor function failed."

    obj = actor1.Add.remote(2)
    assert validate_result(3, ray.get(obj)), "Counter Add actor function failed."

    obj2 = actor1.ExceptionFunc.remote()
    with pytest.raises(CrossLanguageError):
        ray.get(obj2)

    obj3 = actor1.NotExistFunc.remote()
    with pytest.raises(CrossLanguageError):
        ray.get(obj3)

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
