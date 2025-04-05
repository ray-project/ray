import pytest
import ray
import ray.cluster_utils
from ray.exceptions import CrossLanguageError, RayActorError

# Additional imports for AI features
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

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

    large_list = [0] * 100000  # Renamed variable from `list` to `large_list`
    obj4 = ray.cross_language.cpp_function("ReturnLargeArray").remote(large_list)
    assert large_list == ray.get(obj4)

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

    # AI Feature: Testing model predictions
    model = LinearRegression()
    X_train = np.array([[1], [2], [3], [4], [5]])
    y_train = np.array([2, 4, 6, 8, 10])
    model.fit(X_train, y_train)

    def predict_remote(X):
        return ray.remote(lambda x: model.predict(np.array(x).reshape(-1, 1)).tolist()).remote(X)

    predictions = ray.get(predict_remote([[6], [7], [8]]))
    expected_predictions = model.predict(np.array([[6], [7], [8]]).reshape(-1, 1)).tolist()
    assert predictions == expected_predictions

    # Evaluating model performance
    y_pred = model.predict(X_train)
    mse = mean_squared_error(y_train, y_pred)
    assert mse < 1e-10, f"Mean Squared Error is too high: {mse}"

def test_cross_language_cpp_actor():
    actor = ray.cross_language.cpp_actor_class(
        "RAY_FUNC(Counter::FactoryCreate)", "Counter"
    ).remote()
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
