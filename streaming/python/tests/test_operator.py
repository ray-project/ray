from ray.streaming import operator
from ray.streaming import function


def test_create_operator():
    map_func = function.SimpleMapFunction(lambda x: x)
    map_operator = operator.create_operator(map_func)
    assert type(map_operator) is operator.MapOperator
