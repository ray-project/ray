from ray.streaming import function
from ray.streaming import operator
from ray.streaming.operator import OperatorType
from ray.streaming.runtime import gateway_client


def test_create_operator_with_func():
    map_func = function.SimpleMapFunction(lambda x: x)
    map_operator = operator.create_operator_with_func(map_func)
    assert type(map_operator) is operator.MapOperator


class MapFunc(function.MapFunction):
    def map(self, value):
        return str(value)


class EmptyOperator(operator.StreamOperator):
    def __init__(self):
        super().__init__(function.EmptyFunction())

    def operator_type(self) -> OperatorType:
        return OperatorType.ONE_INPUT


def test_load_operator():
    # function_bytes, module_name, class_name,
    descriptor_func_bytes = gateway_client.serialize(
        [None, __name__, MapFunc.__name__, "MapFunction"])
    descriptor_op_bytes = gateway_client.serialize(
        [descriptor_func_bytes, "", ""])
    map_operator = operator.load_operator(descriptor_op_bytes)
    assert type(map_operator) is operator.MapOperator
    descriptor_op_bytes = gateway_client.serialize(
        [None, __name__, EmptyOperator.__name__])
    test_operator = operator.load_operator(descriptor_op_bytes)
    assert isinstance(test_operator, EmptyOperator)
