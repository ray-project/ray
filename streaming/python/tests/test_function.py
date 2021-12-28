from ray.streaming import function
from ray.streaming.runtime import gateway_client


def test_get_simple_function_class():
    simple_map_func_class = function._get_simple_function_class(function.MapFunction)
    assert simple_map_func_class is function.SimpleMapFunction


class MapFunc(function.MapFunction):
    def map(self, value):
        return str(value)


def test_load_function():
    # function_bytes, module_name, function_name/class_name,
    # function_interface
    descriptor_func_bytes = gateway_client.serialize(
        [None, __name__, MapFunc.__name__, "MapFunction"]
    )
    func = function.load_function(descriptor_func_bytes)
    assert type(func) is MapFunc
