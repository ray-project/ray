import ray
from ray.function_manager import FunctionDescriptor


def test_func_desc():
    sync_func = FunctionDescriptor("ray.streaming.operator_instance",
                                   "on_streaming_transfer", "Map")
    async_func = FunctionDescriptor("ray.streaming.operator_instance",
                                    "on_streaming_transfer_sync", "Map")
    print(sync_func)
    print(sync_func.get_function_descriptor_list())


if __name__ == "__main__":
    test_func_desc()
