import json
import ray
from ray.streaming import StreamingContext
import pytest


def map_func1(x):
    return str(x) + str(x)


def filter_func1(x):
    return "b" not in x


@pytest.mark.skip(
    "Use script to setup java worker classpath jars for this test")
def test_data_stream():
    if not ray.is_initialized():
        ray.init(load_code_from_local=True, include_java=True,
                 _internal_config=json.dumps({
                     "num_workers_per_process_java": 1
                 }))
    ctx = StreamingContext.Builder().build()
    stream = ctx.from_values("a", "b", "c") \
        .as_java() \
        .map("org.ray.streaming.runtime.python.PythonGateway$Mapper1") \
        .sink("org.ray.streaming.runtime.python.PythonGateway$Sink1")
    ctx.submit("cross_lang_word_count")
    import time
    time.sleep(3)
    ray.shutdown()


if __name__ == "__main__":
    test_data_stream()
