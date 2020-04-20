import json
import ray
from ray.streaming import StreamingContext
import pytest


def map_func1(x):
    print("HybridStreamTest map_func1", x)
    return str(x)


def filter_func1(x):
    print("HybridStreamTest filter_func1", x)
    return "b" not in x


def sink_func1(x):
    print("HybridStreamTest sink_func1 value:", x)


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
        .map("org.ray.streaming.runtime.demo.CrossLangStreamTest$Mapper1") \
        .filter("org.ray.streaming.runtime.demo.CrossLangStreamTest$Filter1") \
        .as_python() \
        .sink(print)
    ctx.submit("hybrid_stream_test")
    import time
    time.sleep(3)
    ray.shutdown()


if __name__ == "__main__":
    test_data_stream()
