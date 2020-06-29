import ray
from ray.streaming import StreamingContext


def test_data_stream():
    ray.init(load_code_from_local=True, include_java=True)
    ctx = StreamingContext.Builder().build()
    stream = ctx.from_values(1, 2, 3)
    java_stream = stream.as_java_stream()
    python_stream = java_stream.as_python_stream()
    assert stream.get_id() == java_stream.get_id()
    assert stream.get_id() == python_stream.get_id()
    python_stream.set_parallelism(10)
    assert stream.get_parallelism() == java_stream.get_parallelism()
    assert stream.get_parallelism() == python_stream.get_parallelism()
    ray.shutdown()


def test_key_data_stream():
    ray.init(load_code_from_local=True, include_java=True)
    ctx = StreamingContext.Builder().build()
    key_stream = ctx.from_values(
        "a", "b", "c").map(lambda x: (x, 1)).key_by(lambda x: x[0])
    java_stream = key_stream.as_java_stream()
    python_stream = java_stream.as_python_stream()
    assert key_stream.get_id() == java_stream.get_id()
    assert key_stream.get_id() == python_stream.get_id()
    python_stream.set_parallelism(10)
    assert key_stream.get_parallelism() == java_stream.get_parallelism()
    assert key_stream.get_parallelism() == python_stream.get_parallelism()
    ray.shutdown()


def test_stream_config():
    ray.init(load_code_from_local=True, include_java=True)
    ctx = StreamingContext.Builder().build()
    stream = ctx.from_values(1, 2, 3)
    stream.with_config("k1", "v1")
    print("config", stream.get_config())
    assert stream.get_config() == {"k1": "v1"}
    stream.with_config(conf={"k2": "v2", "k3": "v3"})
    print("config", stream.get_config())
    assert stream.get_config() == {"k1": "v1", "k2": "v2", "k3": "v3"}
    java_stream = stream.as_java_stream()
    java_stream.with_config(conf={"k4": "v4"})
    config = java_stream.get_config()
    print("config", config)
    assert config == {"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4"}
    ray.shutdown()
