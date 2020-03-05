import ray
from ray.streaming import StreamingContext


def test_data_stream():
    if not ray.is_initialized():
        ray.init(load_code_from_local=True, include_java=True)
    ctx = StreamingContext.Builder().build()
    stream = ctx.from_values(1, 2, 3)
    java_stream = stream.as_java()
    python_stream = java_stream.as_python()
    assert stream.get_id() == java_stream.get_id()
    assert stream.get_id() == python_stream.get_id()
    python_stream.set_parallelism(10)
    assert stream.get_parallelism() == java_stream.get_parallelism()
    assert stream.get_parallelism() == python_stream.get_parallelism()


def test_key_data_stream():
    if not ray.is_initialized():
        ray.init(load_code_from_local=True, include_java=True)
    ctx = StreamingContext.Builder().build()
    key_stream = ctx.from_values("a", "b", "c").map(lambda x: (x, 1)).key_by(
        lambda x: x[0])
    java_stream = key_stream.as_java()
    python_stream = java_stream.as_python()
    assert key_stream.get_id() == java_stream.get_id()
    assert key_stream.get_id() == python_stream.get_id()
    python_stream.set_parallelism(10)
    assert key_stream.get_parallelism() == java_stream.get_parallelism()
    assert key_stream.get_parallelism() == python_stream.get_parallelism()
